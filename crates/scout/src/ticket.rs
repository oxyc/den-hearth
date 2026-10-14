//! Port of `internal/scout/ticket.go`: play tickets. With CONFIG_KEY set, a stream list names its play
//! URLs `/p/<ticket>`; a ticket carries only what one resolve needs, for one release, until it expires.
//!
//! ```text
//! ticket = base64url(nonce(24) || XChaCha20-Poly1305(K_play, payload))
//! K_play = HKDF-SHA256(ikm = the CONFIG_KEY private key, info = "den-scout/play/v1")
//! ```

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use hkdf::Hkdf;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::config::{Config, DebridAccount, DebridService, MAX_CONFIG_BLOB, SCOPE_AVAILABILITY};
use crate::play::{is_info_hash, PlayTarget};
use crate::seal::SealKeyring;
use crate::util::{b64url_decode, b64url_encode, fold_json_keys, go_to_lower, Shape, LEAF};

pub const PLAY_TICKET_INFO: &str = "den-scout/play/v1";
/// The first path segment of a ticketed play URL. No config segment can be "p" — it does not decode.
pub const TICKET_ROUTE: &str = "p";
/// How long a ticket stays good (PLAY_TICKET_TTL_SECS). It must outlast the stream list that carries it,
/// which a client may first use up to 3×LIST_TTL_SECS after the list was built (a hit served just before
/// the server's freshness ends, then max-age plus stale-while-revalidate on the device), and then the
/// longest viewing session, during which the player may ask the URL again. A day covers both with room.
/// A list used past its freshness (stale-if-error, or the server's stale_list fallback) is held inside this
/// life by staleIfErrorFor.
pub const DEFAULT_PLAY_TICKET_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// The XChaCha20-Poly1305 nonce length, and the Poly1305 tag length.
const NONCE_SIZE: usize = 24;
const TAG_SIZE: usize = 16;

/// Go `errTicketBad` / `errTicketExpired`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TicketError {
    /// "ticket: does not open"
    Bad,
    /// "ticket: expired"
    Expired,
}

/// Go `ticketWire`: a ticket's payload. The target a legacy token names (Go's embedded `playWire`, h/f/s/e,
/// inlined here), the accounts the resolve uses, the expiry, and the install id and epoch revocation checks.
/// Nothing else of the config goes in: no filters, no indexers. Scope records the admission provenance;
/// availability configs may legitimately lack an install id, including when den-remux is authorized to mint
/// tickets for them.
#[derive(Serialize, Deserialize, Default)]
#[serde(default)]
struct TicketWire {
    h: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    f: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    s: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    e: Option<i64>,
    /// [service, token] per account, in config order.
    d: Vec<(String, String)>,
    /// Expiry, unix seconds.
    x: i64,
    #[serde(skip_serializing_if = "String::is_empty")]
    i: String,
    #[serde(skip_serializing_if = "is_zero")]
    p: i64,
    #[serde(skip_serializing_if = "String::is_empty")]
    scope: String,
    /// The indexer's release size in bytes. Absent from tickets minted before it existed, and from releases
    /// whose size no indexer stated; both open with a size of 0, which the link check reads as unknown.
    #[serde(skip_serializing_if = "is_zero")]
    z: i64,
    /// Rt (release title) and Tt (the title id, IMDb) ride along for the play/probe/cancel decision lines
    /// (decisionlog.go) that resolve this ticket later. Never read by `open`'s caller for anything else, and
    /// never the install id (that's `I`, config.IID, a different field already).
    #[serde(skip_serializing_if = "String::is_empty")]
    rt: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    tt: String,
}

fn is_zero(v: &i64) -> bool {
    *v == 0
}

/// Go `t.Unix()`: whole seconds since the epoch, floored for times before it.
fn unix_secs(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs() as i64,
        Err(e) => {
            let d = e.duration();
            let s = d.as_secs() as i64;
            if d.subsec_nanos() == 0 {
                -s
            } else {
                -s - 1
            }
        }
    }
}

/// Go `ticketKeys`: one AEAD per keyring key, current first; tickets are minted with the first and
/// opened with any.
pub struct TicketKeys {
    /// One 32-byte AEAD key per keyring key, current first (Go's `aeads`, held as raw keys here). The tests
    /// seal legacy tickets with these directly.
    pub(crate) keys: Vec<[u8; 32]>,
}

impl TicketKeys {
    /// Go `newTicketKeys`: `None` with no keyring, which is what turns tickets off.
    pub fn new(kr: Option<&SealKeyring>) -> Option<TicketKeys> {
        match kr {
            Some(kr) if !kr.keys.is_empty() => {
                let keys = kr
                    .keys
                    .iter()
                    .map(|k| {
                        // HKDF only refuses an output longer than 255 hashes, so a 32-byte key cannot fail.
                        let mut key = [0u8; 32];
                        Hkdf::<Sha256>::new(None, &k.priv_key)
                            .expand(PLAY_TICKET_INFO.as_bytes(), &mut key)
                            .expect("32 bytes is within HKDF-SHA256's output limit");
                        key
                    })
                    .collect();
                Some(TicketKeys { keys })
            }
            _ => None,
        }
    }

    /// Go `mint`: seals a ticket for one release of this config, good until `exp`.
    pub fn mint(&self, config: &Config, target: &PlayTarget, exp: SystemTime) -> String {
        let w = TicketWire {
            h: target.info_hash.clone(),
            f: target.file_idx,
            s: target.season,
            e: target.episode,
            d: config.debrid.iter().map(|d| (d.service.as_str().to_owned(), d.token.clone())).collect(),
            x: unix_secs(exp),
            i: config.iid.clone(),
            p: config.epoch,
            scope: config.scope.clone(),
            z: target.release_size,
            rt: target.title.clone(),
            tt: target.imdb.clone(),
        };
        // Go ignores the marshal error too; this struct always serialises.
        let pt = serde_json::to_vec(&w).unwrap_or_default();
        // Tickets are minted with the first key, as Go's aeads[0].
        let aead = XChaCha20Poly1305::new(&self.keys[0].into());
        // OsRng panics rather than returning an error, as Go's crypto/rand.Read crashes the process.
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let ct = aead.encrypt(&nonce, pt.as_slice()).expect("XChaCha20-Poly1305 seals any payload");
        let mut out = nonce.to_vec();
        out.extend_from_slice(&ct);
        b64url_encode(&out)
    }

    /// Go `open`: authenticates a ticket and returns the config and target it carries.
    pub fn open(&self, ticket: &str, now: SystemTime) -> Result<(Config, PlayTarget), TicketError> {
        // The same ceiling as a config segment: a ticket for the largest config the field caps admit fits under
        // it (TestPlayTicket_theLargestConfigFits), and nothing larger is worth decoding.
        if ticket.len() > MAX_CONFIG_BLOB {
            return Err(TicketError::Bad);
        }
        let raw = b64url_decode(ticket).map_err(|_| TicketError::Bad)?;
        if raw.len() < NONCE_SIZE + TAG_SIZE {
            return Err(TicketError::Bad);
        }
        let (nonce, ct) = raw.split_at(NONCE_SIZE);
        let nonce = XNonce::from_slice(nonce);
        let pt = self
            .keys
            .iter()
            .find_map(|k| XChaCha20Poly1305::new(&(*k).into()).decrypt(nonce, ct).ok())
            .ok_or(TicketError::Bad)?;
        // Go matches keys to fields case-insensitively, so a payload spelled "X" or "H" still opens there.
        static SHAPE: Shape = Shape::Object(&[
            ("h", &LEAF),
            ("f", &LEAF),
            ("s", &LEAF),
            ("e", &LEAF),
            ("d", &LEAF),
            ("x", &LEAF),
            ("i", &LEAF),
            ("p", &LEAF),
            ("scope", &LEAF),
            ("z", &LEAF),
            ("rt", &LEAF),
            ("tt", &LEAF),
        ]);
        let mut value: serde_json::Value = serde_json::from_slice(&pt).map_err(|_| TicketError::Bad)?;
        fold_json_keys(&mut value, &SHAPE);
        let w: TicketWire = serde_json::from_value(value).map_err(|_| TicketError::Bad)?;
        if unix_secs(now) >= w.x {
            return Err(TicketError::Expired);
        }
        // Only this process can have written a ticket that authenticates, so these checks are not the defence.
        // They hold the resolve to the same shapes the legacy route validates, whatever a future minter writes.
        let h = go_to_lower(&w.h);
        if !is_info_hash(&h) {
            return Err(TicketError::Bad);
        }
        if !w.scope.is_empty() && w.scope != SCOPE_AVAILABILITY {
            return Err(TicketError::Bad);
        }
        let mut debrid = Vec::with_capacity(w.d.len());
        for (service, token) in &w.d {
            match DebridService::parse(service) {
                Some(service) if !token.is_empty() => debrid.push(DebridAccount { service, token: token.clone() }),
                _ => return Err(TicketError::Bad),
            }
        }
        if debrid.is_empty() {
            return Err(TicketError::Bad);
        }
        let config = Config { debrid, iid: w.i, epoch: w.p, scope: w.scope, ..Default::default() };
        let target = PlayTarget {
            info_hash: h,
            file_idx: w.f,
            season: w.s,
            episode: w.e,
            release_size: w.z.max(0),
            title: w.rt,
            imdb: w.tt,
        };
        Ok((config, target))
    }
}
