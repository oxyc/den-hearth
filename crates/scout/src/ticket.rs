//! Port of `internal/scout/ticket.go`: play tickets. With CONFIG_KEY set, a stream list names its play
//! URLs `/p/<ticket>`; a ticket carries only what one resolve needs, for one release, until it expires.
//!
//! ```text
//! ticket = base64url(nonce(24) || XChaCha20-Poly1305(K_play, payload))
//! K_play = HKDF-SHA256(ikm = the CONFIG_KEY private key, info = "den-scout/play/v1")
//! ```

use std::borrow::Cow;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chacha20poly1305::aead::{Aead, AeadCore, AeadInPlace, KeyInit, OsRng};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use hkdf::Hkdf;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::config::{Config, DebridAccount, DebridService, MAX_CONFIG_BLOB, SCOPE_AVAILABILITY};
use crate::play::{is_info_hash, PlayTarget};
use crate::seal::SealKeyring;
use crate::util::{b64url_decode, b64url_encode, fold_json_keys, Shape, LEAF};

pub const PLAY_TICKET_INFO: &str = "den-scout/play/v1";
/// The ticket-key info of the handler that reads the combined Den addon's configs (`Deps::config_slice`). Its own,
/// so a Den ticket never opens on Scout's routes, nor Scout's on the Den addon's, even under one sealing key.
pub const DEN_PLAY_TICKET_INFO: &str = "den-hearth/den-addon/play/v1";
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
///
/// The text fields are `Cow`s so that minting writes the config's and target's strings as they lie, where opening
/// (which always builds owned values) takes them by value.
#[derive(Serialize, Deserialize, Default)]
#[serde(default)]
struct TicketWire<'a> {
    h: Cow<'a, str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    f: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    s: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    e: Option<i64>,
    /// [service, token] per account, in config order.
    d: Vec<(Cow<'a, str>, Cow<'a, str>)>,
    /// Expiry, unix seconds.
    x: i64,
    #[serde(skip_serializing_if = "is_empty")]
    i: Cow<'a, str>,
    #[serde(skip_serializing_if = "is_zero")]
    p: i64,
    #[serde(skip_serializing_if = "is_empty")]
    scope: Cow<'a, str>,
    /// The indexer's release size in bytes. Absent from tickets minted before it existed, and from releases
    /// whose size no indexer stated; both open with a size of 0, which the link check reads as unknown.
    #[serde(skip_serializing_if = "is_zero")]
    z: i64,
    /// Rt (release title) and Tt (the title id, IMDb) ride along for the play/probe/cancel decision lines
    /// (decisionlog.go) that resolve this ticket later. Never read by `open`'s caller for anything else, and
    /// never the install id (that's `I`, config.IID, a different field already).
    #[serde(skip_serializing_if = "is_empty")]
    rt: Cow<'a, str>,
    #[serde(skip_serializing_if = "is_empty")]
    tt: Cow<'a, str>,
}

fn is_zero(v: &i64) -> bool {
    *v == 0
}

fn is_empty(v: &str) -> bool {
    v.is_empty()
}

/// `TicketWire`'s field names, as the JSON spells them.
const WIRE_KEYS: [&str; 12] = ["h", "f", "s", "e", "d", "x", "i", "p", "scope", "z", "rt", "tt"];

/// Whether `json` is an object whose every key is one of `WIRE_KEYS`, spelled exactly, and none repeats. Anything
/// else (another spelling, an unknown key, a duplicate, not an object, not JSON) answers false and is left to the
/// folding path.
fn has_exact_keys(json: &[u8]) -> bool {
    use serde::de::{Deserializer, IgnoredAny, MapAccess, Visitor};

    /// A key, as the bit of `WIRE_KEYS` it names, or 0 when it names none.
    struct Key(u16);
    impl<'de> Deserialize<'de> for Key {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            struct KeyVisitor;
            impl Visitor<'_> for KeyVisitor {
                type Value = Key;
                fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str("a string key")
                }
                fn visit_str<E>(self, s: &str) -> Result<Key, E> {
                    Ok(Key(WIRE_KEYS.iter().position(|k| *k == s).map_or(0, |i| 1 << i)))
                }
            }
            d.deserialize_str(KeyVisitor)
        }
    }

    struct Exact(bool);
    impl<'de> Deserialize<'de> for Exact {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            struct ExactVisitor;
            impl<'de> Visitor<'de> for ExactVisitor {
                type Value = Exact;
                fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str("an object")
                }
                fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Exact, A::Error> {
                    let mut seen = 0u16;
                    while let Some(Key(bit)) = map.next_key()? {
                        if bit == 0 || seen & bit != 0 {
                            return Ok(Exact(false));
                        }
                        seen |= bit;
                        map.next_value::<IgnoredAny>()?;
                    }
                    Ok(Exact(true))
                }
            }
            d.deserialize_map(ExactVisitor)
        }
    }

    // Stopping at the first odd key leaves the rest of the object unread, which the parser reports as an error;
    // that is "not exact" as well.
    matches!(serde_json::from_slice::<Exact>(json), Ok(Exact(true)))
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
        TicketKeys::with_info(kr, PLAY_TICKET_INFO)
    }

    /// `new`, with the keys expanded under `info` (`DEN_PLAY_TICKET_INFO` for the Den addon's handler).
    pub fn with_info(kr: Option<&SealKeyring>, info: &str) -> Option<TicketKeys> {
        match kr {
            Some(kr) if !kr.keys.is_empty() => {
                let keys = kr
                    .keys
                    .iter()
                    .map(|k| {
                        // HKDF only refuses an output longer than 255 hashes, so a 32-byte key cannot fail.
                        let mut key = [0u8; 32];
                        Hkdf::<Sha256>::new(None, &k.priv_key)
                            .expand(info.as_bytes(), &mut key)
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
            h: Cow::Borrowed(&target.info_hash),
            f: target.file_idx,
            s: target.season,
            e: target.episode,
            d: config
                .debrid
                .iter()
                .map(|d| (Cow::Borrowed(d.service.as_str()), Cow::Borrowed(d.token.as_str())))
                .collect(),
            x: unix_secs(exp),
            i: Cow::Borrowed(&config.iid),
            p: config.epoch,
            scope: Cow::Borrowed(&config.scope),
            z: target.release_size,
            rt: Cow::Borrowed(&target.title),
            tt: Cow::Borrowed(&target.imdb),
        };
        // Tickets are minted with the first key, as Go's aeads[0].
        let aead = XChaCha20Poly1305::new(&self.keys[0].into());
        // OsRng panics rather than returning an error, as Go's crypto/rand.Read crashes the process.
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        // nonce || payload || tag in one buffer: the payload is written after the nonce and sealed where it lies.
        // A ticket is a few hundred bytes, so this capacity holds it without growing.
        let mut out = Vec::with_capacity(512);
        out.extend_from_slice(&nonce);
        // Go ignores the marshal error too; this struct always serialises.
        let _ = serde_json::to_writer(&mut out, &w);
        let tag = aead
            .encrypt_in_place_detached(&nonce, &[], &mut out[NONCE_SIZE..])
            .expect("XChaCha20-Poly1305 seals any payload");
        out.extend_from_slice(&tag);
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
        // A ticket this process minted spells every key exactly, once, and then folding changes nothing: it reads
        // straight into the struct, where the folding path builds a `Value` of 20-odd allocations first.
        let w: TicketWire = if has_exact_keys(&pt) {
            serde_json::from_slice(&pt).map_err(|_| TicketError::Bad)?
        } else {
            let mut value: serde_json::Value = serde_json::from_slice(&pt).map_err(|_| TicketError::Bad)?;
            fold_json_keys(&mut value, &SHAPE);
            serde_json::from_value(value).map_err(|_| TicketError::Bad)?
        };
        if unix_secs(now) >= w.x {
            return Err(TicketError::Expired);
        }
        // Only this process can have written a ticket that authenticates, so these checks are not the defence.
        // They hold the resolve to the same shapes the legacy route validates, whatever a future minter writes.
        let h = str::to_lowercase(&w.h);
        if !is_info_hash(&h) {
            return Err(TicketError::Bad);
        }
        if !w.scope.is_empty() && w.scope != SCOPE_AVAILABILITY {
            return Err(TicketError::Bad);
        }
        let mut debrid = Vec::with_capacity(w.d.len());
        for (service, token) in w.d {
            match DebridService::parse(&service) {
                Some(service) if !token.is_empty() => {
                    debrid.push(DebridAccount { service, token: token.into_owned() })
                }
                _ => return Err(TicketError::Bad),
            }
        }
        if debrid.is_empty() {
            return Err(TicketError::Bad);
        }
        let config = Config {
            debrid,
            iid: w.i.into_owned(),
            epoch: w.p,
            scope: w.scope.into_owned(),
            ..Default::default()
        };
        let target = PlayTarget {
            info_hash: h,
            file_idx: w.f,
            season: w.s,
            episode: w.e,
            release_size: w.z.max(0),
            title: w.rt.into_owned(),
            imdb: w.tt.into_owned(),
        };
        Ok((config, target))
    }
}
