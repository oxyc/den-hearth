//! Port of `internal/scout/ticket.go`: play tickets. With CONFIG_KEY set, a stream list names its play
//! URLs `/p/<ticket>`; a ticket carries only what one resolve needs, for one release, until it expires.
//!
//! ```text
//! ticket = base64url(nonce(24) || XChaCha20-Poly1305(K_play, payload))
//! K_play = HKDF-SHA256(ikm = the CONFIG_KEY private key, info = "den-scout/play/v1")
//! ```

use std::time::{Duration, SystemTime};

use crate::config::Config;
use crate::play::PlayTarget;
use crate::seal::SealKeyring;

pub const PLAY_TICKET_INFO: &str = "den-scout/play/v1";
/// The first path segment of a ticketed play URL.
pub const TICKET_ROUTE: &str = "p";
/// Go `defaultPlayTicketTTL`: a day.
pub const DEFAULT_PLAY_TICKET_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Go `errTicketBad` / `errTicketExpired`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TicketError {
    /// "ticket: does not open"
    Bad,
    /// "ticket: expired"
    Expired,
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
        todo!("ticket.go newTicketKeys")
    }

    /// Go `mint`: seals a ticket for one release of this config, good until `exp`.
    pub fn mint(&self, config: &Config, target: &PlayTarget, exp: SystemTime) -> String {
        todo!("ticket.go ticketKeys.mint")
    }

    /// Go `open`: authenticates a ticket and returns the config and target it carries.
    pub fn open(&self, ticket: &str, now: SystemTime) -> Result<(Config, PlayTarget), TicketError> {
        todo!("ticket.go ticketKeys.open")
    }
}
