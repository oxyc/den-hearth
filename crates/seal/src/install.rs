use std::borrow::Borrow;
use std::collections::HashSet;

use base64::Engine;

/// An install id as /configure mints it: 16 random bytes, base64url, unpadded, 22 characters. Decoding is
/// strict (no padding, no non-zero trailing bits), so each id has exactly one spelling and a
/// `REVOKED_INSTALLS` entry cannot be dodged by re-encoding the same bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InstallId(String);

impl InstallId {
    pub const BYTES: usize = 16;
    pub const LEN: usize = 22;

    pub fn parse(s: &str) -> Option<InstallId> {
        let ok = s.len() == Self::LEN
            && base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(s)
                .is_ok_and(|b| b.len() == Self::BYTES);
        ok.then(|| InstallId(s.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for InstallId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

/// Ceiling on an epoch (`CONFIG_EPOCH` and a config's `ep`), so it always fits an `int` in the Go addon and
/// a config cannot carry an absurd one.
pub const MAX_EPOCH: u64 = i32::MAX as u64;

/// Which installs are refused (issue #8 R3). Naming an install id kills one leaked link; raising the
/// epoch kills every link stamped before it. Neither touches the sealing key, and rotating that key would
/// not do this: `CONFIG_KEYS_PREV` keeps links sealed to the old key opening on purpose.
#[derive(Debug, Clone, Default)]
pub struct Revocation {
    revoked: HashSet<InstallId>,
    epoch: u64,
    /// `REQUIRE_INSTALL_ID`: refuse a config with no install id, which is every link built before ids
    /// existed. Those can't be named, and raising the epoch would also refuse the links minted since,
    /// which carry the same epoch.
    require_iid: bool,
}

impl Revocation {
    /// `parse`, with a warning printed on stderr.
    pub fn from_env(revoked: &str, epoch: Option<&str>) -> Revocation {
        Revocation::parse(revoked, epoch, &|m| eprintln!("warning: {m}"))
    }

    /// From `REVOKED_INSTALLS` (comma-separated install ids) and `CONFIG_EPOCH` (default 0). A malformed
    /// entry is skipped with a warning: no config that decodes can carry it, so it could never match. An
    /// epoch that is not an integer from 0 to `MAX_EPOCH` is said loudly and enforced as 0 (refusing every
    /// install over a typo would take the addon down); the startup line's `epoch=` shows the value in force.
    pub fn parse(revoked: &str, epoch: Option<&str>, warn: &dyn Fn(&str)) -> Revocation {
        let mut ids = HashSet::new();
        for entry in revoked.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            match InstallId::parse(entry) {
                Some(id) => {
                    ids.insert(id);
                }
                None => warn(&format!(
                    "skipping a malformed REVOKED_INSTALLS entry (want a {}-character install id)",
                    InstallId::LEN
                )),
            }
        }
        let epoch = match epoch.map(str::trim).filter(|e| !e.is_empty()) {
            None => 0,
            Some(raw) => match raw.parse::<u64>() {
                Ok(n) if n <= MAX_EPOCH => n,
                _ => {
                    warn(&format!(
                        "CONFIG_EPOCH={raw:?} is not an integer from 0 to {MAX_EPOCH} — enforcing epoch 0, so \
                         no config is refused by epoch"
                    ));
                    0
                }
            },
        };
        Revocation { revoked: ids, epoch, require_iid: false }
    }

    /// Also refuse configs that carry no install id (`REQUIRE_INSTALL_ID`).
    pub fn requiring_install_id(mut self, on: bool) -> Revocation {
        self.require_iid = on;
        self
    }

    pub fn revoked_count(&self) -> usize {
        self.revoked.len()
    }

    pub fn is_revoked(&self, iid: &str) -> bool {
        self.revoked.contains(iid)
    }

    pub fn requires_install_id(&self) -> bool {
        self.require_iid
    }

    /// The oldest epoch still admitted — what /configure stamps into a new link.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Whether the install with this id and epoch is still admitted — asked of a config, and of a signed
    /// play link bound to one. In this order: the id (the more specific answer), the epoch, then the
    /// missing id.
    pub fn check(&self, iid: Option<&str>, ep: u64) -> Result<(), Rejected> {
        self.check_with(iid, ep, false)
    }

    /// `check` for a SCOPED config, which is minted separately from the install and may carry no id:
    /// `REQUIRE_INSTALL_ID` does not apply, named revocation and the epoch still do.
    pub fn check_scoped(&self, iid: Option<&str>, ep: u64) -> Result<(), Rejected> {
        self.check_with(iid, ep, true)
    }

    fn check_with(&self, iid: Option<&str>, ep: u64, scoped: bool) -> Result<(), Rejected> {
        if let Some(iid) = iid.filter(|iid| self.revoked.contains(*iid)) {
            return Err(Rejected::Revoked { iid_prefix: iid.chars().take(6).collect() });
        }
        if ep < self.epoch {
            return Err(Rejected::EpochTooOld { ep, epoch: self.epoch });
        }
        if self.require_iid && iid.is_none() && !scoped {
            return Err(Rejected::NoInstallId);
        }
        Ok(())
    }
}

/// Why a config segment was not accepted. Every variant gets the same answer on the wire; the difference
/// is for the log only, and nothing here is a credential: a revoked id is kept to its first six
/// characters, and an admitted install's id never gets this far.
#[derive(Debug, PartialEq, Eq)]
pub enum Rejected {
    Undecodable,
    /// A plaintext config while sealing is on: every install `/configure` issues is sealed then.
    Plaintext,
    Revoked {
        iid_prefix: String,
    },
    EpochTooOld {
        ep: u64,
        epoch: u64,
    },
    NoInstallId,
}

impl std::fmt::Display for Rejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Rejected::Undecodable => f.write_str("config does not decode"),
            Rejected::Plaintext => f.write_str("plaintext config while sealing is on"),
            Rejected::Revoked { iid_prefix } => write!(f, "install revoked (iid={iid_prefix}…)"),
            Rejected::EpochTooOld { ep, epoch } => {
                write!(f, "install epoch too old (ep={ep} < CONFIG_EPOCH={epoch})")
            }
            Rejected::NoInstallId => f.write_str("no install id (REQUIRE_INSTALL_ID is on)"),
        }
    }
}
