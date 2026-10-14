//! The combined Den addon that hearth serves under `/den`: one config holding each service's part, sealed by one
//! key ring and admitted by one revocation policy, read by the same service handlers that read their own configs.
//!
//! The combined config is a JSON object with the install's identity at the top and one member per service:
//!
//! ```json
//! {"iid": "…", "ep": 0, "scout": {"debrid": […]}, "subs": {"osKey": "…"}, "reel": {"tmdbKey": "…"}}
//! ```
//!
//! Each member is exactly the config that service's own `/configure` builds, minus `iid` and `ep`. A service
//! reads its member as though it were its own config, with the top-level `iid` and `ep` put in, so it labels and
//! admits the request as the combined install. A service's own config has no such member, so a segment of one kind
//! never opens as the other.

use crate::{open_segment, Keyring, Opened, Rejected, Revocation};

/// The members of a combined config, one per service, in the order the manifest lists their resources.
pub const SLICES: [&str; 3] = ["scout", "subs", "reel"];

/// What a service needs to read a request hearth routed under `/den`. Hearth puts one in the request's extensions
/// (a client cannot), and a request without one is the service's own, read on the service's own terms.
#[derive(Clone)]
pub struct Addon {
    /// Opens the combined config. `None` with `key_invalid` false is sealing off: plaintext configs only.
    pub keyring: Option<Keyring>,
    /// The combined key is set but malformed: every combined config is refused until it is fixed.
    pub key_invalid: bool,
    /// The combined addon's revocation, which replaces the service's own for these requests.
    pub revocation: Revocation,
    /// `<HEARTH_PUBLIC_BASE_URL>/den`, the base every link minted for the request points under. `None` derives
    /// the origin from the request and puts `MOUNT` after it.
    pub public_base: Option<String>,
    /// The member of the combined config this service reads (one of `SLICES`).
    pub slice: &'static str,
}

/// The path the combined addon is served under.
pub const MOUNT: &str = "/den";

impl Addon {
    /// `blob` opened down to this service's member of it (`open_slice`).
    pub fn open(&self, blob: &str) -> Result<Opened, Rejected> {
        open_slice(self.keyring.as_ref(), blob, self.slice)
    }

    /// The base links are minted under, given the origin the service would derive from the request.
    pub fn base(&self, derived_origin: &str) -> String {
        match &self.public_base {
            Some(base) => base.trim_end_matches('/').to_owned(),
            None => format!("{}{MOUNT}", derived_origin.trim_end_matches('/')),
        }
    }
}

/// Read the combined config segment `blob` down to the JSON of its `slice` member, with the config's `iid` and
/// `ep` copied in. Fails closed like `open_segment`, and also when the config is not an object or has no such
/// member (a request for a resource the install did not configure).
pub fn open_slice(keyring: Option<&Keyring>, blob: &str, slice: &str) -> Result<Opened, Rejected> {
    let opened = open_segment(keyring, blob)?;
    let json = slice_json(&opened.json, slice).ok_or(Rejected::Undecodable)?;
    Ok(Opened { json, sealed: opened.sealed })
}

/// The `slice` member of a combined config, as JSON, with the top-level `iid` and `ep` in place of any the member
/// carries. A member cannot name an install of its own: the install is the whole config, and an id inside a member
/// is matched case-insensitively (Scout reads keys that way) so no spelling of one survives.
pub fn slice_json(json: &[u8], slice: &str) -> Option<Vec<u8>> {
    let serde_json::Value::Object(mut whole) = serde_json::from_slice(json).ok()? else {
        return None;
    };
    let serde_json::Value::Object(mut part) = whole.remove(slice)? else {
        return None;
    };
    part.retain(|key, _| !key.eq_ignore_ascii_case("iid") && !key.eq_ignore_ascii_case("ep"));
    for key in ["iid", "ep"] {
        if let Some(value) = whole.remove(key) {
            part.insert(key.to_owned(), value);
        }
    }
    serde_json::to_vec(&serde_json::Value::Object(part)).ok()
}
