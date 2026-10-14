//! Re-exports for `benches/reel.rs`: the hot paths live in private modules. Compiled only with
//! `--features bench`; nothing here is reachable from a normal build, and each name is the production item.

pub use crate::addon::{build_meta, manifest};
pub use crate::crop::{parse_all_crops, typical_crop};
pub use crate::sign::{message, tag, verify, Binding, Signer};
