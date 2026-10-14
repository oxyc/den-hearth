//! Entry file names. Both schemes are what the services already wrote, so a directory an earlier release left
//! behind opens unchanged.

use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use base64::Engine;
use sha2::{Digest, Sha256};

/// How a key becomes a file name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Naming {
    /// `<base64url(sha256(key))>.ent`. Any key is a short, safe name; the key cannot be read back from it.
    Hashed,
    /// `<base64url(key)>`, or for a key past [`MAX_PLAIN`] bytes `<base64url(first 32 chars)>~<fnv1a64 of the key>`.
    /// The store stays greppable by key namespace, and the namespace is what a cost-ranked eviction reads back.
    Readable,
}

/// Bytes of key that base64 to a comfortably legal file name: NAME_MAX is 255 and base64 is 4 out per 3 in.
const MAX_PLAIN: usize = 180;

impl Naming {
    pub(crate) fn file_name(self, key: &str) -> String {
        match self {
            Naming::Hashed => format!("{}.ent", B64.encode(Sha256::digest(key.as_bytes()))),
            Naming::Readable if key.len() <= MAX_PLAIN => B64.encode(key.as_bytes()),
            Naming::Readable => {
                // On a char boundary: a key is UTF-8 and a byte slice through a multibyte char panics.
                let head: String = key.chars().take(32).collect();
                // `~` is not in the base64url alphabet, so a plain name can never equal a hashed one. The hash is a
                // FIXED algorithm: it names a file that outlives the process, and std's hasher may change between
                // compiler releases, which would orphan every long-key entry at once.
                format!("{}~{:016x}", B64.encode(head.as_bytes()), fnv1a64(key.as_bytes()))
            }
        }
    }

    /// Whether `name` is an entry file of this scheme, as opposed to a temp file or something foreign.
    pub(crate) fn is_entry(self, name: &str) -> bool {
        match self {
            Naming::Hashed => name.ends_with(".ent"),
            Naming::Readable => {
                !name.is_empty()
                    && name.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'~'))
            }
        }
    }

    /// The key a `Readable` name was made from, or for a long key its first 32 characters: enough for the
    /// namespace. `Hashed` names cannot be read back.
    pub(crate) fn key_prefix(self, name: &str) -> Option<String> {
        if self != Naming::Readable {
            return None;
        }
        let head = name.split('~').next().unwrap_or(name);
        let bytes = B64.decode(head).ok()?;
        Some(String::from_utf8_lossy(&bytes).into_owned())
    }

    /// A temp file left by the other generation of this scheme: Readable names once wrote `<name>.t<N>`.
    pub(crate) fn is_legacy_temp(self, name: &str) -> bool {
        self == Naming::Readable && name.rsplit_once('.').is_some_and(|(_, ext)| ext.starts_with('t'))
    }
}

/// FNV-1a, 64-bit: the same function as `den_http::stable_hash`, kept here so this crate has no HTTP dependency. A
/// test pins the name it produces.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}
