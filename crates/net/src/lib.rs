//! One public-address classifier. Scout's household sources, Subtitles' resync hops and Remux's source fetches all
//! refuse a URL that leads off the public internet, and each had its own list, missing ranges the others covered.
//! This is the union of them, so a range added here is refused everywhere.
//!
//! It also holds the one reading of a config path segment's base64url, so Scout, Reel and Subtitles open the same
//! links, and the one guarded signal to a child's process group (`kill_group`), so Reel and Remux refuse the same
//! process group ids, and the one `Playable` report Remux and Reel read a browser's capabilities with.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

mod playable;
pub use playable::{DolbyVisionPlay, Playable};

use base64::alphabet::URL_SAFE;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use base64::Engine as _;

/// A config segment's base64url, padded or not (lenient, like the TS decoder and Go's decoder).
///
/// Trailing `=` are trimmed, carriage returns and newlines are skipped anywhere, and non-zero trailing bits are
/// accepted, as Go's `RawURLEncoding` does. Install ids are NOT read with this: they stay strict.
pub fn b64url_decode(s: &str) -> Result<Vec<u8>, String> {
    const LENIENT: GeneralPurpose = GeneralPurpose::new(
        &URL_SAFE,
        GeneralPurposeConfig::new()
            .with_encode_padding(false)
            .with_decode_allow_trailing_bits(true)
            .with_decode_padding_mode(DecodePaddingMode::RequireNone),
    );
    let trimmed = s.trim_end_matches('=');
    // Nothing but a hand-wrapped segment holds a line break, so the copy that drops them is the rare path.
    if !trimmed.bytes().any(|b| matches!(b, b'\r' | b'\n')) {
        return LENIENT.decode(trimmed).map_err(|e| e.to_string());
    }
    let cleaned: String = trimmed.chars().filter(|c| !matches!(c, '\r' | '\n')).collect();
    LENIENT.decode(cleaned).map_err(|e| e.to_string())
}

/// Whether `ip` is an address on the public internet: not loopback, private (RFC 1918), CGNAT (100.64/10, where a
/// tailnet lives), link-local (the 169.254.169.254 metadata endpoint), unspecified, broadcast, multicast,
/// documentation, benchmarking or reserved space; and for IPv6 not ULA, link-local, site-local, Teredo, the
/// documentation prefix, the discard prefix or the `::/96` block (which holds `::` and `::1` and the deprecated
/// IPv4-compatible form).
///
/// An IPv4-mapped address (`::ffff:a.b.c.d`) is judged as the IPv4 address it carries. The other IPv6 forms that
/// embed an IPv4 address are refused outright, whatever they embed: NAT64 (`64:ff9b::/96`, and local-use
/// `64:ff9b:1::/48`), 6to4 (`2002::/16`) and the SIIT IPv4-translated block (`::ffff:0:0:0/96`) all reach
/// IPv4 through a gateway on the path, which may be one inside the network, and no source or media host is
/// reached that way.
pub fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_public_v4(v4),
        IpAddr::V6(v6) => is_public_v6(v6),
    }
}

fn is_public_v6(v6: Ipv6Addr) -> bool {
    if let Some(v4) = v6.to_ipv4_mapped() {
        return is_public_v4(v4);
    }
    let s = v6.segments();
    !(s[..6] == [0; 6] // ::/96
        || (s[..4] == [0; 4] && s[4] == 0xffff && s[5] == 0) // SIIT ::ffff:0:0:0/96
        || (s[..6] == [0x64, 0xff9b, 0, 0, 0, 0]) // NAT64 64:ff9b::/96
        || (s[0] == 0x0064 && s[1] == 0xff9b && s[2] == 1) // local-use NAT64 64:ff9b:1::/48
        || s[0] == 0x2002 // 6to4 2002::/16
        || v6.is_multicast()
        || s[0] & 0xfe00 == 0xfc00 // ULA fc00::/7
        || s[0] & 0xffc0 == 0xfe80 // link-local fe80::/10
        || s[0] & 0xffc0 == 0xfec0 // deprecated site-local fec0::/10
        || (s[0] == 0x0100 && s[1..4] == [0, 0, 0]) // discard-only 100::/64
        || (s[0] == 0x2001 && s[1] == 0x0db8) // documentation 2001:db8::/32
        || (s[0] == 0x2001 && s[1] == 0)) // Teredo 2001::/32
}

fn is_public_v4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_multicast()
        || ip.is_documentation() // 192.0.2/24, 198.51.100/24, 203.0.113/24
        || a == 0 // "this network" 0/8
        || (a == 100 && b & 0xc0 == 64) // CGNAT 100.64/10
        || (a == 192 && b == 0 && c == 0) // IETF protocol assignments 192.0.0/24
        || (a == 192 && b == 88 && c == 99) // deprecated 6to4 relay anycast 192.88.99/24
        || (a == 198 && b & 0xfe == 18) // benchmarking 198.18/15
        || a >= 240) // reserved 240/4 (and the broadcast address)
}

/// SIGKILL a child's process group: the child and whatever it forked (yt-dlp's ffmpeg, ffmpeg's own children),
/// which `kill_on_drop` alone leaves running. Nothing off unix.
pub fn kill_group(pgid: u32) {
    #[cfg(unix)]
    signal_group(pgid, libc::SIGKILL);
    #[cfg(not(unix))]
    let _ = pgid;
}

/// Send `sig` to a process group. Already-exited is ESRCH, which nobody cares about.
#[cfg(unix)]
pub fn signal_group(pgid: u32, sig: libc::c_int) {
    // 0 is our own group and 1 would make kill(-1) "every process we may signal" — the whole container; above
    // i32::MAX the negation is a single unrelated pid (`-(u32::MAX - 7) as i32` is 8). None of them is reachable
    // from child.id(), which is exactly why none of them may be a live signal.
    if pgid > 1 && pgid <= i32::MAX as u32 {
        // Negative pid = the whole group.
        unsafe { libc::kill(-(pgid as i32), sig) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_public_internet_is_public() {
        for ip in [
            "8.8.8.8",
            "1.1.1.1",
            "93.184.216.34",
            "1.0.0.0",
            "9.255.255.255",
            "11.0.0.1",
            "126.255.255.255",
            "128.0.0.1",
            "100.128.0.1",    // past CGNAT
            "100.63.255.255", // before CGNAT
            "172.15.255.255",
            "172.32.0.1",
            "192.169.0.1",
            "198.17.255.255",
            "198.20.0.1",
            "223.255.255.255",
            "2606:4700:4700::1111",
            "2001:4860:4860::8888",
            "2001:470::1",
            "2a00:1450:4001::200e",
            "192.88.98.255", // next to the 6to4 relay block
            "192.89.0.1",
            "::ffff:8.8.8.8", // IPv4-mapped public
        ] {
            assert!(is_public(ip.parse().unwrap()), "{ip} counted as non-public");
        }
    }

    #[test]
    fn everything_else_is_not() {
        for (ip, what) in [
            ("0.0.0.0", "unspecified"),
            ("0.1.2.3", "this network"),
            ("127.0.0.1", "loopback"),
            ("127.255.255.254", "loopback"),
            ("10.0.0.1", "RFC 1918"),
            ("172.16.0.1", "RFC 1918"),
            ("172.31.255.255", "RFC 1918"),
            ("192.168.86.193", "RFC 1918, den itself"),
            ("100.64.0.1", "CGNAT / tailnet"),
            ("100.127.255.255", "CGNAT's last address"),
            ("169.254.169.254", "link-local, cloud metadata"),
            ("192.0.0.1", "IETF protocol assignments"),
            ("192.0.2.1", "documentation TEST-NET-1"),
            ("198.51.100.7", "documentation TEST-NET-2"),
            ("203.0.113.9", "documentation TEST-NET-3"),
            ("198.18.0.1", "benchmarking"),
            ("198.19.255.255", "benchmarking"),
            ("224.0.0.1", "multicast"),
            ("239.255.255.255", "multicast"),
            ("240.0.0.1", "reserved"),
            ("255.255.255.254", "reserved"),
            ("255.255.255.255", "broadcast"),
            ("::", "unspecified"),
            ("::1", "loopback"),
            ("::127.0.0.1", "IPv4-compatible loopback"),
            ("::8.8.8.8", "IPv4-compatible public"),
            ("fc00::1", "ULA"),
            ("fd7a:115c:a1e0::1", "ULA, tailnet"),
            ("fe80::1", "link-local"),
            ("fec0::1", "site-local"),
            ("ff02::1", "multicast"),
            ("100::1", "discard-only"),
            ("2001::1", "Teredo"),
            ("2001:0:4136:e378:8000:63bf:3fff:fdd2", "Teredo"),
            ("2001:db8::1", "documentation"),
            ("2001:db8:ffff::1", "documentation"),
            ("::ffff:127.0.0.1", "IPv4-mapped loopback"),
            ("::ffff:10.0.0.1", "IPv4-mapped private"),
            ("::ffff:100.64.0.1", "IPv4-mapped CGNAT"),
            ("::ffff:169.254.169.254", "IPv4-mapped metadata"),
            ("::ffff:192.0.2.1", "IPv4-mapped documentation"),
            ("192.88.99.1", "deprecated 6to4 relay anycast"),
            ("192.88.99.255", "deprecated 6to4 relay anycast"),
            ("::ffff:192.88.99.1", "IPv4-mapped 6to4 relay anycast"),
            ("::ffff:0:8.8.8.8", "SIIT of a public address"),
            ("::ffff:0:127.0.0.1", "SIIT of loopback"),
            ("::ffff:0:a9fe:a9fe", "SIIT of the metadata endpoint"),
            ("64:ff9b::808:808", "NAT64 of a public address"),
            ("2002:808:808::1", "6to4 of a public address"),
            ("64:ff9b::7f00:1", "NAT64 of loopback"),
            ("64:ff9b::c0a8:101", "NAT64 of 192.168.1.1"),
            ("64:ff9b::a9fe:a9fe", "NAT64 of the metadata endpoint"),
            ("64:ff9b:1::1", "local-use NAT64"),
            ("64:ff9b:1:ffff::808:808", "local-use NAT64 of a public address"),
            ("2002:c0a8:101::1", "6to4 of 192.168.1.1"),
            ("2002:c0a8:5601::1", "6to4 of 192.168.86.1"),
            ("2002:7f00:1::", "6to4 of loopback"),
        ] {
            assert!(!is_public(ip.parse().unwrap()), "{ip} ({what}) counted as public");
        }
    }
}
