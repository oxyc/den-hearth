//! One public-address classifier. Scout's household sources, Subtitles' resync hops and Remux's source fetches all
//! refuse a URL that leads off the public internet, and each had its own list, missing ranges the others covered.
//! This is the union of them, so a range added here is refused everywhere.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Whether `ip` is an address on the public internet: not loopback, private (RFC 1918), CGNAT (100.64/10, where a
/// tailnet lives), link-local (the 169.254.169.254 metadata endpoint), unspecified, broadcast, multicast,
/// documentation, benchmarking or reserved space; and for IPv6 not ULA, link-local, site-local, Teredo, the
/// documentation prefix, the discard prefix or the `::/96` block (which holds `::` and `::1` and the deprecated
/// IPv4-compatible form).
///
/// An IPv6 form that carries an IPv4 address (mapped, the well-known NAT64 `64:ff9b::/96`, 6to4 `2002::/16`) is
/// judged as the IPv4 address it carries, since that is the host a gateway on the path reaches. Local-use NAT64
/// (`64:ff9b:1::/48`) has no fixed embedding and is never public.
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
    let embedded = |hi: u16, lo: u16| {
        let [a, b] = hi.to_be_bytes();
        let [c, d] = lo.to_be_bytes();
        Ipv4Addr::new(a, b, c, d)
    };
    if s[..6] == [0x64, 0xff9b, 0, 0, 0, 0] {
        return is_public_v4(embedded(s[6], s[7]));
    }
    if s[0] == 0x2002 {
        return is_public_v4(embedded(s[1], s[2]));
    }
    !(s[..6] == [0; 6] // ::/96
        || v6.is_multicast()
        || s[0] & 0xfe00 == 0xfc00 // ULA fc00::/7
        || s[0] & 0xffc0 == 0xfe80 // link-local fe80::/10
        || s[0] & 0xffc0 == 0xfec0 // deprecated site-local fec0::/10
        || (s[0] == 0x0064 && s[1] == 0xff9b && s[2] == 1) // local-use NAT64 64:ff9b:1::/48
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
        || (a == 198 && b & 0xfe == 18) // benchmarking 198.18/15
        || a >= 240) // reserved 240/4 (and the broadcast address)
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
            "::ffff:8.8.8.8",   // IPv4-mapped public
            "64:ff9b::808:808", // NAT64 of 8.8.8.8
            "2002:808:808::1",  // 6to4 of 8.8.8.8
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
