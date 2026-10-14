//! Percent-decoding, hand-rolled so no addon needs a URL or regex dependency for it.

/// Minimal percent-decode (`%XX`, and `+` to a space) — enough for the extra-args a Stremio client sends and for
/// query parameters. A `%` not followed by two hex digits stays as it came; invalid UTF-8 is replaced.
pub fn percent_decode(s: &str) -> String {
    decode(s, true)
}

/// Percent-decoding for a PATH segment, where `+` is a literal plus. Form-decoding a path turns "HDR10+.WEB-DL" into
/// "HDR10 .WEB-DL", which is then what the release name is ranked on.
pub fn percent_decode_path(s: &str) -> String {
    decode(s, false)
}

fn decode(s: &str, plus_is_space: bool) -> String {
    let needs_decoding = s.contains('%') || (plus_is_space && s.contains('+'));
    if !needs_decoding {
        return s.to_owned();
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                if let (Some(h), Some(l)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                    out.push(h << 4 | l);
                    i += 3;
                    continue;
                }
                out.push(b'%');
                i += 1;
            }
            b'+' if plus_is_space => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plus_is_a_space_in_a_query_and_a_plus_in_a_path() {
        assert_eq!(percent_decode("a+b%20c"), "a b c");
        assert_eq!(percent_decode_path("HDR10+.WEB-DL%2Fx"), "HDR10+.WEB-DL/x");
    }

    #[test]
    fn a_malformed_escape_is_left_as_it_came() {
        assert_eq!(percent_decode_path("100%"), "100%");
        assert_eq!(percent_decode_path("%zz1"), "%zz1");
        assert_eq!(percent_decode_path("%4"), "%4");
        assert_eq!(percent_decode_path("tt1:2:3%41"), "tt1:2:3A");
        assert_eq!(percent_decode_path("%e2%82%ac"), "\u{20ac}");
    }
}
