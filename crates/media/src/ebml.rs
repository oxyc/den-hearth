//! EBML, the element syntax Matroska is written in: a variable-length id, a variable-length size, then that many
//! bytes of payload, which for a master element are more elements.
//!
//! Everything here takes a slice and answers `None` rather than reading past it, and walks a master element's
//! children rather than searching for an id: a SeekHead stores the ids of the elements it points at as data, so
//! scanning for the four bytes of an id finds the pointer before the element.

/// The width a leading byte announces: its leading zero bits, plus one. A zero byte announces 9, which no id
/// (at most 4) or size (at most 8) can have.
fn width(first: u8) -> usize {
    first.leading_zeros() as usize + 1
}

/// An element id and its length in bytes. The marker bits stay in the id, as the specification writes ids
/// (`0x1A45DFA3`).
pub fn read_id(buf: &[u8]) -> Option<(u32, usize)> {
    let n = width(*buf.first()?);
    if n > 4 || buf.len() < n {
        return None;
    }
    Some((buf[..n].iter().fold(0u32, |id, &b| id << 8 | u32::from(b)), n))
}

/// The size at `at`: its value, its length in bytes, and whether it is EBML's "unknown size" (every value bit
/// set), which a Segment or Cluster being written to a stream carries.
pub fn read_size(buf: &[u8], at: usize) -> Option<(u64, usize, bool)> {
    let first = *buf.get(at)?;
    let n = width(first);
    let end = at.checked_add(n)?;
    if n > 8 || end > buf.len() {
        return None;
    }
    // Widened before shifting: an 8-byte size leaves no value bits in the first byte, and a u8 shifted by 8
    // overflows.
    let mask = (0xFFu32 >> n) as u8;
    let mut value = u64::from(first & mask);
    let mut unknown = first & mask == mask;
    for &b in &buf[at + 1..end] {
        value = value << 8 | u64::from(b);
        unknown &= b == 0xFF;
    }
    Some((value, n, unknown))
}

/// An element header: its id, its payload size (`None` for the unknown size) and the header's length.
pub fn header(buf: &[u8]) -> Option<(u32, Option<u64>, usize)> {
    let (id, id_len) = read_id(buf)?;
    let (size, size_len, unknown) = read_size(buf, id_len)?;
    Some((id, (!unknown).then_some(size), id_len + size_len))
}

/// The children of a master element's payload, as `(id, payload)`. Stops at the first malformed, truncated or
/// unknown-sized child rather than guessing past it; [`Children::reached_end`] tells that from running out.
pub fn children(buf: &[u8]) -> Children<'_> {
    Children { buf, pos: 0 }
}

pub struct Children<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for Children<'a> {
    type Item = (u32, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        let (id, size, header_len) = header(self.buf.get(self.pos..)?)?;
        let start = self.pos + header_len;
        let end = start.checked_add(usize::try_from(size?).ok()?)?;
        if end > self.buf.len() {
            return None;
        }
        self.pos = end;
        Some((id, &self.buf[start..end]))
    }
}

impl Children<'_> {
    /// Whether every byte has been walked: false after the iterator ended at a child it could not read, for a
    /// caller that discards a master element it cannot read to the end.
    pub fn reached_end(&self) -> bool {
        self.pos >= self.buf.len()
    }
}

/// The payload of the first child with this id.
pub fn child(buf: &[u8], want: u32) -> Option<&[u8]> {
    children(buf).find(|(id, _)| *id == want).map(|(_, body)| body)
}

/// The unsigned value of the first child with this id, 0 when there is none.
pub fn child_uint(buf: &[u8], want: u32) -> u64 {
    child(buf, want).map_or(0, uint)
}

/// A big-endian unsigned integer. The specification allows eight bytes at most; a longer value reads its first
/// eight.
pub fn uint(buf: &[u8]) -> u64 {
    buf.iter().take(8).fold(0u64, |value, &b| value << 8 | u64::from(b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn element_headers_decode_sizes_and_the_unknown_marker() {
        assert_eq!(header(&[0x1A, 0x45, 0xDF, 0xA3, 0x9F]), Some((0x1A45_DFA3, Some(31), 5)));
        // A Segment of unknown size: all value bits of an 8-byte size set.
        let unknown = [0x18, 0x53, 0x80, 0x67, 0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        assert_eq!(header(&unknown), Some((0x1853_8067, None, 12)));
        // A one-byte unknown size is 0xFF, and 0x7F is a size.
        assert_eq!(header(&[0xAE, 0xFF]), Some((0xAE, None, 2)));
        assert_eq!(header(&[0xAE, 0xFE]), Some((0xAE, Some(0x7E), 2)));
        assert_eq!(header(&[0x00]), None, "a zero byte is no valid id");
        assert_eq!(header(&[0x1A, 0x45]), None, "truncated");
        assert_eq!(header(&[0x08, 0, 0, 0, 0, 0]), None, "a five-byte id does not exist");
    }

    #[test]
    fn an_id_keeps_its_marker_bits_and_a_size_loses_them() {
        assert_eq!(read_id(&[0xAE, 0x81]), Some((0xAE, 1)));
        assert_eq!(read_id(&[0x22, 0xB5, 0x9C, 0x83]), Some((0x22_B59C, 3)));
        assert_eq!(read_id(&[0x22, 0xB5]), None);
        assert_eq!(read_size(&[0xAE, 0x40, 0x7F], 1), Some((0x7F, 2, false)));
        assert_eq!(read_size(&[0x01, 0, 0, 0, 0, 0, 0, 5], 0), Some((5, 8, false)));
        assert_eq!(read_size(&[0x40], 0), None, "a two-byte size cut after its first byte");
        assert_eq!(read_size(&[0x40], 5), None, "past the end");
        assert_eq!(read_size(&[0x00, 0, 0, 0, 0, 0, 0, 0, 0], 0), None, "a nine-byte size does not exist");
    }

    #[test]
    fn children_stop_where_the_bytes_do() {
        // Two one-byte children, then one claiming more than is left, then bytes that are never read.
        let buf = [0xD7, 0x81, 1, 0x83, 0x81, 2, 0xAE, 0x85, 9, 0xD7, 0x81, 3];
        let got: Vec<_> = children(&buf).collect();
        assert_eq!(got, vec![(0xD7, &[1u8][..]), (0x83, &[2u8][..])]);
        assert_eq!(child(&buf, 0x83), Some(&[2u8][..]));
        assert_eq!(child(&buf, 0xAE), None, "a child that overruns the buffer is not one");
        assert_eq!(child_uint(&buf, 0x83), 2);
        assert_eq!(child_uint(&buf, 0x84), 0, "absent");
        // An unknown-sized child ends the walk: its extent can't be known from here.
        assert_eq!(children(&[0xD7, 0xFF, 1, 2, 3]).count(), 0);
    }

    #[test]
    fn a_walk_says_whether_it_ended_at_the_end_or_at_a_bad_child() {
        let mut whole = children(&[0xD7, 0x81, 1, 0x83, 0x81, 2]);
        assert!(!whole.reached_end());
        assert_eq!(whole.by_ref().count(), 2);
        assert!(whole.reached_end());
        let mut cut = children(&[0xD7, 0x81, 1, 0x83, 0x85, 2]);
        assert_eq!(cut.by_ref().count(), 1);
        assert!(!cut.reached_end(), "the second child overruns the buffer");
        assert!(children(&[]).reached_end());
    }

    #[test]
    fn an_unsigned_value_is_big_endian_and_at_most_eight_bytes() {
        assert_eq!(uint(&[]), 0);
        assert_eq!(uint(&[0x02, 0x7C, 0x6D, 0x5D]), 0x027C_6D5D);
        assert_eq!(uint(&[1, 0, 0, 0, 0, 0, 0, 0, 0xFF]), 1 << 56, "a ninth byte is not read");
    }

    #[test]
    fn no_short_prefix_makes_the_walkers_panic() {
        // Every one-, two- and three-byte buffer, with the overflow checks the test profile has on.
        for a in 0..=255u8 {
            let _ = header(&[a]);
            for b in 0..=255u8 {
                let _ = header(&[a, b]);
                let _ = read_size(&[a, b], 0);
                let _ = children(&[a, b, 0x81]).count();
                for c in [0u8, 1, 0x7F, 0x80, 0xFF] {
                    let _ = header(&[a, b, c]);
                    let _ = children(&[a, b, c, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]).count();
                }
            }
        }
    }
}
