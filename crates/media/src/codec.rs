//! The configuration records the video codecs keep in a container: MP4's `avcC`, `hvcC`, `av1C` and the Dolby
//! Vision `dvcC`/`dvvC`/`dvwC` boxes, and Matroska's CodecPrivate and BlockAddIDExtraData, which hold the same
//! bytes (ISO/IEC 14496-15, the AV1 ISOBMFF binding, Dolby's ISOBMFF spec).
//!
//! Each reader takes the record as a slice and answers `None` (or 0, for a bit depth) when it is too short or isn't
//! the codec's, so a caller never needs a length check of its own. They say what the record says and decide
//! nothing: what to do with a bit depth, a profile or an HDR claim is each probe's own.

/// The boxes a Dolby Vision configuration record travels in. Per Dolby's ISOBMFF spec, `dvcC` holds profiles up to
/// 7, `dvvC` 8 to 10, and `dvwC` anything past 10. Matroska uses the same four characters, as a big-endian
/// number, for its BlockAddIDType (`mkv::is_dovi_block_addition`).
pub const DOVI_CONFIG_TYPES: [[u8; 4]; 3] = [*b"dvcC", *b"dvvC", *b"dvwC"];

/// A `DOVIDecoderConfigurationRecord`, which starts:
///
/// ```text
/// byte 0   dv_version_major                 8 bits
/// byte 1   dv_version_minor                 8 bits
/// byte 2   dv_profile                       high 7 bits
///          dv_level                         low bit of byte 2 + high 5 bits of byte 3
/// byte 3   rpu / el / bl_present_flag       low 3 bits
/// byte 4   dv_bl_signal_compatibility_id    high 4 bits
/// ```
///
/// Source: Dolby, "Dolby Vision Streams Within the ISO Base Media File Format", and the same fields read in the
/// same order by ffmpeg's `libavformat/dovi_isom.c` (`ff_isom_parse_dvcc_dvvc`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Dovi {
    pub profile: u8,
    /// Dolby Vision's own level (1-13).
    pub level: u8,
    /// The base layer's compatibility id; `None` where the record ends before byte 4.
    pub compat: Option<u8>,
}

/// Reads a Dolby Vision record. Like ffmpeg, refuses one shorter than 4 bytes: the profile and level are there,
/// the compatibility id may not be.
pub fn dovi(record: &[u8]) -> Option<Dovi> {
    let b = record.get(..4)?;
    Some(Dovi {
        profile: b[2] >> 1,
        level: ((b[2] & 1) << 5) | (b[3] >> 3),
        compat: record.get(4).map(|c| c >> 4),
    })
}

/// The start of an `AVCDecoderConfigurationRecord`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Avc {
    /// configurationVersion, 1 in every record that exists.
    pub version: u8,
    /// AVCProfileIndication: 66 Baseline, 77 Main, 88 Extended, 100 High, 110 High 10, ...
    pub profile: u8,
    pub constraints: u8,
    /// AVCLevelIndication, the level times 10.
    pub level: u8,
}

pub fn avc(record: &[u8]) -> Option<Avc> {
    let b = record.get(..4)?;
    Some(Avc { version: b[0], profile: b[1], constraints: b[2], level: b[3] })
}

/// The luma bit depth an `avcC` implies, 0 when it can't tell: by AVCProfileIndication (byte 1). High 10 (110)
/// answers 10 even for 8-bit content, because a decoder without High 10 refuses the profile whatever the samples
/// are. Baseline, Main, Extended and High (66, 77, 88, 100) are 8-bit by definition. The 4:2:2 and 4:4:4 profiles
/// allow either depth and stay unknown: reading theirs means parsing the SPS, for profiles no release uses.
pub fn avc_bit_depth(record: &[u8]) -> u8 {
    match record.get(1) {
        Some(110) => 10,
        Some(66 | 77 | 88 | 100) => 8,
        _ => 0,
    }
}

/// The front of an `HEVCDecoderConfigurationRecord`: through general_level_idc.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Hevc {
    /// general_profile_space, 0 to 3.
    pub profile_space: u8,
    /// general_tier_flag: High tier.
    pub high_tier: bool,
    /// general_profile_idc: 1 Main, 2 Main 10, ...
    pub profile: u8,
    /// general_profile_compatibility_flags, bit 31 being profile 0 (RFC 6381 writes them bit-reversed).
    pub compat_flags: u32,
    /// The six general_constraint_indicator_flags bytes.
    pub constraints: [u8; 6],
    /// general_level_idc, the level times 30.
    pub level: u8,
}

pub fn hevc(record: &[u8]) -> Option<Hevc> {
    let b = record.get(..13)?;
    Some(Hevc {
        profile_space: b[1] >> 6,
        high_tier: b[1] & 0x20 != 0,
        profile: b[1] & 0x1F,
        compat_flags: u32::from_be_bytes([b[2], b[3], b[4], b[5]]),
        constraints: [b[6], b[7], b[8], b[9], b[10], b[11]],
        level: b[12],
    })
}

/// bitDepthLumaMinus8, the low 3 bits of byte 17 of an `hvcC`, plus 8; 0 when the record doesn't reach it. The 17
/// bytes before it are version (1), profile (1), compatibility flags (4), constraint flags (6), level (1),
/// min_spatial_segmentation (2), parallelismType (1) and chromaFormat (1).
pub fn hevc_bit_depth(record: &[u8]) -> u8 {
    record.get(17).map_or(0, |b| (b & 0x07) + 8)
}

/// The front of an `AV1CodecConfigurationRecord`: marker and version (0x81), seq_profile (3 bits) and
/// seq_level_idx_0 (5), then seq_tier_0, high_bitdepth, twelve_bit, monochrome, chroma_subsampling_x and _y and
/// chroma_sample_position (2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Av1 {
    pub profile: u8,
    pub level: u8,
    pub high_tier: bool,
    pub high_bitdepth: bool,
    pub twelve_bit: bool,
    pub monochrome: bool,
    pub subsampling_x: u8,
    pub subsampling_y: u8,
    pub sample_position: u8,
}

impl Av1 {
    /// 8, or with high_bitdepth 10, or 12 with twelve_bit as well.
    pub fn bit_depth(&self) -> u8 {
        match (self.high_bitdepth, self.twelve_bit) {
            (false, _) => 8,
            (true, false) => 10,
            (true, true) => 12,
        }
    }
}

/// `None` for a record under 3 bytes, or one that doesn't start with the marker and version byte 0x81.
pub fn av1(record: &[u8]) -> Option<Av1> {
    let b = record.get(..3)?;
    if b[0] != 0x81 {
        return None;
    }
    Some(Av1 {
        profile: b[1] >> 5,
        level: b[1] & 0x1F,
        high_tier: b[2] & 0x80 != 0,
        high_bitdepth: b[2] & 0x40 != 0,
        twelve_bit: b[2] & 0x20 != 0,
        monochrome: b[2] & 0x10 != 0,
        subsampling_x: (b[2] >> 3) & 1,
        subsampling_y: (b[2] >> 2) & 1,
        sample_position: b[2] & 3,
    })
}

/// An `av1C`'s bit depth; 0 when it isn't one.
pub fn av1_bit_depth(record: &[u8]) -> u8 {
    av1(record).map_or(0, |a| a.bit_depth())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dolby_vision_record_gives_profile_level_and_compatibility() {
        // Profile 8 at level 6, base layer HDR10 (compatibility 1): the record mkvmerge and dovi_tool write.
        let record = [1, 0, 8 << 1, 6 << 3 | 0b101, 1 << 4, 0, 0, 0];
        assert_eq!(dovi(&record), Some(Dovi { profile: 8, level: 6, compat: Some(1) }));
        // The level's top bit is the low bit of byte 2.
        assert_eq!(dovi(&[1, 0, 5 << 1 | 1, 0b0001_1000, 0]).map(|d| (d.profile, d.level)), Some((5, 35)));
        // Four bytes are enough for the profile; the compatibility id is not there.
        assert_eq!(dovi(&record[..4]), Some(Dovi { profile: 8, level: 6, compat: None }));
        assert_eq!(dovi(&record[..3]), None);
        assert_eq!(dovi(&[]), None);
    }

    #[test]
    fn avc_profiles_give_their_bit_depth_and_unknown_ones_none() {
        for (profile, depth) in [(66, 8), (77, 8), (88, 8), (100, 8), (110, 10), (122, 0), (244, 0), (0, 0)] {
            assert_eq!(avc_bit_depth(&[1, profile, 0, 40]), depth, "profile {profile}");
        }
        assert_eq!(avc_bit_depth(&[1]), 0);
        assert_eq!(
            avc(&[1, 0x64, 0, 0x28, 0xFF]),
            Some(Avc { version: 1, profile: 100, constraints: 0, level: 40 })
        );
        assert_eq!(avc(&[1, 0x64, 0]), None);
    }

    #[test]
    fn an_hevc_record_gives_profile_tier_level_and_depth() {
        // Main 10 (profile 2) in the High tier, compatible with Main 10 only, constraint bytes B0.., level 5.1.
        let mut record = [0u8; 23];
        record[0] = 1;
        record[1] = 0x20 | 2;
        record[2..6].copy_from_slice(&0x2000_0000u32.to_be_bytes());
        record[6] = 0xB0;
        record[12] = 153;
        record[17] = 0xF8 | 2; // bitDepthLumaMinus8 in the low three bits, reserved ones above
        assert_eq!(
            hevc(&record),
            Some(Hevc {
                profile_space: 0,
                high_tier: true,
                profile: 2,
                compat_flags: 0x2000_0000,
                constraints: [0xB0, 0, 0, 0, 0, 0],
                level: 153,
            })
        );
        assert_eq!(hevc_bit_depth(&record), 10);
        assert_eq!(hevc(&record[..12]), None);
        assert!(hevc(&record[..13]).is_some());
        assert_eq!(hevc_bit_depth(&record[..17]), 0, "a record that stops before byte 17 says nothing");
        assert_eq!(
            hevc(&[1, 0xC1 | 0x20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]).map(|h| h.profile_space),
            Some(3)
        );
    }

    #[test]
    fn an_av1_record_gives_profile_level_tier_and_depth() {
        // Main profile, level index 13, Main tier, 10-bit, 4:2:0 with the sample position 0.
        let record = [0x81, 13, 0x40 | 1 << 3 | 1 << 2, 0];
        assert_eq!(
            av1(&record),
            Some(Av1 {
                profile: 0,
                level: 13,
                high_tier: false,
                high_bitdepth: true,
                twelve_bit: false,
                monochrome: false,
                subsampling_x: 1,
                subsampling_y: 1,
                sample_position: 0,
            })
        );
        assert_eq!(av1_bit_depth(&record), 10);
        assert_eq!(av1_bit_depth(&[0x81, 0, 0x60]), 12);
        assert_eq!(av1_bit_depth(&[0x81, 0, 0x00]), 8);
        assert_eq!(av1_bit_depth(&[0x81, 0, 0xE0]), 12);
        assert_eq!(av1(&[0x80, 13, 0]), None, "not the marker and version byte");
        assert_eq!(av1(&[0x81, 13]), None);
        assert_eq!(
            av1(&[0x81, 2 << 5 | 31, 0x80]).map(|a| (a.profile, a.level, a.high_tier)),
            Some((2, 31, true))
        );
    }

    #[test]
    fn no_record_of_any_short_length_makes_a_reader_panic() {
        for len in 0..40usize {
            for fill in [0u8, 1, 0x7F, 0x80, 0x81, 0xFF] {
                let record = vec![fill; len];
                let _ = (dovi(&record), avc(&record), avc_bit_depth(&record), hevc(&record));
                let _ = (hevc_bit_depth(&record), av1(&record), av1_bit_depth(&record));
            }
        }
    }
}
