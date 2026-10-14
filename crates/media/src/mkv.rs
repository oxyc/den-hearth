//! The Matroska element ids both probes read, in the specification's notation (marker bits kept), and the two
//! rules about them they would otherwise each write down.
//!
//! Elements only one probe reads (Remux's Cues, Tags and Cluster ids, Scout's none) stay with that probe.

pub const EBML: u32 = 0x1A45_DFA3;
pub const SEGMENT: u32 = 0x1853_8067;
pub const TRACKS: u32 = 0x1654_AE6B;
pub const TRACK_ENTRY: u32 = 0xAE;
pub const TRACK_TYPE: u32 = 0x83;
pub const CODEC_ID: u32 = 0x86;
pub const CODEC_PRIVATE: u32 = 0x63A2;
pub const DEFAULT_DURATION: u32 = 0x23_E383;
pub const LANGUAGE: u32 = 0x22_B59C;
/// The BCP-47 language element.
pub const LANGUAGE_BCP47: u32 = 0x22_B59D;
pub const VIDEO: u32 = 0xE0;
pub const PIXEL_WIDTH: u32 = 0xB0;
pub const PIXEL_HEIGHT: u32 = 0xBA;
pub const COLOUR: u32 = 0x55B0;
pub const TRANSFER_CHARACTERISTICS: u32 = 0x55BA;
pub const AUDIO: u32 = 0xE1;
pub const CHANNELS: u32 = 0x9F;
pub const BLOCK_ADDITION_MAPPING: u32 = 0x41E4;
pub const BLOCK_ADD_ID_TYPE: u32 = 0x41E7;
pub const BLOCK_ADD_ID_EXTRA_DATA: u32 = 0x41ED;

/// A TrackEntry's TrackType.
pub const TRACK_TYPE_VIDEO: u8 = 1;
pub const TRACK_TYPE_AUDIO: u8 = 2;
pub const TRACK_TYPE_SUBTITLE: u8 = 17;

/// Whether a BlockAddIDType names a Dolby Vision configuration: it is the box name the record travels under in an
/// MP4, read as a big-endian number (`dvcC`, `dvvC`, `dvwC`). The mapping's BlockAddIDExtraData then holds the
/// record itself (Matroska codec mappings; what ffmpeg's `mkv_parse_block_addition_mappings` reads).
pub fn is_dovi_block_addition(block_add_id_type: u64) -> bool {
    crate::codec::DOVI_CONFIG_TYPES.iter().any(|tag| block_add_id_type == u64::from(u32::from_be_bytes(*tag)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_dolby_vision_configuration_boxes_are_dolby_vision() {
        for tag in [b"dvcC", b"dvvC", b"dvwC"] {
            assert!(is_dovi_block_addition(u64::from(u32::from_be_bytes(*tag))));
        }
        assert!(!is_dovi_block_addition(u64::from(u32::from_be_bytes(*b"mvcC"))));
        assert!(!is_dovi_block_addition(0));
        assert!(!is_dovi_block_addition(u64::from(u32::from_be_bytes(*b"dvcC")) | 1 << 32));
    }
}
