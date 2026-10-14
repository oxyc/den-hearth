//! Go: `internal/scout/core_test.go` (639 lines). Work package 6.
//!
//! A grab bag: config, stream ids, play tokens and the memory cache are this package's own modules, but
//! `TestPickEpisodeFile`, `TestSelectFileIDTorBox`, `TestPickFileIDPrefersEpisodeMatch`, `TestCleanLabel*`
//! and `TestStreamAttributes` exercise modules owned by work packages 1 and 5. Port the whole file here;
//! those tests stay red until the module they exercise lands.

use crate::util::b64url_encode;

/// Go `blob` (core_test.go:11): a JSON string as an unpadded base64url config path segment.
pub(crate) fn blob(json_str: &str) -> String {
    b64url_encode(json_str.as_bytes())
}
