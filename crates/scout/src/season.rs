//! Port of `internal/scout/season.go`: picking the right file out of a season pack (SxxExx, 1x02,
//! "season 1 episode 2", absolute numbering). `TorrentFile` is FROZEN (see PORTING.md): every store lists
//! files in this shape.

/// FROZEN. Go `TorrentFile`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TorrentFile {
    pub index: i64,
    pub name: String,
    pub size_bytes: Option<i64>,
}
