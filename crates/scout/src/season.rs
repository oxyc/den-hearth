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

/// Go `pickEpisodeFile`: the index of the file in a season pack that holds `season`/`episode`. The
/// `Err` side stands in for Go's `error`; the season porter settles its real type.
pub(crate) fn pick_episode_file(_files: &[TorrentFile], _season: i64, _episode: i64) -> Result<Option<i64>, String> {
    todo!()
}

/// Go `episodePatterns`' result: the matchers for one season and episode. Its fields come with the
/// implementation.
pub(crate) struct EpisodePatterns;

/// Go `episodePatterns`: the matchers for `season`/`episode`, including the absolute-numbering forms.
pub(crate) fn episode_patterns(_season: i64, _episode: i64) -> EpisodePatterns {
    todo!()
}

/// Go `matchesEpisode`: whether a release title names the episode `p` was built for.
pub(crate) fn matches_episode(_title: &str, _p: &EpisodePatterns) -> bool {
    todo!()
}
