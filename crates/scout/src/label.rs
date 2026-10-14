//! Port of `internal/scout/label.go`. Work package 5.

/// Go `mib` (label.go).
pub(crate) const MIB: i64 = 1_048_576;

/// Go `bingeGroup`: the release identity two episodes of one season share, or differ on.
pub(crate) fn binge_group(_lower: &str, _title: &str) -> String {
    todo!()
}

/// Go `releaseGroup`: the trailing `-GROUP` of a release name, lowercased; "" when there is none.
pub(crate) fn release_group(_title: &str) -> String {
    todo!()
}
