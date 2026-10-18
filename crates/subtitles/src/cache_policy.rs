//! Pure cache/backoff policy shared by subtitle-serving workflows.
//!
//! Network downloads, translation charges and alignment subprocesses stay in their cohesive
//! modules; this module only decides names and scopes of durable answers and short-lived failures.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlignmentKind {
    None,
    Reference,
    Audio,
}

impl AlignmentKind {
    pub fn from_inputs(has_audio_url: bool, has_reference: bool) -> Self {
        match (has_audio_url, has_reference) {
            (true, _) => Self::Audio,
            (false, true) => Self::Reference,
            (false, false) => Self::None,
        }
    }

    pub fn wanted(self) -> bool {
        self != Self::None
    }

    pub fn serve_reason(self, sparse_fallback: bool) -> &'static str {
        if sparse_fallback {
            return "sparse_fallback";
        }
        match self {
            Self::Audio => "tier2",
            Self::Reference => "tier1",
            Self::None => "no_sync_needed",
        }
    }
}

/// A subprocess failure is shared because it is a fact about the work. A reference-download
/// refusal is credential-scoped because quota and revocation belong to one OpenSubtitles identity.
pub fn sync_failure_markers(prefix: &str, cache_key: &str, credential_identity: u64) -> (String, String) {
    (format!("{prefix}{cache_key}"), format!("{prefix}{credential_identity:016x}:{cache_key}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audio_wins_and_sparse_only_changes_the_serving_reason() {
        let kind = AlignmentKind::from_inputs(true, true);
        assert_eq!(kind, AlignmentKind::Audio);
        assert_eq!(kind.serve_reason(false), "tier2");
        assert_eq!(kind.serve_reason(true), "sparse_fallback");
    }

    #[test]
    fn failure_scope_has_distinct_global_and_credential_keys() {
        assert_eq!(
            sync_failure_markers("syncfail:", "body", 0x2a),
            ("syncfail:body".into(), "syncfail:000000000000002a:body".into())
        );
    }
}
