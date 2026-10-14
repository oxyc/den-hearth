//! Port of `internal/scout/validate.go`. Work package 6.
//!
//! Checks a debrid token at the moment it is pasted: one unauthenticated read per service, judged from the right
//! place for that service. A provider being down is never reported as a wrong token.

use crate::config::DebridService;
use crate::hostlimit::HostLimiter;
use crate::scrape::Doer;

/// Go `validateResult`. `throttled` is set when our own pacing refused the check; the route answers 429 for it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ValidateResult {
    pub valid: bool,
    /// Why not, in words a configure page can show. Never the upstream body.
    pub reason: String,
    pub throttled: bool,
}

/// Go `validateBudget`: how long the whole check may take.
pub const VALIDATE_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);

/// Go `maxValidateBody`: bounds the request body.
pub const MAX_VALIDATE_BODY: usize = 4 << 10;

/// Go `validateDebridToken`: asks the service whether this token identifies an account. The Go function reads the
/// package-level `validateLimiter`; the port takes it as `limiter` so a test can pass its own without a global swap.
pub async fn validate_debrid_token(doer: &dyn Doer, limiter: &HostLimiter, svc: DebridService, token: &str) -> ValidateResult {
    todo!("validate.go validateDebridToken")
}
