//! Per-service environment. The three services read bare names (`CACHE_DIR`, `METRICS_TOKEN`,
//! `CONFIG_KEY`, `SCOUT_ORIGINS`, ...) and several of them read the same ones, so in one process a bare
//! name could only ever configure all of them alike.
//!
//! Instead, each service is configured by `REEL_<NAME>`, `SUBS_<NAME>` or `REMUX_<NAME>`, which wins
//! over the bare `<NAME>` for that service only. A bare name with no prefixed twin still reaches every
//! service, which is what you want for `FFMPEG_PATH` and not what you want for `CACHE_DIR`.
//!
//! Configuration is resolved without changing the process environment: each service's
//! `Config::from_lookup` receives this lookup directly.

use std::env;

/// Read one service setting. A prefixed value wins even when it is empty; otherwise the bare value is
/// the compatibility fallback. For a mounted service, Hearth's public base supplies `PUBLIC_BASE_URL`
/// only when that service has no prefixed value, preserving the URL shape the old scoped loader produced.
pub fn lookup(prefix: &str, mount: Option<&str>, name: &str) -> Option<String> {
    if let Ok(value) = env::var(format!("{prefix}{name}")) {
        return Some(value);
    }
    if name == "PUBLIC_BASE_URL" {
        if let (Some(mount), Ok(base)) = (mount, env::var("HEARTH_PUBLIC_BASE_URL")) {
            return Some(format!("{}/{mount}", base.trim_end_matches('/')));
        }
    }
    env::var(name).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    // The environment is process-wide and tests run in parallel, so each test uses names no other
    // test (or service) touches.

    #[test]
    fn a_prefixed_name_wins_for_the_service_it_names() {
        env::set_var("SCOPED_A_X", "bare");
        env::set_var("ALPHA_SCOPED_A_X", "alpha");
        env::set_var("BETA_SCOPED_A_X", "beta");
        assert_eq!(lookup("ALPHA_", None, "SCOPED_A_X").as_deref(), Some("alpha"));
        assert_eq!(lookup("BETA_", None, "SCOPED_A_X").as_deref(), Some("beta"));
    }

    #[test]
    fn a_bare_name_with_no_twin_reaches_every_service() {
        env::set_var("SCOPED_B_Y", "shared");
        assert_eq!(lookup("GAMMA_", None, "SCOPED_B_Y").as_deref(), Some("shared"));
    }

    #[test]
    fn an_empty_prefixed_value_still_suppresses_the_bare_fallback() {
        env::set_var("SCOPED_C_Z", "bare");
        env::set_var("DELTA_SCOPED_C_Z", "");
        assert_eq!(lookup("DELTA_", None, "SCOPED_C_Z").as_deref(), Some(""));
    }

    #[test]
    fn the_public_base_gets_the_mount_unless_the_service_names_its_own() {
        env::set_var("HEARTH_PUBLIC_BASE_URL", "https://node.example/");
        env::set_var("EPSILON_PUBLIC_BASE_URL", "https://own.example");
        assert_eq!(
            lookup("ZETA_", Some("reel"), "PUBLIC_BASE_URL").as_deref(),
            Some("https://node.example/reel")
        );
        assert_eq!(
            lookup("EPSILON_", Some("reel"), "PUBLIC_BASE_URL").as_deref(),
            Some("https://own.example")
        );
        env::remove_var("HEARTH_PUBLIC_BASE_URL");
        env::remove_var("EPSILON_PUBLIC_BASE_URL");
    }
}
