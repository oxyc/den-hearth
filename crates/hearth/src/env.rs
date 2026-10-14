//! Per-service environment. The three services read bare names (`CACHE_DIR`, `METRICS_TOKEN`,
//! `CONFIG_KEY`, `SCOUT_ORIGINS`, ...) and several of them read the same ones, so in one process a bare
//! name could only ever configure all of them alike.
//!
//! Instead, each service is configured by `REEL_<NAME>`, `SUBS_<NAME>` or `REMUX_<NAME>`, which wins
//! over the bare `<NAME>` for that service only. A bare name with no prefixed twin still reaches every
//! service, which is what you want for `FFMPEG_PATH` and not what you want for `CACHE_DIR`.
//!
//! This runs while the process is still single-threaded, before the runtime exists, because the
//! services read the environment inside `Config::from_env` and nowhere else.

use std::collections::HashMap;
use std::env;

/// Run `load` with every `<prefix><NAME>` variable also visible as `<NAME>`, then put the environment
/// back as it was. `mount`, when given, is the path the service is served under: with
/// `HEARTH_PUBLIC_BASE_URL` set and no `<prefix>PUBLIC_BASE_URL`, the service's `PUBLIC_BASE_URL` becomes
/// `<HEARTH_PUBLIC_BASE_URL>/<mount>`, so the absolute URLs it hands out carry the prefix hearth routes on.
pub fn scoped<T>(prefix: &str, mount: Option<&str>, load: impl FnOnce() -> T) -> T {
    let mut set: HashMap<String, String> = env::vars()
        .filter_map(|(key, value)| {
            Some((key.strip_prefix(prefix).filter(|name| !name.is_empty())?.to_string(), value))
        })
        .collect();
    if let (Some(mount), Ok(base)) = (mount, env::var("HEARTH_PUBLIC_BASE_URL")) {
        set.entry("PUBLIC_BASE_URL".to_string())
            .or_insert_with(|| format!("{}/{mount}", base.trim_end_matches('/')));
    }
    let saved: Vec<(String, Option<String>)> =
        set.keys().map(|name| (name.clone(), env::var(name).ok())).collect();
    for (name, value) in &set {
        env::set_var(name, value);
    }
    let loaded = load();
    for (name, previous) in saved {
        match previous {
            Some(value) => env::set_var(name, value),
            None => env::remove_var(name),
        }
    }
    loaded
}

#[cfg(test)]
mod tests {
    use super::*;

    // The environment is process-wide and tests run in parallel, so each test uses names no other
    // test (or service) touches.

    #[test]
    fn a_prefixed_name_wins_for_the_service_it_names_and_is_put_back() {
        env::set_var("SCOPED_A_X", "bare");
        env::set_var("ALPHA_SCOPED_A_X", "alpha");
        env::set_var("BETA_SCOPED_A_X", "beta");
        assert_eq!(scoped("ALPHA_", None, || env::var("SCOPED_A_X").unwrap()), "alpha");
        assert_eq!(scoped("BETA_", None, || env::var("SCOPED_A_X").unwrap()), "beta");
        assert_eq!(env::var("SCOPED_A_X").unwrap(), "bare");
    }

    #[test]
    fn a_bare_name_with_no_twin_reaches_every_service() {
        env::set_var("SCOPED_B_Y", "shared");
        assert_eq!(scoped("GAMMA_", None, || env::var("SCOPED_B_Y").unwrap()), "shared");
    }

    #[test]
    fn a_name_only_the_prefix_set_is_removed_afterwards() {
        env::set_var("DELTA_SCOPED_C_Z", "only");
        assert_eq!(scoped("DELTA_", None, || env::var("SCOPED_C_Z").unwrap()), "only");
        assert!(env::var("SCOPED_C_Z").is_err());
    }

    #[test]
    fn the_public_base_gets_the_mount_unless_the_service_names_its_own() {
        env::set_var("HEARTH_PUBLIC_BASE_URL", "https://node.example/");
        env::set_var("EPSILON_PUBLIC_BASE_URL", "https://own.example");
        assert_eq!(
            scoped("ZETA_", Some("reel"), || env::var("PUBLIC_BASE_URL").unwrap()),
            "https://node.example/reel"
        );
        assert_eq!(
            scoped("EPSILON_", Some("reel"), || env::var("PUBLIC_BASE_URL").unwrap()),
            "https://own.example"
        );
        env::remove_var("HEARTH_PUBLIC_BASE_URL");
        env::remove_var("EPSILON_PUBLIC_BASE_URL");
    }
}
