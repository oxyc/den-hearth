//! Port of `internal/scout/manifest.go`. Work package 4.

use std::sync::OnceLock;

use serde::Serialize;

use crate::config::Config;

/// Go `manifestJSON`: the Stremio manifest. Field order and values match the golden.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ManifestJson {
    pub id: String,
    pub version: String,
    pub name: String,
    pub description: String,
    pub resources: Vec<String>,
    pub types: Vec<String>,
    #[serde(rename = "idPrefixes")]
    pub id_prefixes: Vec<String>,
    pub catalogs: Vec<()>,
    #[serde(rename = "behaviorHints")]
    pub behavior_hints: ManifestHints,
    /// DenInstallID is the configured install's id, spelled as REVOKED_INSTALLS takes it, so the owner can
    /// revoke one link from what the app shows. Omitted when the config carries none.
    #[serde(rename = "denInstallId", skip_serializing_if = "String::is_empty")]
    pub den_install_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ManifestHints {
    pub configurable: bool,
    #[serde(rename = "configurationRequired")]
    pub configuration_required: bool,
}

/// devVersion is what a binary reports when nothing told it which release it is: `go run`, tests, a plain
/// `docker build`. Release builds never see it — the Dockerfile refuses an image without DEN_VERSION.
pub(crate) const DEV_VERSION: &str = "0.0.0-dev";

/// manifestVersion is the release this process reports in the manifest, /metrics and the startup line. It
/// comes from DEN_VERSION, which the image carries from the VERSION file, rather than from a constant in the
/// source: a constant changes the compiled package on every release, which recompiled the binary for a
/// release that changed nothing else.
pub(crate) fn manifest_version() -> &'static str {
    static VERSION: OnceLock<String> = OnceLock::new();
    VERSION.get_or_init(|| version_from(|k| std::env::var(k).unwrap_or_default()))
}

/// Go `versionFrom`: DEN_VERSION when set, else DEV_VERSION.
pub(crate) fn version_from(get: impl Fn(&str) -> String) -> String {
    let v = get("DEN_VERSION");
    if v.is_empty() {
        DEV_VERSION.to_owned()
    } else {
        v
    }
}

/// Go `buildManifest`: the manifest for a config (`None` = unconfigured).
pub(crate) fn build_manifest(config: Option<&Config>) -> ManifestJson {
    let mut description = "Self-hosted stream aggregator for Den — configure to add your debrid token.".to_owned();
    let mut install_id = String::new();
    if let Some(config) = config {
        install_id = config.iid.clone();
        let services: Vec<&str> = config.debrid.iter().map(|d| d.service.as_str()).collect();
        description = format!("Cached-first, ranked, junk-filtered streams via {}.", services.join(", "));
    }
    ManifestJson {
        id: "com.den.scout".to_owned(),
        version: manifest_version().to_owned(),
        name: "Den Scout".to_owned(),
        description,
        resources: vec!["stream".to_owned()],
        types: vec!["movie".to_owned(), "series".to_owned()],
        id_prefixes: vec!["tt".to_owned()],
        catalogs: Vec::new(),
        behavior_hints: ManifestHints { configurable: true, configuration_required: config.is_none() },
        den_install_id: install_id,
    }
}
