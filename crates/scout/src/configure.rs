//! Port of `internal/scout/configure.go` (which embeds `configure.html`). Work package 4.

/// The /configure page — embedded so the binary is self-contained (distroless, no runtime file).
/// Builds the install link in the browser and seals the debrid token to the addon's key
/// (den-scout/docs/SEALED-CONFIG.md); shares the layout + DenSeal bundle with den-subtitles/den-reel.
pub(crate) const CONFIGURE_PAGE: &str = include_str!("configure.html");
