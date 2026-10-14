//! Go: `internal/scout/version_test.go` (20 lines).

use crate::manifest::DEV_VERSION;

#[test]
fn version_from() {
    let env = |v: &'static str| move |k: &str| if k == "DEN_VERSION" { v.to_owned() } else { String::new() };
    assert_eq!(crate::manifest::version_from(env("1.2.3")), "1.2.3", "DEN_VERSION set");
    assert_eq!(crate::manifest::version_from(env("")), DEV_VERSION, "DEN_VERSION unset");
}
