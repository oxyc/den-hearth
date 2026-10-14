//! Go: `internal/scout/redirect_test.go` (132 lines). Work package 4.
//!
//! Tests `RefuseRedirectReplay`, the redirect policy in `deps.go`. Go's `http.Client.CheckRedirect` has no
//! reqwest twin of the same shape (`reqwest::redirect::Policy::custom` is the nearest), so the port
//! decides how the policy is expressed.
