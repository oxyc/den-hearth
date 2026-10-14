//! Go: `internal/scout/mock_test.go` (18 lines). Work package 6, and the first thing it lands.
//!
//! Not a test: the shared helpers (`mockDoer`, `resp`) every other test module uses. A `Doer` that
//! routes requests to a closure, and a `reqwest::Response` built from a status and a body. Every other
//! work package waits on this one file, so it goes in before anything else.
