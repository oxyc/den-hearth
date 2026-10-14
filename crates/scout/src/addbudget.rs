//! Port of `internal/scout/addbudget.go`. Work package 3.
//!
//! A local ceiling on debrid adds, per account: a rolling-window counter that every store consults
//! immediately before the request that queues a torrent. Signatures only until the logic lands.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use crate::config::DebridService;
use crate::stores::StoreError;

/// Go `addBudget`: a rolling-window counter, per account.
pub(crate) struct AddBudget {
    /// Go `spent`: the add times still inside the window, per account.
    pub(crate) spent: Mutex<HashMap<String, Vec<SystemTime>>>,
    pub(crate) window: Duration,
    pub(crate) limit: i64,
    /// Go `now`, injectable so the tests do not sleep for an hour.
    pub(crate) now: Box<dyn Fn() -> SystemTime + Send + Sync>,
}

impl AddBudget {
    /// Go `newAddBudget`.
    pub(crate) fn new(window: Duration, limit: i64) -> AddBudget {
        todo!("addbudget.go newAddBudget")
    }

    /// Go `addBudget.take`: records an add against the account and reports whether it is allowed.
    pub(crate) fn take(&self, account: &str, prefetch: bool) -> bool {
        todo!("addbudget.go addBudget.take")
    }

    /// Go `addBudget.remaining`: how many adds the account may still make this window.
    pub(crate) fn remaining(&self, account: &str) -> i64 {
        todo!("addbudget.go addBudget.remaining")
    }
}

/// Go's nil-receiver `take`: a nil budget admits everything.
pub(crate) fn budget_take(budget: Option<&AddBudget>, account: &str, prefetch: bool) -> bool {
    todo!("addbudget.go addBudget.take on a nil receiver")
}

/// Go's nil-receiver `remaining`: -1 for a nil budget, which has no count to report.
pub(crate) fn budget_remaining(budget: Option<&AddBudget>, account: &str) -> i64 {
    todo!("addbudget.go addBudget.remaining on a nil receiver")
}

/// Go `globalAddBudget`: one per process, keyed by service and account.
pub(crate) fn global_add_budget() -> Arc<AddBudget> {
    todo!("addbudget.go globalAddBudget")
}

/// Replaces the process-wide budget and returns the one it replaced. Go assigns `globalAddBudget` directly;
/// the Rust form returns the previous value so a test can put it back.
pub(crate) fn set_global_add_budget(budget: Arc<AddBudget>) -> Arc<AddBudget> {
    todo!("addbudget.go globalAddBudget (swap)")
}

/// Go `spendAdd`: charges one add against a service's account, or refuses.
pub(crate) fn spend_add(
    svc: DebridService,
    token: &str,
    info_hash: &str,
    prefetch: bool,
) -> Result<(), StoreError> {
    todo!("addbudget.go spendAdd")
}

/// Go `refundAdd`: gives the charge back only when the request was never sent at all.
pub(crate) fn refund_add(svc: DebridService, token: &str, err: &(dyn std::error::Error + 'static)) {
    todo!("addbudget.go refundAdd")
}

/// Go `errRequestNotSent`: the one error that means nothing reached the service.
#[derive(Debug)]
pub(crate) struct RequestNotSent;

impl std::fmt::Display for RequestNotSent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("request was not sent")
    }
}

impl std::error::Error for RequestNotSent {}

/// Go `budgetAccount`: the key one account's allowance is kept under.
pub(crate) fn budget_account(svc: DebridService, token: &str) -> String {
    todo!("addbudget.go budgetAccount")
}
