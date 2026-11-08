//! TorBox's fixed API origin, rewritten to a local fake: for the outage tests and the cross-repo e2e build
//! (`stub-upstreams`, `HEARTH_STUB_TORBOX`). Never in a build that ships.

use std::sync::Arc;

use den_scout::scrape::{DoError, Doer};
use den_scout::stores::torbox::TORBOX_API;

/// Scout's production client, with TorBox's fixed origin rewritten to `base`: below the pause layer, as the real
/// network is, so pause keys and budgets stay TorBox's own.
pub(crate) struct ToFakeTorbox {
    pub(crate) inner: Arc<dyn Doer>,
    pub(crate) base: String,
}

/// `Doer` is an `async_trait`; this is the method it expands to, written out so Hearth needs no macro crate of its own.
impl Doer for ToFakeTorbox {
    fn do_request<'life0, 'async_trait>(
        &'life0 self,
        mut req: reqwest::Request,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<reqwest::Response, DoError>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let origin = TORBOX_API.trim_end_matches("/v1/api");
            if let Some(rest) = req.url().as_str().strip_prefix(origin) {
                *req.url_mut() = reqwest::Url::parse(&format!("{}{rest}", self.base)).unwrap();
            }
            self.inner.do_request(req).await
        })
    }
}
