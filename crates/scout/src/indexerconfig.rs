//! Port of `internal/scout/indexerconfig.go`. Work package 5.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::config::{Config, DebridAccount, Indexer};
use crate::rank::RawStream;
use crate::scrape::{DoError, Doer, Scraper, ScrapeQuery};

/// Go `mintedTTL`: how long a minted indexer config is reused.
pub(crate) const MINTED_TTL: Duration = Duration::from_secs(12 * 60 * 60);
/// Go `maxMintedEntries`: the ceiling on the minted-config cache, by count.
pub(crate) const MAX_MINTED_ENTRIES: usize = 1024;

/// Go `mintedConfig`: one minted indexer URL, when it was minted and when it was last used.
#[derive(Debug, Clone)]
pub(crate) struct MintedConfig {
    pub url: String,
    pub at: Instant,
    pub used: Instant,
}

/// Go `minted` with `mintedMu`: the minted-config cache. `None` until first use.
pub(crate) static MINTED: Mutex<Option<HashMap<String, MintedConfig>>> = Mutex::new(None);

/// Go `pruneMintedLocked`: drops expired entries, then evicts the least recently used past the ceiling.
pub(crate) fn prune_minted_locked(_minted: &mut HashMap<String, MintedConfig>) {
    todo!()
}

/// Go `mintCometURL`: the Comet config built locally from the account, carrying its debrid token.
pub(crate) fn mint_comet_url(_account: &DebridAccount) -> String {
    todo!()
}

/// Go `mintMediaFusionURL`: asks MediaFusion to encrypt the account; "" on a refusal or an unreachable host.
pub(crate) async fn mint_media_fusion_url(_account: &DebridAccount, _client: Option<&dyn Doer>) -> String {
    todo!()
}

/// Go `primaryDebrid`: the account a config's searches are made with, None when it has none.
pub(crate) fn primary_debrid(_cfg: Option<&Config>) -> Option<DebridAccount> {
    todo!()
}

/// Go `indexerBaseWithConfig`: the base URL an indexer is asked at for this config. The bool is true when the
/// URL is "" because minting failed transiently (an outage), false when it can never be minted (no account).
pub(crate) async fn indexer_base_with_config(
    _indexer: &Indexer,
    _cfg: &Config,
    _client: Option<&dyn Doer>,
) -> (String, bool) {
    todo!()
}

/// Go `unaskableScraper`: stands in for an indexer that could not be asked at all. `transient` says whether
/// that is an outage (counts against the quorum) or a permanent lack of configuration (excused from it).
pub(crate) struct UnaskableScraper {
    pub(crate) indexer: Indexer,
    pub(crate) transient: bool,
}

#[async_trait]
impl Scraper for UnaskableScraper {
    fn id(&self) -> Indexer {
        self.indexer.clone()
    }
    async fn scrape(&self, _q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError> {
        todo!()
    }
}
