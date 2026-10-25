//! Exact episode releases that this install has actually proved held or resolved ready.
//!
//! A live scrape may legitimately become shorter later. It must not erase a release whose exact episode ticket
//! was held by an account or produced a playable link, but preserving the whole old list would also preserve
//! arbitrary and dead candidates. This ledger keeps only that proof, scoped to the sealed install/account and
//! bounded by the shared cache.

use std::collections::HashSet;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::cache::{Cache, CacheClass};
use crate::config::Config;
use crate::handler::to_json;
use crate::id::StreamId;
use crate::rank::RawStream;
use crate::stores::ResolveTarget;
use crate::util::key_hash;

const LEDGER_TTL: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const MAX_RELEASES: usize = 8;
const MAX_TITLE_BYTES: usize = 2 << 10;

#[derive(Clone, Deserialize, Serialize)]
struct KnownRelease {
    hash: String,
    title: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    size: i64,
}

fn is_zero(value: &i64) -> bool {
    *value == 0
}

/// The ticket carries exactly these fields from the full config. Hash the canonical form: neither the cache key nor
/// the value contains a debrid token, while a different account/install/epoch cannot read this install's proof.
fn scope(config: &Config) -> String {
    key_hash(&to_json(&json!({
        "debrid": config.debrid.iter().map(|account| {
            json!({"service": account.service.as_str(), "token": account.token})
        }).collect::<Vec<_>>(),
        "iid": config.iid,
        "epoch": config.epoch,
    })))
}

fn key(config: &Config, imdb: &str, season: i64, episode: i64) -> String {
    format!("continuity:v1:{}:{imdb}:{season}:{episode}", scope(config))
}

fn read(cache: &dyn Cache, key: &str) -> Vec<KnownRelease> {
    cache.get(key).and_then(|raw| serde_json::from_str::<Vec<KnownRelease>>(&raw).ok()).unwrap_or_default()
}

/// Remember an exact episode only after its play route proved the account holds it or produced a usable redirect.
pub(crate) fn remember_held(cache: &dyn Cache, config: &Config, target: &ResolveTarget) {
    let (Some(season), Some(episode)) = (target.season, target.episode) else { return };
    if target.imdb.is_empty()
        || target.title.is_empty()
        || target.title.len() > MAX_TITLE_BYTES
        || target.info_hash.is_empty()
    {
        return;
    }
    let key = key(config, &target.imdb, season, episode);
    let mut releases = read(cache, &key);
    releases.retain(|release| release.hash != target.info_hash);
    releases.insert(
        0,
        KnownRelease {
            hash: target.info_hash.clone(),
            title: target.title.clone(),
            size: target.release_size.max(0),
        },
    );
    releases.truncate(MAX_RELEASES);
    if let Ok(value) = serde_json::to_string(&releases) {
        cache.put_class(&key, &value, LEDGER_TTL, CacheClass::Durable);
    }
}

/// Add proven exact-episode candidates missing from this scrape. Ranking and every configured filter still apply.
pub(crate) fn restore(
    cache: &dyn Cache,
    config: &Config,
    sid: &StreamId,
    seeds: &mut Vec<RawStream>,
) -> usize {
    if !sid.has_ep {
        return 0;
    }
    let known = read(cache, &key(config, &sid.imdb, sid.season, sid.episode));
    let mut hashes: HashSet<String> = seeds.iter().map(|source| source.info_hash.clone()).collect();
    let mut restored = 0;
    for release in known {
        if !hashes.insert(release.hash.clone()) {
            continue;
        }
        seeds.push(RawStream {
            info_hash: release.hash,
            title: release.title,
            size_bytes: (release.size > 0).then_some(release.size),
            source: "continuity".to_owned(),
            ..RawStream::default()
        });
        restored += 1;
    }
    restored
}

/// A definitive dead/not-held answer retires this proof. A transient refusal never reaches this function.
pub(crate) fn forget(cache: &dyn Cache, config: &Config, target: &ResolveTarget) {
    let (Some(season), Some(episode)) = (target.season, target.episode) else { return };
    if target.imdb.is_empty() || target.info_hash.is_empty() {
        return;
    }
    let key = key(config, &target.imdb, season, episode);
    let mut releases = read(cache, &key);
    let before = releases.len();
    releases.retain(|release| release.hash != target.info_hash);
    if releases.len() == before {
        return;
    }
    if releases.is_empty() {
        // A volatile tombstone removes an older durable generation without writing an SSD file to say "gone".
        cache.put_class(&key, "", Duration::from_nanos(1), CacheClass::Volatile);
    } else if let Ok(value) = serde_json::to_string(&releases) {
        cache.put_class(&key, &value, LEDGER_TTL, CacheClass::Durable);
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::config::{DebridAccount, DebridService};
    use crate::diskcache::TieredCache;
    use crate::rank::{rank_streams, RankFilters};

    const PACK_HASH: &str = "7a5053aa07497a5053aa07497a5053aa07497a50";
    const OTHER_HASH: &str = "ffffffffffffffffffffffffffffffffffffffff";

    fn config(token: &str) -> Config {
        Config {
            debrid: vec![DebridAccount { service: DebridService::TorBox, token: token.to_owned() }],
            iid: "living-room".to_owned(),
            epoch: 7,
            ..Config::default()
        }
    }

    fn episode() -> StreamId {
        StreamId {
            kind: "series".to_owned(),
            imdb: "tt2309405".to_owned(),
            season: 1,
            episode: 10,
            has_ep: true,
        }
    }

    fn ready_target() -> ResolveTarget {
        ResolveTarget {
            info_hash: PACK_HASH.to_owned(),
            file_idx: Some(10),
            season: Some(1),
            episode: Some(10),
            release_size: 1_572_864_000,
            title: "Jordskott.S01E10.1080p.BluRay.x264-SKGTV.mkv".to_owned(),
            imdb: "tt2309405".to_owned(),
            ..ResolveTarget::default()
        }
    }

    #[test]
    fn held_pack_survives_restart_and_sparse_complete_scrape() {
        let dir = TempDir::new().unwrap();
        let cfg = config("torbox-secret");

        // A rich E10 scrape supplied a season pack and its prefetch ticket subsequently redirected ready.
        let rich = [RawStream {
            info_hash: PACK_HASH.to_owned(),
            title: ready_target().title,
            source: "torrentio".to_owned(),
            ..RawStream::default()
        }];
        assert_eq!(rich.len(), 1);
        {
            let cache = TieredCache::new_with_disk_bytes(1 << 20, 1 << 20, dir.path());
            let mut target = ready_target();
            target.info_hash.clone_from(&rich[0].info_hash);
            target.title.clone_from(&rich[0].title);
            remember_held(&cache, &cfg, &target);
        }

        // After a process restart, a later successful but sparse scrape omits that pack entirely.
        let cache = TieredCache::new_with_disk_bytes(1 << 20, 1 << 20, dir.path());
        let mut sparse_complete = vec![RawStream {
            info_hash: OTHER_HASH.to_owned(),
            title: "Jordskott.S01E10.720p.WEB-DL.x264-OTHER.mkv".to_owned(),
            source: "torrentio".to_owned(),
            ..RawStream::default()
        }];
        assert_eq!(restore(&cache, &cfg, &episode(), &mut sparse_complete), 1);
        let restored = sparse_complete.iter().find(|stream| stream.info_hash == PACK_HASH).unwrap();
        assert_eq!(restored.title, "Jordskott.S01E10.1080p.BluRay.x264-SKGTV.mkv");
        assert_eq!(restored.source, "continuity");
        assert_eq!(restored.size_bytes, Some(1_572_864_000));
        let ranked = rank_streams(
            &sparse_complete,
            RankFilters { result_cap: 20, episode: Some([1, 10]), ..RankFilters::default() },
        );
        assert!(
            ranked.iter().any(|stream| stream.info_hash == PACK_HASH),
            "the restored pack did not survive ordinary episode ranking"
        );
    }

    #[test]
    fn proof_is_account_scoped_and_definitive_absence_evicts_it() {
        let dir = TempDir::new().unwrap();
        let owner = config("owner-secret");
        let other = config("other-secret");
        let target = ready_target();
        {
            let cache = TieredCache::new(1 << 20, dir.path());
            remember_held(&cache, &owner, &target);

            let owner_key = key(&owner, &target.imdb, 1, 10);
            assert!(!owner_key.contains("owner-secret"));
            assert!(!owner_key.contains("living-room"));
            let persisted = std::fs::read_to_string(
                std::fs::read_dir(dir.path()).unwrap().next().unwrap().unwrap().path(),
            )
            .unwrap();
            assert!(!persisted.contains("owner-secret"));
            assert!(!persisted.contains("http://") && !persisted.contains("https://"));

            let mut sources = Vec::new();
            assert_eq!(restore(&cache, &other, &episode(), &mut sources), 0);
            assert!(sources.is_empty());
            let mut prior_episode = episode();
            prior_episode.episode = 9;
            assert_eq!(restore(&cache, &owner, &prior_episode, &mut sources), 0);

            assert_eq!(restore(&cache, &owner, &episode(), &mut sources), 1);
            forget(&cache, &owner, &target);
        }

        // The tombstone retires the durable generation too, not only the live process's memory copy.
        let cache = TieredCache::new(1 << 20, dir.path());
        let mut sources = Vec::new();
        assert_eq!(restore(&cache, &owner, &episode(), &mut sources), 0);
        assert!(sources.is_empty());
    }

    #[test]
    fn ledger_is_bounded_and_deduplicates_live_scrape_hashes() {
        let cache = TieredCache::new(1 << 20, std::path::Path::new(""));
        let cfg = config("torbox-secret");
        for n in 0..(MAX_RELEASES + 3) {
            let mut target = ready_target();
            target.info_hash = format!("{n:040x}");
            remember_held(&cache, &cfg, &target);
        }
        let mut sources =
            vec![RawStream { info_hash: format!("{:040x}", MAX_RELEASES + 2), ..RawStream::default() }];
        assert_eq!(restore(&cache, &cfg, &episode(), &mut sources), MAX_RELEASES - 1);
        assert_eq!(sources.len(), MAX_RELEASES);
    }
}
