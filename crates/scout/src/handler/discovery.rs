//! Selection seam between the shared discovery/ranking pipeline and internal managed playback.

use super::*;

pub(super) fn managed_candidates<'a>(
    list: &'a RankedList,
    request: &ManagedHlsRequest,
) -> Result<Vec<&'a RawStream>, ManagedHlsError> {
    let mut candidates: Vec<&RawStream> = list.ranked.iter().collect();
    if let Some(filename) = request.filename.as_deref().filter(|name| !name.is_empty()) {
        candidates.retain(|candidate| candidate.title == filename);
    }
    // TorBox managed HLS has produced materially wrong Dolby Vision pictures on both LG webOS and iOS, while the
    // same releases through Remux play correctly. This is therefore a property of the managed rendition, not one
    // renderer: never silently trade correctness for the direct path. An automatic choice can take the next
    // non-DV release, and an explicitly selected DV release falls through to Remux unchanged. Atmos alone remains
    // eligible; it cannot account for damaged picture.
    candidates.retain(|candidate| !stream_attributes((*candidate).clone()).dolby_vision);
    if let Some(client) = request.playable.as_ref().filter(|_| request.receiver) {
        candidates.retain(|candidate| {
            client.cost((*candidate).clone(), stream_attributes((*candidate).clone()))
                < crate::client::VIDEO_CONVERTED_COST
        });
    }
    if candidates.is_empty() {
        return Err(if list.degraded.is_empty() {
            ManagedHlsError::NotFound
        } else {
            ManagedHlsError::Unavailable
        });
    }
    Ok(candidates)
}

impl Inner {
    // --- stream lists -----------------------------------------------------------------------------------

    pub(super) async fn handle_stream(
        self: &Arc<Self>,
        req: &Request<Bytes>,
        config_blob: &str,
        parts: &[&str],
    ) -> Resp {
        let Some(sid) = parse_stream_id(at(parts, 2), at(parts, 3)) else {
            return error(StatusCode::BAD_REQUEST, "bad_id");
        };
        let mut resp = self.handle_stream_for(req, config_blob, &sid).await;
        resp.headers_mut().append(VARY, HeaderValue::from_static(PLAYABLE_HEADER));
        resp
    }

    pub(super) async fn handle_stream_for(
        self: &Arc<Self>,
        req: &Request<Bytes>,
        config_blob: &str,
        sid: &StreamId,
    ) -> Resp {
        let start = Instant::now();
        let origin = self.public_origin(req);
        // audit #7 (collision-resistant key) + #8 (origin part) + #16 (key off the raw blob, decode later).
        let mut cache_key =
            format!("list:{}:{}:{}", key_hash(config_blob), key_hash(&origin), stream_cache_id(sid));
        // A warm hit serves the list without opening the config, so a list den-remux built for a scoped config
        // must never be what a request without its key hits. Its own key keeps the two apart.
        if self.remux_authorized(req) {
            cache_key.push_str(":remux");
        }
        // A list ranked for a browser's report is another list: cached apart, and the response says it varies.
        let (client, client_key) = client_playable(req);
        if !client_key.is_empty() {
            cache_key.push_str(":playable:");
            cache_key.push_str(&client_key);
        }
        let list_key = ListKey { config_blob, sid, origin: &origin, cache_key: &cache_key };

        // ?debug=1 — "where did this list go", answered exactly instead of guessed from a log line.
        //
        // Bypasses the CACHE deliberately: a cached entry carries no accounting, so answering from one would
        // return a body with nothing in it, and the result is not written back either — a hand-run diagnostic
        // should pay for itself rather than displace a good entry.
        //
        // It does NOT bypass singleflight; it uses a key of its own. Sharing the normal key would hand a
        // debug request whatever an in-flight plain build produced (no accounting), while having no key at
        // all meant every concurrent debug request ran its own scrape AND its own debrid cache-check fan-out.
        //
        // And it is PACED, because collapsing concurrent requests does nothing about sequential ones — a
        // single-connection `while true; do curl …?debug=1; done` was still an unbounded stream of scrapes
        // and cache-check batches. The host limiter in scrape.go paces the INDEXERS and there is no
        // equivalent anywhere on the debrid side, so a debug loop puts unpaced repeated checkcached batches
        // on a debrid account, and a throttled debrid reads back to a viewer as "this release does not exist".
        //
        // The limiter is consulted AFTER the config is validated, and under one shared key. Both matter: an
        // undecodable blob must not be able to mint limiter state or spend the allowance, and a per-cache-key
        // bucket gave an attacker a fresh full burst for every distinct blob or episode number — so the
        // ceiling this exists to impose measured exactly zero, while the never-evicted bucket map grew for
        // free. Sharing one bucket means debugging two titles at once contends, which is the right trade for
        // something a person runs by hand.
        //
        // Not a general answer to /stream traffic with a varying config blob, which misses the list cache in
        // the same way. That is pre-existing, is bounded by the indexer limiter, and is a separate question
        // from this branch.
        if query_is(req, "debug", "1") {
            let Some(config) = self.open_config(config_blob) else {
                return error(StatusCode::BAD_REQUEST, "bad_config");
            };
            if let Some(refused) = self.refuse_scoped(req, &config) {
                return refused;
            }
            let limiter = lock(&self.debug_limiter).clone();
            if !limiter.allow(DEBUG_LIMITER_KEY) {
                let mut resp = error(StatusCode::TOO_MANY_REQUESTS, "debug_rate_limited");
                resp.headers_mut().insert(RETRY_AFTER, HeaderValue::from_static("10"));
                return resp;
            }
            let key = format!("{cache_key}:debug");
            let job = ListJob::new(config, list_key, client, rid(req));
            let res = match self.shared(key, self.clone().build_task(job, true)).await {
                SharedBuild::Ready(res) => res,
                SharedBuild::Busy => return list_build_busy(),
                SharedBuild::Panicked => return error(StatusCode::INTERNAL_SERVER_ERROR, "internal"),
            };
            let (_, _, _, body) = split_cached(&res.value);
            let mut resp = Response::builder()
                .header(CONTENT_TYPE, JSON_TYPE)
                .header(CACHE_CONTROL, NO_STORE)
                .body(full(body.to_owned()))
                .expect("a static response");
            mark_degraded(&mut resp, &res.degraded);
            return with_timing(resp, &res.timing, start);
        }

        if let Some(hit) = self.deps.cache.get(&cache_key) {
            // The completeness rides WITH the entry. Shortening the header only on the build meant the very
            // next requester — Stremio races and cancels addon requests, so that happens routinely — was told
            // to hold a knowingly-short list for five minutes and a day on stale-if-error, which is the harm
            // the shortening exists to prevent, defeated one branch over.
            let (complete, fresh_until, etag, body) = split_cached(&hit);
            // Past the stale window a complete list is held only as a last resort: rebuild in the foreground, as on
            // a miss, and fall back to it only if no indexer answers.
            if complete
                && fresh_until > 0
                && now_unix() >= fresh_until + stale_window_for(self.deps.list_ttl).as_secs() as i64
            {
                // A useful complete list stays immediately usable throughout its ticket-safe hold. Refresh it
                // behind the response exactly like ordinary SWR; an empty answer still rebuilds in front because
                // stale emptiness is not useful and may hide a newly released title.
                if cached_list_nonempty(body) && SystemTime::now() < unix_time(fresh_until) + self.outage_hold
                {
                    metrics().list_cache_hit.fetch_add(1, Ordering::Relaxed);
                    metrics().list_cache_stale.fetch_add(1, Ordering::Relaxed);
                    self.rebuild_behind(req, list_key);
                    let (mut body, mut etag) = (Cow::Borrowed(body), Cow::Borrowed(etag));
                    if let Some(relabelled) = stale_body(&body, "stale_list") {
                        etag = Cow::Owned(etag_for(&relabelled));
                        body = Cow::Owned(relabelled);
                    }
                    let body = entry_body(&hit, body);
                    let mut resp = cacheable(body, &etag, JSON_TYPE, &self.stale_list_cache);
                    mark_degraded(&mut resp, "stale_list");
                    return with_timing(resp, "cache;desc=stale_list", start);
                }
                metrics().list_cache_miss.fetch_add(1, Ordering::Relaxed);
                return self.last_resort(req, start, list_key, Held { fresh_until, etag, body }).await;
            }
            let mut header = self.list_cache.as_str();
            let mut served = "cache;desc=hit";
            metrics().list_cache_hit.fetch_add(1, Ordering::Relaxed);
            let (mut body, mut etag) = (Cow::Borrowed(body), Cow::Borrowed(etag));
            if !complete {
                header = &self.partial_list_cache;
            } else if fresh_until > 0 && now_unix() >= fresh_until {
                metrics().list_cache_stale.fetch_add(1, Ordering::Relaxed);
                // Stale but complete: answer now from what we have and refresh behind the reply. Only
                // COMPLETE lists get this — serving a knowingly-short one past its own expiry is the harm
                // the branch above exists to prevent, and a longer life is the last thing it should get.
                header = &self.stale_list_cache;
                served = "cache;desc=stale";
                self.rebuild_behind(req, list_key);
                if let Some(relabelled) = stale_body(&body, "") {
                    etag = Cow::Owned(etag_for(&relabelled));
                    body = Cow::Owned(relabelled);
                }
            }
            let body = entry_body(&hit, body);
            return with_timing(cacheable(body, &etag, JSON_TYPE, header), served, start);
        }

        // Miss: decode now (#16 — a warm hit never pays decode/validate).
        metrics().list_cache_miss.fetch_add(1, Ordering::Relaxed);
        let Some(config) = self.open_config(config_blob) else {
            return error(StatusCode::BAD_REQUEST, "bad_config");
        };
        // Only the miss path needs this: a warm hit is keyed on the blob, and nothing builds a list for a
        // scoped blob to hit.
        if let Some(refused) = self.refuse_scoped(req, &config) {
            return refused;
        }

        // Detach the shared build from the request: a client disconnect (Stremio routinely races and cancels
        // addon requests) must not cancel the scrape mid-flight and let an empty list get cached and served
        // to every follower. The build runs on a task of its own, which a dropped request does not abort.
        let job = ListJob::new(config, list_key, client, rid(req));
        match self.shared(cache_key.clone(), self.clone().build_task(job, false)).await {
            SharedBuild::Ready(res) => self.write_built(start, &res),
            SharedBuild::Busy => list_build_busy(),
            SharedBuild::Panicked => error(StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        }
    }

    /// The future of one shared build: everything it needs is owned, so it can outlive the request that started it.
    pub(super) async fn build_task(self: Arc<Self>, job: ListJob, debug: bool) -> BuildResult {
        let deadline = Self::deadline_in(self.deps.scrape_timeout + LIST_BUILD_SLACK);
        self.build_stream_list(deadline, &job, debug).await
    }

    /// Go `sf.Do`: concurrent callers of one key share one run of the build and its result. The build runs on a task
    /// of its own, so a caller that goes away leaves it running for the others. A panicked build is answered as a
    /// 500, where Go's singleflight re-raises it into the request's recover; admission refusal is a retryable 503.
    pub(super) async fn shared<F>(self: &Arc<Self>, key: String, build: F) -> SharedBuild
    where
        F: Future<Output = BuildResult> + Send + 'static,
    {
        let mut rx = {
            let mut flights = lock(&self.flights);
            match flights.get(&key) {
                Some(rx) => rx.clone(),
                None => {
                    // Admission is intentionally inside the flight-map critical section and after the join
                    // check. Therefore two callers for one key cannot both spend slots, and a refused key is
                    // never visible in the map. Across handlers, all gates point at the same ScoutRuntime.
                    let Some(admission) = self.deps.runtime.try_admit_list_build() else {
                        return SharedBuild::Busy;
                    };
                    let (tx, rx) = watch::channel(None);
                    flights.insert(key.clone(), rx.clone());
                    let done = FlightDone { inner: self.clone(), key };
                    let runtime = self.deps.runtime.clone();
                    tokio::spawn(crate::runtime::scope(runtime, async move {
                        let _admission = admission;
                        let _done = done;
                        let res = build.await;
                        let _ = tx.send(Some(Arc::new(res)));
                    }));
                    rx
                }
            }
        };
        loop {
            if let Some(res) = rx.borrow().clone() {
                return SharedBuild::Ready(res);
            }
            if rx.changed().await.is_err() {
                return rx.borrow().clone().map_or(SharedBuild::Panicked, SharedBuild::Ready);
            }
        }
    }

    /// lastResort answers a request for a complete list that is past its stale window: a foreground build, as on
    /// a miss, unless no indexer answers it — then the held list, marked X-Den-Degraded: stale_list and kept by
    /// the client for a minute. An empty list in an outage reads as "nothing to play"; the last good one plays.
    ///
    /// Only within outageHold of the list's freshness, which keeps every ticket in it alive for a viewing. After
    /// such a build the key cools off for rebuildCooloff, during which the held list is served without scraping.
    pub(super) async fn last_resort(
        self: &Arc<Self>,
        req: &Request<Bytes>,
        start: Instant,
        key: ListKey<'_>,
        held: Held<'_>,
    ) -> Resp {
        let ListKey { config_blob, sid, cache_key, .. } = key;
        let Held { fresh_until, etag, body } = held;
        let Some(config) = self.open_config(config_blob) else {
            return error(StatusCode::BAD_REQUEST, "bad_config");
        };
        if let Some(refused) = self.refuse_scoped(req, &config) {
            return refused;
        }
        let now = Instant::now();
        let usable = SystemTime::now() < unix_time(fresh_until) + self.outage_hold;
        let serve_held = |phases: &str| -> Resp {
            let ago = SystemTime::now().duration_since(unix_time(fresh_until)).unwrap_or_default();
            log_limited(
                "stale-list",
                &format!(
                    "{} {}: no indexer answered; serving the last complete list, {} past its freshness",
                    sid.kind,
                    sid.imdb,
                    go_duration(Duration::from_secs(ago.as_secs()))
                ),
            );
            // The held body says what it was when built; served now, it is stale, and says so. A new body is a new
            // ETag, so a client revalidating the fresh list is not told its copy still stands.
            let (mut held, mut held_etag) = (Cow::Borrowed(body), Cow::Borrowed(etag));
            if let Some(relabelled) = stale_body(body, "stale_list") {
                held_etag = Cow::Owned(etag_for(&relabelled));
                held = Cow::Owned(relabelled);
            }
            let mut resp = cacheable(held.into_owned(), &held_etag, JSON_TYPE, &self.stale_list_cache);
            mark_degraded(&mut resp, "stale_list");
            with_timing(resp, phases, start)
        };
        if usable && lock(&self.rebuilds).in_outage(cache_key, now) {
            return serve_held("cache;desc=stale_list");
        }
        let (client, _) = client_playable(req);
        let job = ListJob::new(config, key, client, rid(req));
        let res = match self.shared(cache_key.to_owned(), self.clone().build_task(job, false)).await {
            SharedBuild::Ready(res) => res,
            SharedBuild::Busy => return list_build_busy(),
            SharedBuild::Panicked => return error(StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        };
        if res.degraded == "indexers" && usable {
            lock(&self.rebuilds).mark_outage(cache_key, now, now + REBUILD_COOLOFF);
            return serve_held(&format!("{}, cache;desc=stale_list", res.timing));
        }
        self.write_built(start, &res)
    }

    /// writeBuilt answers with a list built for this request (or for the build it joined).
    pub(super) fn write_built(&self, start: Instant, res: &BuildResult) -> Resp {
        // A follower of a shared build reports the build it was handed — which is the work its answer cost.
        let (_, _, etag, body) = split_cached(&res.value);
        // A degraded build is deliberately not cached server-side, "so the next request retries instead of
        // serving the blip for the whole TTL" — and then the same body went out with `max-age=300,
        // stale-if-error=86400`, which URLSession's shared cache honours. The guard was defeated one layer
        // down, on the only client that matters: an outage's empty list stuck on the device for five minutes,
        // and up to a day on any later error.
        let cache_header = if !res.degraded.is_empty() {
            NO_STORE
        } else if !res.complete {
            // The SAME lesson one line up, for the partial case: the server held this list for a minute and
            // then told the client to keep it for five, with stale-if-error for a day. A list knowingly
            // missing an indexer's releases must not outlive its short server-side life on the device.
            &self.partial_list_cache
        } else {
            &self.list_cache
        };
        let mut resp = cacheable(body.to_owned(), etag, JSON_TYPE, cache_header);
        // Signal a degraded build so the app can say "sources temporarily unavailable" rather than treating
        // an empty list as "no results" (a total indexer/cache-check outage otherwise looks identical).
        mark_degraded(&mut resp, &res.degraded);
        with_timing(resp, &res.timing, start)
    }

    /// rebuildBehind refreshes an expired list alongside the stale answer, which is written by the caller the
    /// moment this returns.
    ///
    /// Deduped through the same singleflight key the foreground build uses, so a burst of viewers arriving on
    /// an expired title produces one scrape — and a foreground build already in flight absorbs this one rather
    /// than racing it.
    ///
    /// The work runs detached from the request, on a task of its own: the request is over by the time it
    /// matters. The config is decoded on this path rather than on the hit path, because a warm hit must keep
    /// paying nothing for decode/validate — this runs only on the rare stale hit.
    pub(super) fn rebuild_behind(self: &Arc<Self>, req: &Request<Bytes>, key: ListKey<'_>) {
        let ListKey { config_blob, cache_key, .. } = key;
        let budget = self.deps.scrape_timeout + LIST_BUILD_SLACK;
        let now = Instant::now();
        // Booked BEFORE the task exists, and before the config is decoded, so a flood of stale hits costs
        // one map lookup each instead of a task each.
        let (gen, booked) = lock(&self.rebuilds).book_rebuild(cache_key, now, now + budget);
        if !booked {
            return;
        }
        // Until the task exists and owns the release, THIS function owns it — including if something
        // between here and the spawn panics. A slot lost here is lost for the life of the process, reported by
        // nothing; eight of those and no title ever refreshes in the background again.
        let mut release =
            RebuildRelease { inner: self.clone(), key: cache_key.to_owned(), gen, cooloff: None };
        let Some(config) = self.open_config(config_blob) else {
            // It decoded when the entry was built. If it no longer does, or the install has since been revoked,
            // serving stale is all we can do — and a revoked install's play URLs in it are refused anyway.
            return;
        };
        let (client, _) = client_playable(req);
        let this = self.clone();
        let job = ListJob::new(config, key, client, rid(req));
        let flight_key = cache_key.to_owned();
        spawn_background(self.deps.runtime.clone(), "stale list rebuild", async move {
            // The booking is released from the guard, so a panic cannot strand the key permanently.
            let build = this.clone().build_task(job, false);
            let res = this.shared(flight_key, build).await;
            // A degraded build caches nothing, so the entry is still stale and the next request would book
            // another rebuild at once. Hold the key until there is some prospect of a different answer.
            if matches!(res, SharedBuild::Ready(ref r) if !r.degraded.is_empty()) {
                release.cooloff = Some(Instant::now() + REBUILD_COOLOFF);
            }
            drop(release);
        });
        // The guard moved into the task; nothing is left to release here.
    }

    // --- building a list --------------------------------------------------------------------------------

    /// buildStreamList scrapes → cache-checks → ranks → serializes, caches the framed entry (see joinCached).
    ///
    /// `debug` is false on every normal request. When it is set, the accounting rides along in the response body and
    /// the result is NOT cached — a debug build is a diagnostic, not an entry other viewers should be served.
    pub(super) async fn build_stream_list(
        self: &Arc<Self>,
        deadline: TokioInstant,
        job: &ListJob,
        debug: bool,
    ) -> BuildResult {
        let mut dbg = debug.then(RankDebug::default);
        let opts = RankOpts { client: job.client.as_ref(), check_cache: true, rid: &job.rid };
        let list = self.rank_list(deadline, &job.config, &job.sid, dbg.as_mut(), opts).await;
        // A movie's list answers the availability route's question too, so a title someone opened needs no
        // check of its own there. Not from a degraded build, which knows nothing either way, nor a debug one, nor
        // one ranked for a browser, whose cap may have kept different releases.
        if job.sid.kind == "movie" && list.degraded.is_empty() && dbg.is_none() && job.client.is_none() {
            self.record_list_verdict(&format!("{}{}", verdict_prefix(&job.config), job.sid.imdb), &list);
        }
        self.finish_stream_list(job, dbg, list).await
    }

    /// rankList scrapes, cache-checks and ranks — for client, when a browser reported what it plays. Nothing here
    /// changes anything on the debrid: the cache check is a read. Both answers are cached on their own (each
    /// indexer's in scrapeAllCached, each held release in the stores), so ranking the same title another way asks
    /// nobody again.
    pub(crate) async fn rank_list(
        self: &Arc<Self>,
        deadline: TokioInstant,
        config: &Config,
        sid: &StreamId,
        dbg: Option<&mut RankDebug>,
        opts: RankOpts<'_>,
    ) -> RankedList {
        let RankOpts { client, check_cache, rid } = opts;
        let q = ScrapeQuery {
            kind: sid.kind.clone(),
            imdb: sid.imdb.clone(),
            season: sid.season,
            episode: sid.episode,
            has_ep: sid.has_ep,
            rid: rid.to_owned(),
        };
        let mut phase = Instant::now();
        let scrapers: Vec<Arc<dyn Scraper>> =
            (self.deps.make_scrapers)(config).into_iter().map(Arc::from).collect();
        let scraped = scrape_covered_until(
            &scrapers,
            &q,
            self.deps.scrape_timeout,
            deadline,
            Some(self.deps.cache.clone()),
            self.deps.list_ttl,
        )
        .await;
        let (mut seeds, scrape_ok, scrape_complete) = (scraped.seeds, scraped.any_ok, scraped.complete);
        let mut sources = scraped.sources;
        sources.extend(quarantined_reports(config));
        let cov = Coverage { complete: scrape_complete, sources };
        metrics().coverage(&cov.sources);
        let mut timing = den_http::timing("scrape", phase.elapsed());

        // The accounting object the rank decision line reads. When the request asked for ?debug=1 it is the SAME
        // object, so ?debug=1's accounting is unchanged; otherwise this is the only place the rank decision line
        // gets its drop reasons and picks from, and nothing is serialised to a client.
        let has_dbg = dbg.is_some();
        let mut own_dbg = RankDebug::default();
        let log_dbg: &mut RankDebug = match dbg {
            Some(d) => d,
            None => &mut own_dbg,
        };
        // Recorded HERE, before anything trims it. Taken after the two filters below instead, the count read
        // as "this is all the indexers had" while both of them had already removed releases that appear in no
        // drop tally either — on an RD-only install that is most of the list, which is exactly the confusion
        // this route exists to end.
        if has_dbg {
            log_dbg.deduped = seeds.len() as i64;
        }

        // Cap the seed set before the cache-check fan-out: a misbehaving/hostile indexer returning thousands
        // of tiny stream objects would otherwise mean hundreds of concurrent outbound debrid requests. The
        // cap is well above any real title's stream count, so it rarely bites — but when it does it now keeps
        // the most promising releases rather than the first ones the scrape happened to return.
        if seeds.len() > MAX_SEEDS {
            log_limited(
                "seed-cap",
                &format!(
                    "{} {}: {} seeds capped to {} (best-scored kept)",
                    sid.kind,
                    sid.imdb,
                    seeds.len(),
                    MAX_SEEDS
                ),
            );
            if has_dbg {
                *log_dbg.dropped_by.entry("seedCap".to_owned()).or_insert(0) +=
                    (seeds.len() - MAX_SEEDS) as i64;
            }
            seeds = cap_seeds(&seeds, MAX_SEEDS as i64);
        }

        let mut truth = CacheTruth { holders: HashMap::new(), known: HashMap::new(), complete: true };
        let mut truth_ok = true;
        if check_cache {
            let pool = StorePool { stores: (self.deps.make_stores)(config) };
            let hashes: Vec<String> = seeds.iter().map(|s| s.info_hash.clone()).collect();
            phase = Instant::now();
            (truth, truth_ok) = match timeout_at(deadline, pool.cache_check(&hashes)).await {
                Ok(answer) => answer,
                // The build's own clock ran out: every store would have failed, so nobody could be asked.
                Err(_) => {
                    (CacheTruth { holders: HashMap::new(), known: HashMap::new(), complete: false }, false)
                }
            };
            timing.push_str(", ");
            timing.push_str(&den_http::timing("cache-check", phase.elapsed()));
            for s in seeds.iter_mut() {
                s.cached = truth.cached(&s.info_hash);
                // Whether that false means "not held" or "nobody could ask" is decided here, once, and carried —
                // rather than being re-guessed by every consumer from a header they may not have. Per HASH, not
                // per request: the checks are batched, so one failed batch leaves 100 releases unknown while the
                // rest of the list is perfectly well known. Stamping the request-wide `truthOK` on all of them
                // asserted the answer for exactly the hashes nobody had an answer for.
                s.cache_known = truth.known(&s.info_hash);
            }
        }
        // A degraded upstream (every indexer failed, or every cache-truth store's check failed) yields a
        // misleading empty/partial list; return it for this request but don't cache it, so the next request
        // retries instead of serving the blip for the whole TTL.
        // Track consecutive total-scrape failures for /health (reset on any successful scrape). The log marks
        // the moment /health changes its answer, once each way, rather than every build that failed: the
        // crossing is exactly one Add landing on the threshold, and the recovery the one Swap that finds the
        // count at or past it.
        if scrape_ok {
            if self.scrape_fails.swap(0, Ordering::Relaxed) >= SCRAPE_FAIL_THRESHOLD {
                emit("health: degraded → ok, an indexer answered again");
            }
        } else if self.scrape_fails.fetch_add(1, Ordering::Relaxed) + 1 == SCRAPE_FAIL_THRESHOLD {
            emit(&format!("health: ok → degraded (indexers): {INDEXERS_DOWN_DETAIL}"));
        }
        if scrape_complete {
            if self.partial_builds.swap(0, Ordering::Relaxed) >= SCRAPE_FAIL_THRESHOLD {
                emit("health: coverage partial → complete, every source answered again");
            }
        } else if self.partial_builds.fetch_add(1, Ordering::Relaxed) + 1 == SCRAPE_FAIL_THRESHOLD {
            emit(&format!("health: coverage complete → partial: {SOURCES_MISSING_DETAIL}"));
        }

        // TOTAL failure and PARTIAL failure are different states and drive different decisions, so they get
        // different variables. Folding them into one turned off the cached-only filter for the whole request
        // on any partial failure — which handed a viewer who asked for "only what plays now" releases the
        // store had definitively said it does not hold. The coarse gate silently won over the per-hash filter
        // that was added in the same commit to make exactly this case work.
        //
        // A cache-truth store being entirely OUT is an outage too, even when the others answer normally:
        // their replies cannot rule anything out on its behalf. Treating that as an ordinary answer let a
        // cachedOnly request return an empty list, with no degraded header, cached for the full TTL.
        let truth_out = check_cache && has_cache_truth(config) && (!truth_ok || !truth.complete());
        // audit #4: with no cache-truth store (RD-only), the cached-only filter would drop everything. Also
        // skip it when the cache-truth stores are unreachable ENTIRELY (don't drop everything on a blip). A
        // partial failure keeps the filter on — the per-hash CacheKnown check inside it drops only what is
        // known not to be held, and keeps what nobody could ask about.
        let eff_cached_only = check_cache && config.cached_only && has_cache_truth(config) && !truth_out;
        // RD-only: drop releases RD blocks by filename (they'd 404 at resolve).
        if rd_only(config) {
            let before = seeds.len();
            seeds.retain(|s| !real_debrid_blocked(&s.title));
            // The biggest dropper on an RD-only install by far — realDebridBlocked matches web-dl, webrip,
            // bdrip, hdrip and dvdrip as substrings, which is most of what indexers return. Attributed, or
            // ?debug=1 reports a list that shrank for no stated reason.
            if has_dbg && before > seeds.len() {
                *log_dbg.dropped_by.entry("realDebridBlocked".to_owned()).or_insert(0) +=
                    (before - seeds.len()) as i64;
            }
        }

        // Expected title, plus a release year for movies → drop torrents mistagged with another title's id.
        // A series' title only sinks a release naming another show (cinemeta.go says why it never drops).
        // Best-effort: a lookup failure just means no year/title filter.
        let mut expected_year: Option<i64> = None;
        let mut expected_title_tokens = HashSet::new();
        let mut show_tokens = HashSet::new();
        if let Some(meta) = &self.deps.meta {
            if let Ok((m, true)) = timeout_at(deadline, meta.lookup(&sid.kind, &sid.imdb)).await {
                if sid.kind == "series" {
                    if !m.title.is_empty() {
                        show_tokens = title_tokens(&m.title);
                    }
                } else {
                    if m.year != 0 {
                        expected_year = Some(m.year);
                    }
                    if !m.title.is_empty() {
                        expected_title_tokens = title_tokens(&m.title);
                    }
                }
            }
        }

        // Probe facts are ranking inputs, not decorations. Hydrate every locally-known fact before the
        // client compatibility cost is computed; the old order ranked first and discovered the fact only
        // afterward, so a warm probe could not affect the list it was attached to.
        self.hydrate_probe_facts(&mut seeds, Some(sid));
        // A store's own file list, cached from an earlier resolve attempt, corrects a pack-sized episode for
        // free — and does not depend on the probe fan-out's own resolve succeeding (see hydrateKnownSizes).
        self.hydrate_known_sizes(config, &mut seeds, Some(sid));
        // Whatever neither pass above corrected keeps the indexer's number only where the title itself backs
        // it as this episode's; otherwise it becomes a labelled pack rather than a wrong plain size.
        hydrate_pack_only_sizes(&mut seeds, Some(sid));
        let episode = sid.has_ep.then_some([sid.season, sid.episode]);
        let candidates = seeds.len() as i64;
        let rank_start = Instant::now();
        let ranked = rank_streams_debug(
            &seeds,
            RankFilters {
                exclude_cam: config.filters.exclude_cam,
                resolutions: config.filters.resolutions.clone(),
                prefer_resolution: config.filters.prefer_resolution.clone(),
                hdr_only: config.filters.hdr_only,
                min_seeders: config.filters.min_seeders,
                max_size_gb: config.filters.max_size_gb,
                exclude_regex: config.filters.exclude_regex.clone(),
                cached_only: eff_cached_only,
                result_cap: config.result_cap,
                expected_year,
                expected_title_tokens,
                episode,
                show_tokens,
                client: client.cloned(),
            },
            Some(&mut *log_dbg),
        );
        log_rank_decision(sid, candidates, &ranked, log_dbg, rank_start.elapsed(), rid);

        // Degraded is judged on what is actually SERVED, not on what was scraped.
        //
        // A partial cache-check failure is only a problem if it touched a release the viewer will see. Counted
        // over all 500 seeds instead, a failed batch covering releases that every filter would have dropped
        // still marked the response degraded — which means not cached, which means the next /stream re-runs
        // the whole eight-second scrape. That is a user-visible cost paid for a fact about nothing.
        let unchecked = if check_cache && has_cache_truth(config) {
            ranked.iter().filter(|s| !s.cache_known).count()
        } else {
            0
        };
        let degraded_reason = if !scrape_ok {
            "indexers"
        } else if truth_out || unchecked > 0 {
            "cache-check"
        } else {
            ""
        };
        if unchecked > 0 {
            log_limited(
                "cache-check-partial",
                &format!(
                    "{} {}: {} of {} served releases could not be cache-checked",
                    sid.kind,
                    sid.imdb,
                    unchecked,
                    ranked.len()
                ),
            );
        }

        // Where a list is lost. An empty answer has half a dozen possible authors — no indexer had it, a
        // filter removed everything, the cache-only gate dropped an uncached set — and they are
        // indistinguishable in the reply. This is the line that separates them, and the one that ended the
        // guessing about an episode torrentio was serving 50 results for.
        //
        // EMPTY only. The condition used to also fire on `len(ranked) < len(seeds)`, which is true whenever
        // the result cap trims — twenty of them, by default, on any popular title — so a line written for
        // the case worth investigating printed on very nearly every request and buried itself. "Some were
        // dropped" is now a counter on /metrics and an exact answer on ?debug=1; this stays for the one
        // outcome that is genuinely worth a line in the log.
        if ranked.is_empty() {
            let year = expected_year.map_or_else(|| "<nil>".to_owned(), |y| y.to_string());
            log_limited(
                "empty-list",
                &format!(
                    "{} {}: scraped {} → ranked {} (cachedOnly={} year={} filters: res={:?} minSeed={} maxGB={} cam={} hdr={})",
                    sid.kind,
                    sid.imdb,
                    seeds.len(),
                    ranked.len(),
                    eff_cached_only,
                    year,
                    config.filters.resolutions,
                    config.filters.min_seeders.unwrap_or(0),
                    config.filters.max_size_gb.map_or_else(|| "<nil>".to_owned(), |n| n.to_string()),
                    config.filters.exclude_cam,
                    config.filters.hdr_only
                ),
            );
        }
        RankedList {
            ranked,
            truth,
            client: client.cloned(),
            degraded: degraded_reason.to_owned(),
            complete: scrape_complete,
            coverage: cov,
            timing,
        }
    }

    /// finishStreamList probes the top of a ranked list, serialises it and caches it — the half of a build only a
    /// stream list needs.
    pub(super) async fn finish_stream_list(
        self: &Arc<Self>,
        job: &ListJob,
        dbg: Option<RankDebug>,
        list: RankedList,
    ) -> BuildResult {
        let ListJob { config, config_blob, sid, origin, cache_key, .. } = job;
        let RankedList {
            ranked,
            truth,
            client,
            degraded: degraded_reason,
            complete: scrape_complete,
            coverage,
            mut timing,
        } = list;
        let degraded = !degraded_reason.is_empty();
        // Ask the top few releases what they actually contain. After ranking, so the probe follows the order
        // the viewer will see; before serialisation, so a cached probe rides along in this same response.
        let phase = Instant::now();
        if self.probe_top_for_client(config, &ranked, Some(sid), &truth, client.as_ref()) {
            timing.push_str(", ");
            timing.push_str(&den_http::timing("probe", phase.elapsed()));
        }

        let play_url = self.play_urls(config, config_blob, origin);
        let out: Vec<StreamOut> = ranked.iter().map(|s| to_stremio_stream(s, sid, &*play_url)).collect();
        // A list missing one indexer's releases is worth serving and worth caching — just not for as long
        // as a complete one. Refusing to cache it at all put the full scrape and a fresh debrid fan-out on
        // every single request whenever one upstream was flaky, which is a worse answer than the slightly
        // short list it was protecting against.
        let mut ttl = self.deps.list_ttl;
        if !scrape_complete {
            // Capped by the configured freshness for the same reason the header is: a knowingly-short list
            // must not outlive a complete one, which the flat 60s did whenever the TTL was set below it.
            ttl = PARTIAL_LIST_TTL.min(self.deps.list_ttl);
        }
        // The entry outlives its freshness so the hit path can answer from it while a rebuild runs, and fall back
        // to it through an indexer outage (lastResort) — but only when it is COMPLETE. A partial list expires
        // exactly when it says it does: it is already knowingly short, and the one thing it must not get is a
        // longer life.
        let now = SystemTime::now();
        let mut fresh_until = 0;
        let mut hold = ttl;
        if scrape_complete {
            fresh_until = (now + ttl).duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs() as i64);
            hold = ttl + stale_window_for(ttl).max(self.outage_hold);
        }
        let cached = !degraded && dbg.is_none();
        let whole_seconds = |t: SystemTime| {
            UNIX_EPOCH + Duration::from_secs(t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs()))
        };
        let env = AnswerEnvelope {
            v: ANSWER_ENVELOPE_VERSION,
            answer_kind: decide_answer_kind(out.len() as i64, scrape_complete, &degraded_reason).to_owned(),
            degraded: degraded_reason.clone(),
            generated_at: Some(whole_seconds(now)),
            expires_at: cached.then(|| whole_seconds(now + ttl)),
            coverage,
        };
        let body = to_json(&StreamsResponse { streams: out, debug: dbg, den: env });
        let etag = etag_for(&body);
        let value = join_cached(scrape_complete, fresh_until, &etag, &body);
        if degraded {
            metrics().build_degraded.fetch_add(1, Ordering::Relaxed);
        } else if !scrape_complete {
            metrics().build_partial.fetch_add(1, Ordering::Relaxed);
        } else {
            metrics().build_ok.fetch_add(1, Ordering::Relaxed);
        }
        // A debug build carries accounting in its body and must not become the entry every other viewer is
        // served — nor displace a good one.
        if cached {
            if !scrape_complete {
                log_limited(
                    "partial-list",
                    &format!(
                        "{} {}: an indexer did not answer; caching this list for {} only",
                        sid.kind,
                        sid.imdb,
                        go_duration(ttl)
                    ),
                );
            }
            self.deps.cache.put(cache_key, &value, hold);
        }
        BuildResult { value, degraded: degraded_reason, complete: scrape_complete, timing }
    }

    /// playURLs is how one stream list names its play URLs: a ticket per release while tickets are on, all
    /// expiring together, else the legacy config segment plus token.
    ///
    /// Only a list build calls this, and a scoped config is refused before any list is built, so the read-only
    /// config a browser holds never mints a ticket.
    pub(super) fn play_urls(
        self: &Arc<Self>,
        config: &Config,
        config_blob: &str,
        origin: &str,
    ) -> Box<dyn Fn(&PlayTarget) -> String + Send> {
        let origin = origin.to_owned();
        let Some(_) = &self.tickets else {
            let blob = config_blob.to_owned();
            return Box::new(move |t| format!("{origin}/{blob}/play/{}", encode_play_token(t)));
        };
        let exp = SystemTime::now() + self.deps.play_ticket_ttl;
        let (this, config) = (self.clone(), config.clone());
        Box::new(move |t| match &this.tickets {
            Some(tickets) => format!("{origin}/{TICKET_ROUTE}/{}", tickets.mint(&config, t, exp)),
            None => String::new(),
        })
    }
}
