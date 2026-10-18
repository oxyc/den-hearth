//! Play/cancel route gates and terminal responses.

use super::*;

pub(super) fn begin_play(req: &Request<Bytes>, start: Instant) -> Option<Resp> {
    if req.method() == Method::GET {
        return None;
    }
    Some(with_timing(method_not_allowed("GET"), "", start))
}

impl Inner {
    // --- probing ----------------------------------------------------------------------------------------

    /// Go `hydrateProbeFacts`: attaches the track details already known for these releases. Only the CACHE is
    /// consulted on the request path — a map lookup, no network.
    pub(crate) fn hydrate_probe_facts(&self, streams: &mut [RawStream], sid: Option<&StreamId>) {
        hydrate_probe_facts(Some(&*self.deps.cache), streams, sid);
        let hits = streams.iter().filter(|s| s.probe.is_some()).count();
        if hits > 0 {
            metrics().probe_cache_hit.fetch_add(hits as i64, Ordering::Relaxed);
        }
    }

    /// hydrateKnownSizes corrects a stream's size from what a store already knows about the file it
    /// resolves to — read off a torrent's own file list, which a store caches while picking that file for
    /// /play or a cache check, never a fresh network call of its own. It runs on every request, not just the
    /// probe's topN frontier, and does not wait on the probe fan-out's own resolve to succeed: the Fauda S1E3
    /// install's TorBox mylist kept answering "no usable answer" to that resolve, so the ranged-GET probe
    /// never warmed, while an EARLIER attempt had already gone far enough to list the pack's files and cached
    /// them. This reads that cache. The probe is still the only source of track/language facts, so it keeps
    /// running regardless of what this corrects.
    ///
    /// Movie-only guard, same reasoning as applyProbedSize: there is no pack for a movie's title size to have
    /// been confused with, so a movie is never given a lookup at all.
    pub(crate) fn hydrate_known_sizes(
        &self,
        config: &Config,
        streams: &mut [RawStream],
        sid: Option<&StreamId>,
    ) {
        let Some(sid) = sid.filter(|sid| sid.has_ep) else { return };
        let mut pool: Option<StorePool> = None;
        for s in streams.iter_mut() {
            if s.info_hash.is_empty()
                || s.pack_size_bytes.is_some()
                || s.probe.as_ref().is_some_and(|p| p.size_bytes > 0)
            {
                // A ranged probe measured what the playback URL actually serves, so it is more authoritative
                // than an older store file listing even when their difference was too small to name a pack.
                continue;
            }
            let pool = pool.get_or_insert_with(|| StorePool { stores: (self.deps.make_stores)(config) });
            let target = ResolveTarget {
                info_hash: s.info_hash.clone(),
                file_idx: s.file_idx,
                season: Some(sid.season),
                episode: Some(sid.episode),
                ..Default::default()
            };
            if let Some(known) = known_file_size(pool, &target) {
                crate::probe_fanout::apply_known_size(s, known);
            }
        }
    }

    /// probeTopForClient: the half of probing that decides what to probe, and starts it behind the reply. It reports
    /// whether probing is on at all, so Server-Timing names the phase only when there was one.
    pub(crate) fn probe_top_for_client(
        self: &Arc<Self>,
        config: &Config,
        streams: &[RawStream],
        sid: Option<&StreamId>,
        truth: &CacheTruth,
        client: Option<&ClientPlayable>,
    ) -> bool {
        // Opt-in: no client, no probing. Probing costs a debrid RESOLVE per release, so it must never happen
        // by accident — a caller that hasn't asked for it (a test, an embedder) gets the old behaviour
        // exactly, and the stream list is built without touching the debrid account at all.
        if self.probe_doer().is_none() {
            return false;
        }
        let frontier: HashSet<usize> = match client {
            Some(_) => browser_probe_candidates(streams, config, client),
            None => (0..streams.len().min(PROBE_TOP_N)).collect(),
        };
        for (i, s) in streams.iter().enumerate() {
            if !frontier.contains(&i) {
                if s.cached && s.probe.is_none() {
                    metrics().torbox_avoid("probe_frontier");
                }
                continue;
            }
            if s.probe.is_none() && s.cached && !s.info_hash.is_empty() {
                metrics().probe_cache_miss.fetch_add(1, Ordering::Relaxed);
            }
        }
        let pending = select_probe_jobs(streams, config, sid, truth, client);
        if !pending.is_empty() {
            self.probe_behind(config, pending);
        }
        true
    }

    /// probeBehind warms the probe cache after the reply has gone out.
    ///
    /// Its own task, not the request's: the request is over, so inheriting it would cancel every probe
    /// immediately. Deduped through the handler's singleflight so a title several clients ask for at once is
    /// probed once, not once per request.
    pub(crate) fn probe_behind(self: &Arc<Self>, config: &Config, jobs: Vec<ProbeJob>) {
        let this = self.clone();
        let config = config.clone();
        spawn_background(self.deps.runtime.clone(), "probe fan-out", async move {
            let deadline = TokioInstant::now() + PROBE_BUDGET;
            let pool = Arc::new(StorePool { stores: (this.deps.make_stores)(&config) });
            let config = Arc::new(config);
            // Gentle on the debrid account: a handful of resolves, not a burst.
            let slots = Arc::new(Semaphore::new(3));
            let mut running = tokio::task::JoinSet::new();
            for job in jobs {
                let Ok(slot) = slots.clone().acquire_owned().await else { break };
                let (this, pool, config) = (this.clone(), pool.clone(), config.clone());
                let runtime = this.deps.runtime.clone();
                running.spawn(crate::runtime::scope(runtime.clone(), async move {
                    let _slot = slot;
                    // Per job, and not only on the task above: each job runs on its OWN task, so a recover on the
                    // parent catches nothing that happens here.
                    let what = format!("probe {}", crate::stores::short_hash(&job.target.info_hash));
                    let run = tokio::spawn(crate::runtime::scope(runtime, {
                        let (this, pool, config) = (this.clone(), pool.clone(), config.clone());
                        async move { this.run_probe_job(deadline, &pool, &config, job).await }
                    }));
                    if let Err(e) = run.await {
                        if e.is_panic() {
                            recover_background(&what, &*e.into_panic());
                        }
                    }
                }));
            }
            while running.join_next().await.is_some() {}
        });
    }

    pub(super) async fn run_probe_job(
        &self,
        deadline: TokioInstant,
        pool: &StorePool,
        config: &Config,
        job: ProbeJob,
    ) {
        self.probe_flight
            .run(&format!("probe:{}", job.key), async {
                let resolved =
                    match timeout_at(deadline, pool.resolve_cached_only(&job.target, &job.holders)).await {
                        Ok(r) => r,
                        Err(_) => Err(StoreError::Cancelled),
                    };
                let link = match resolved {
                    Ok(link) if !link.is_empty() => link,
                    other => {
                        // Silent until now: a resolve failure here left no trace anywhere, so a release
                        // whose correction never arrived was indistinguishable from one nobody had viewed
                        // yet (den-scout#23) — this is exactly what Fauda S1E3 hit, repeatedly, with
                        // nothing in the log to say so. Key is a fixed literal, never the hash: logLimited
                        // keys a condition, not an instance.
                        let why = match other {
                            Err(e) => e.to_string(),
                            Ok(_) => "<nil>".to_owned(),
                        };
                        log_limited(
                            "probe-resolve-failed",
                            &format!(
                                "probe could not resolve {} to read its size/tracks: {why}",
                                crate::stores::short_hash(&job.target.info_hash)
                            ),
                        );
                        return;
                    }
                };
                let Some(client) = self.probe_doer() else { return };
                let probed = match timeout_at(deadline, crate::probe::probe_tracks(&*client, &link)).await {
                    Ok(r) => r,
                    Err(_) => Err("context deadline exceeded".to_owned()),
                };
                // The link was minted for this probe and is exactly what /play would mint again a moment
                // later, so it is kept rather than thrown away. Only when the read got an answer: a
                // container this cannot parse is still a file being served, but a refused or unreachable
                // link is a failure, and a failure is never remembered.
                if probed.is_ok() || probed.as_ref().is_err_and(|e| e == crate::probe::ERR_NO_TRACKS) {
                    self.links.put(&link_memo_key(config, &job.target), &link, false, SystemTime::now());
                }
                if let Ok(p) = probed {
                    self.deps.cache.put(&job.key, &to_json(&p), PROBE_TTL);
                }
            })
            .await;
    }

    // --- /play ------------------------------------------------------------------------------------------

    pub(super) async fn handle_play(
        self: &Arc<Self>,
        req: &Request<Bytes>,
        config_blob: &str,
        parts: &[&str],
    ) -> Resp {
        // GET only — HEAD is refused rather than answered.
        //
        // A HEAD here would have to either resolve (the whole point of not allowing it: an uncached release
        // is ADDED by that resolve) or be answered from the read-only path `?probe=1` uses. The second was
        // the tempting option and it is worse than useless: what a caller wants from this route is the
        // `location` of a freshly-minted link, and a HEAD that starts nothing cannot produce one — so it
        // would spend up to three upstream reads per prefetch to return a header no client can play from.
        //
        // Nothing legitimate issues it: Stremio asks for the list, and Den's AVPlayer follows the 302 with a
        // GET. What does issue HEAD is link prefetchers and unfurlers, which is precisely the traffic that
        // must not reach a debrid account.
        let start = Instant::now();
        if let Some(refused) = begin_play(req, start) {
            return refused;
        }
        let mut phases = String::new();
        let resp = self.handle_legacy_play(req, config_blob, parts, &mut phases).await;
        with_timing(resp, &phases, start)
    }

    pub(super) async fn handle_legacy_play(
        self: &Arc<Self>,
        req: &Request<Bytes>,
        config_blob: &str,
        parts: &[&str],
        phases: &mut String,
    ) -> Resp {
        // While tickets are on, this route only serves stream lists cached from before them, and a client can
        // hold one for a day on stale-if-error. LEGACY_PLAY_UNTIL is when that allowance ends.
        if self.tickets.is_some()
            && self.deps.legacy_play_until.is_some_and(|until| SystemTime::now() >= until)
        {
            return error(StatusCode::FORBIDDEN, "legacy_play_closed");
        }
        let Some(target) = decode_play_token(at(parts, 2)) else {
            return error(StatusCode::BAD_REQUEST, "bad_token");
        };
        let Some(config) = self.open_config(config_blob) else {
            return error(StatusCode::BAD_REQUEST, "bad_config");
        };
        if let Some(refused) = self.refuse_scoped(req, &config) {
            return refused;
        }
        if self.tickets.is_some() && !self.legacy_play_noted.swap(true, Ordering::Relaxed) {
            emit(
                "serving a legacy /<config>/play URL while play tickets are on — a client still holds a \
                 stream list from before them (logged once per process)",
            );
        }
        self.resolve_play(req, &config, &target, phases).await
    }

    /// handleTicketPlay is GET /p/<ticket>. The ticket stands in for both the config segment and the token of
    /// the legacy route, and what it opens to goes through the same resolve.
    pub(super) async fn handle_ticket_play(self: &Arc<Self>, req: &Request<Bytes>, ticket: &str) -> Resp {
        let start = Instant::now();
        if let Some(refused) = begin_play(req, start) {
            return refused;
        }
        let mut phases = String::new();
        let resp = match self.tickets.as_ref().map(|t| t.open(ticket, SystemTime::now())) {
            // 410, not 400: the ticket was good and has lapsed. What the client holds is an old stream list, not
            // a wrong one, and fetching the list again gets it fresh tickets.
            Some(Err(TicketError::Expired)) => error(StatusCode::GONE, "ticket_expired"),
            // A revoked install gets the answer a ticket that does not open gets.
            Some(Ok((config, target))) if self.admit_install(&config) => {
                self.resolve_play(req, &config, &target, &mut phases).await
            }
            _ => error(StatusCode::BAD_REQUEST, "bad_ticket"),
        };
        with_timing(resp, &phases, start)
    }

    /// resolvePlay is the play route's work once a request is admitted, whichever route admitted it. What it spent
    /// its time on goes in `phases`, for Server-Timing.
    pub(super) async fn resolve_play(
        self: &Arc<Self>,
        req: &Request<Bytes>,
        config: &Config,
        target: &PlayTarget,
        phases: &mut String,
    ) -> Resp {
        let start = Instant::now();
        let rid = rid(req);
        let rt = ResolveTarget {
            info_hash: target.info_hash.clone(),
            file_idx: target.file_idx,
            season: target.season,
            episode: target.episode,
            release_size: target.release_size,
            title: target.title.clone(),
            imdb: target.imdb.clone(),
            // ?prefetch=1 — nobody is waiting on this add, so it may not spend the adds kept for Play.
            prefetch: query_is(req, "prefetch", "1"),
            ..Default::default()
        };
        let resp = self.answer_play(req, config, target, rt.clone(), rid.clone(), start, phases).await;
        let route = if query_is(req, "probe", "1") { "probe" } else { "play" };
        crate::releaselog::observe(&rt, &rid, route, start.elapsed(), &resp);
        resp
    }

    /// `resolve_play`'s answer for `rt`, before it is logged as the release's state.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn answer_play(
        self: &Arc<Self>,
        req: &Request<Bytes>,
        config: &Config,
        target: &PlayTarget,
        rt: ResolveTarget,
        rid: String,
        start: Instant,
        phases: &mut String,
    ) -> Resp {
        let pool = StorePool { stores: (self.deps.make_stores)(config) };
        let resolve_deadline = Self::deadline_in(RESOLVE_BUDGET);
        let status_budget = self.status_budget();

        // ?probe=1 — answer, don't act. Asking /play for an uncached release is what QUEUES it, so a client
        // polling this URL to render a progress bar was adding the torrent again on every poll. TorBox allows
        // 60 adds an hour; a single three-minute wait spent thirty-six of them, and the account was throttled
        // out of playing anything at all. A probe reports what is true right now and starts nothing.
        if query_is(req, "probe", "1") {
            // On the STATUS budget, not the resolve one. A probe answers and starts nothing, and the client
            // polls it on a two-second cadence — so it belongs under the ceiling statusBudget's own comment
            // describes ("far under the resolve budget so a wait answers promptly instead of hanging the
            // poll"), rather than pinning a task and a connection for forty-five seconds against a slow
            // debrid.
            //
            // It is NOT free, and an earlier note here saying "up to three upstream calls, all reads" is no
            // longer true of either half. Since the readiness enquiry started asking any store that holds an
            // id — the fix for a probe that answered 404 for a release /play served — a poll against a
            // Real-Debrid holding walks that store's read chain: 2 calls when the release is ready (info,
            // unrestrict/link — it was 4 until resolveExisting stopped re-making a selection RD had already
            // made) and 3 while it is still downloading (info, selectFiles, info), at a two-second cadence,
            // with a state-changing selectFiles on each downloading poll and a fresh unrestricted link minted
            // and discarded on every ready poll. /play walks the same chain on the same cadence, so
            // this is the cost of the two routes agreeing rather than a new class of work — but it is real,
            // and it is recorded in FOLLOWUP.md rather than left as a comment claiming otherwise.
            //
            // The three SHARE this budget rather than each getting a slice, and the honest consequence is that
            // a status read slow enough to spend all eight seconds leaves the cache check a dead clock, so
            // the probe answers 503 "could not ask" for the length of that wait. That is the safe direction —
            // it never claims an absence — and slicing here is not obviously better: the pool already slices
            // per store, and stacking a second division on top is how an earlier fix left every store a budget
            // too short to answer in. Left alone deliberately; revisit if a real account is seen hitting it.
            let probe_deadline = Self::deadline_in(status_budget);
            return self.handle_probe(config, &pool, &target.info_hash, &rt, &rid, probe_deadline).await;
        }

        // A link minted in the last few minutes for this same file is served as it is: no status read, no
        // resolve, nothing upstream at all. Before this, the stream list's probe minted a link and threw it
        // away, and the play that followed a second later minted the same file again.
        //
        // `fresh=1` is the client saying the link it last got would not open. That link is dropped rather
        // than kept as a fallback — it is the only evidence there is about it, and the evidence is bad — and
        // the resolve below mints its replacement. Dropped up front, not only overwritten on success, so a
        // resolve that fails this time does not leave the dead link to be served on the next plain request.
        let memo_key = link_memo_key(config, &rt);
        if query_is(req, "fresh", "1") {
            self.links.forget(&memo_key);
        } else if let Some(hit) = self.links.get(&memo_key, SystemTime::now()) {
            // A link /play minted was checked before it was served; one the probe minted was not. So the check
            // runs once per link rather than once per play. A remembered link that fails it is dropped and a
            // fresh one minted below, not answered as dead — it may only be the one that went stale.
            let verdict = if hit.checked {
                LinkVerdict::Playable
            } else {
                self.verify_link(&pool, &rt, &hit.link).await
            };
            if verdict == LinkVerdict::Playable || verdict == LinkVerdict::Unverified {
                if verdict == LinkVerdict::Playable {
                    self.links.mark_checked(&memo_key, &hit.link);
                }
                log_limited(
                    "play-memo-hit",
                    &play_decision(
                        "play",
                        "served",
                        &format!("link minted in the last {}", go_duration(LINK_MEMO_TTL)),
                        "",
                        start.elapsed(),
                        &rid,
                        &rt,
                    ),
                );
                return write_play_redirect(&hit.link);
            }
            self.links.forget(&memo_key);
        }

        // Held already? Then try to serve it before asking whether it is downloading.
        //
        // The status read below is a mylist round trip on TorBox and an info call on Real-Debrid, and for a
        // release the account has finished it only ever answers "not downloading" — after which the resolve
        // runs anyway. Every warm play paid for that answer. HoldingServices is a cache read per store, so a
        // release no store has an id for pays nothing here and goes down the old path untouched.
        //
        // NoAdd is what makes it safe to put this ahead of the status read, and the stores enforce it rather
        // than this caller promising it: a remembered id that turns out stale is exactly when a resolve would
        // reach for createtorrent, and a NoAdd resolve refuses instead. Any failure falls through to the
        // sequence below unchanged, so what this step cannot do costs one read-only attempt and nothing more.
        //
        // Skipped while /play is answering "downloading" for this file. A download in progress is when this
        // step always fails — no link yet — and each poll would pay a link request on top of the one status
        // read it needs. playPendingTTL lets it lapse soon after the polls stop.
        //
        // On a slice of the resolve clock, and HALF the status budget rather than all of it, because what
        // follows still needs its share: the ordinary status read, then — on the path about to add — the
        // escalated read, which declines unless twice its own budget remains. With shipped constants that is
        // 45 − 4 − 8 = 33 s left against the 32 it needs; a full 8 s here would leave 29 and switch the
        // escalation off for exactly the slow accounts it exists for.
        let pending_key = play_pending_key(&memo_key);
        let mut held_unresolved = false;
        if self.deps.cache.get(&pending_key).is_none() {
            let holders = pool.holding_services(&rt);
            if !holders.is_empty() {
                let read_only = ResolveTarget { no_add: true, ..rt.clone() };
                let held_start = Instant::now();
                let resolved =
                    timeout(self.warm_resolve_budget(), pool.resolve_cached_only(&read_only, &holders)).await;
                *phases = den_http::timing("held", held_start.elapsed());
                match resolved {
                    Ok(Ok(link)) => return self.serve_play_link(&pool, &rt, &memo_key, link).await,
                    other => {
                        held_unresolved = matches!(
                            &other,
                            Ok(Err(StoreError::Other(_)
                                | StoreError::Cancelled
                                | StoreError::Unavailable(_)))
                        );
                        let why = match other {
                            Ok(Err(e)) => e.to_string(),
                            _ => "context deadline exceeded".to_owned(),
                        };
                        log_limited(
                            "play-held-miss",
                            &play_decision(
                                "play",
                                "fallback",
                                &format!("held resolve failed ({why}), asking status"),
                                "",
                                start.elapsed(),
                                &rid,
                                &rt,
                            ),
                        );
                    }
                }
            }
        }

        // Already known to be downloading? Answer from the status alone. A waiting client polls this URL for
        // the whole fetch, and a full resolve is ~3 upstream calls (cache miss → add → link), so re-running it
        // per poll is how an account gets itself throttled — and a throttled 5xx is exactly what a client
        // cannot distinguish from a real answer.
        //
        // On the STATUS budget, like the probe route and the post-failure retry below. This one had kept the
        // 45-second resolve budget, so the read that exists to answer a poll PROMPTLY could hold the poll for
        // forty-five seconds against a slow debrid — the thing statusBudget's own comment forbids. Two of its
        // three siblings already agreed; this was the third.
        let status_deadline = Self::deadline_in(status_budget);
        let outcome = pool.status_outcome(&rt, Some(status_deadline)).await;
        if let ProviderStatus::Downloading(status) = &outcome.result {
            return self.write_play_queued(&pending_key, &rt, &rid, start.elapsed(), status);
        }
        let unknown = outcome.unresolved;

        // "Nobody is fetching it" and "I could not find out in time" are different answers, and only the
        // first may lead to an add.
        //
        // pool.Status reports both as ok=false, so a TIMED-OUT read fell through to the resolve below, which
        // queues the torrent — one that was very often already downloading. TorBox's Status makes two upstream
        // calls, one of them the account listing this package measures at ~13 MB on a 2,000-torrent account,
        // so an 8-second budget is genuinely reachable there. Measured against a 9-second listing: an add that
        // the old 45-second budget did not make. Opening a ten-episode season on a large account could spend
        // ten of the fifty hourly adds re-queueing torrents already in flight.
        //
        // Moving the budget back is not the fix — it was shortened deliberately, because this read answers a
        // poll on a two-second cadence and must not hold it for forty-five seconds. So the path that is ABOUT
        // TO SPEND AN ADD gets ONE more short read, on the budget below.
        //
        // Not the resolve budget, which is what the first version of this did and was worse than the bug it
        // fixed. The resolve clock starts at the top of this function, so a fresh resolveBudget for
        // the escalated read always outlives it: the read burned the whole 45 s, the clock was already dead when
        // ResolvePreferring got it, the add became impossible, and the release was NEVER queued. A permanently
        // slow listing turned into a permanent 404 with nothing downloading, which is not a trade, it is a
        // regression. Measured: 404 after ~61 s where the old code played in 8 s.
        //
        // So it is parented to the resolve clock, which makes outliving the resolve budget impossible by
        // construction, it is short, and it is skipped entirely unless enough budget remains to do the add
        // afterwards. If it still cannot tell, the resolve proceeds and may add — because a duplicate add is a
        // bounded, self-healing cost, and never starting the download is not.
        // Asked of the POOL, not inferred from this function's clock. The pool gives each store a slice of the
        // budget, so a store can time out while the pool still returns long before the caller's deadline —
        // "did my whole budget elapse" was false exactly when a store had failed to answer, which is the case
        // this branch exists for. The pool knows which store ran out of time, so it is the one that says.
        //
        // Only the stores that could not answer are re-asked. Going back through the whole pool re-ran a
        // question the other stores had already answered definitively — three store reads instead of one on a
        // three-account install, per poll, on a path that is already struggling.
        if !unknown.is_empty() {
            if let Some(slow_deadline) =
                escalated_status_ctx(Some(resolve_deadline), TokioInstant::now(), status_budget)
            {
                let outcome = status_outcome_among(&unknown, &rt, Some(slow_deadline)).await;
                if let ProviderStatus::Downloading(status) = &outcome.result {
                    log_limited(
                        "play-slow-status",
                        &play_decision(
                            "play",
                            "fallback",
                            &format!("status needed longer than {} to answer", go_duration(status_budget)),
                            "",
                            start.elapsed(),
                            &rid,
                            &rt,
                        ),
                    );
                    return self.write_play_queued(&pending_key, &rt, &rid, start.elapsed(), status);
                }
                let still_unknown = outcome.unresolved;
                if held_unresolved {
                    let Some(store) = still_unknown.first() else {
                        drop(still_unknown);
                        return write_unavailable(
                            STORE_UNANSWERED_WAIT,
                            &json!({"error": "status_unavailable"}),
                        );
                    };
                    let service = store.service();
                    log_limited(
                        "play-status-unknown",
                        &play_decision(
                            "play",
                            "refused",
                            "a holding store could not answer whether the release is downloading",
                            service.as_str(),
                            start.elapsed(),
                            &rid,
                            &rt,
                        ),
                    );
                    return write_unavailable(
                        STORE_UNANSWERED_WAIT,
                        &json!({"error": "status_unavailable", "service": service.as_str()}),
                    );
                }
            } else if held_unresolved {
                let Some(store) = unknown.first() else {
                    return write_unavailable(STORE_UNANSWERED_WAIT, &json!({"error": "status_unavailable"}));
                };
                return write_unavailable(
                    STORE_UNANSWERED_WAIT,
                    &json!({"error": "status_unavailable", "service": store.service().as_str()}),
                );
            }
        }
        drop(unknown);

        // Which services already hold it, so the resolve starts with one that can serve now rather than one
        // that would have to download it. Only worth asking with more than one account configured — with one
        // there is nothing to choose between, and it would be a wasted upstream call on every play.
        //
        // BUDGETED, like every other read on this path. Its only product is an ORDERING, so it must never be
        // able to cost the resolve it is ordering — and handed the bare resolve clock it could: a slow
        // checkcached ate what the two status reads left, ResolvePreferring then ran on an expired clock,
        // every store failed instantly, nothing was queued, and a healthy release came back 404.
        //
        // That is the regression escalatedStatusCtx exists to prevent, one line further down and not caught
        // by it, because the escalation was given a carved-out slice and this call was not. Measured with
        // shipped constants on a two-account install: 404 after 53.0s with the check holding the clock for
        // 21.0s, against 302 in 195µs when it answers at once.
        //
        // Failure here is NOT free, and an earlier note claiming it was is corrected rather than kept. Losing
        // the ordering means the pool tries configured order, so an account that would have served the
        // release from cache can be tried after one that has to fetch it — measured at one add spent and a
        // download the viewer waits on for a file another of their accounts already held.
        //
        // Capped anyway, because the two costs are not comparable: losing the ordering costs at most one add
        // on a release that still plays, while losing the RESOLVE costs a 404 on a live release AND an add
        // per configured store. The trade is a real one and it has a window — with shipped constants the
        // check is reached at t=24s with 21s left, so a checkcached answering between 8s and ~21s used to
        // order correctly and now does not. Under 8s both behave the same; over ~21s the old code produced
        // the 404 this cap exists to remove.
        //
        // The FREE half first, and unconditionally: a store that already has an id for this release can serve
        // it with no add at all, and finding that out is a cache read per store.
        //
        // It is also better evidence than the cache check. TorBox's checkcached reports what TORBOX has, not
        // what this account has; Real-Debrid contributes no cache truth whatsoever — its CacheCheck answers
        // all-false by design, so HeldBy can never name it; and Premiumize drops out whenever its own check
        // is throttled or times out. Ordering on the cache check alone therefore made a release the SECOND
        // account already held invisible, so configured order won and TorBox — always first — was asked to
        // resolve, which means createtorrent.
        //
        // Measured on a TorBox + Real-Debrid install with RD already holding the torrent: /play posted
        // createtorrent to TorBox, spent one of the fifty and left a duplicate torrent there — then the
        // probe, which consults Status first, found TorBox fetching and told a client that had been answering
        // "ready" for three polls to keep waiting for a download it never needed. With this line the same
        // play resolves from RD in four upstream calls instead of six, spends nothing, and the probe stays
        // ready throughout.
        //
        // This is the enquiry handleProbe was given for exactly this reason; it was never applied to the
        // route that pays for being wrong.
        let mut preferred = pool.holding_services(&rt);
        if config.debrid.len() > 1 {
            let check_deadline = Self::deadline_in(status_budget).min(resolve_deadline);
            if let Ok((play_truth, _)) =
                timeout_at(check_deadline, pool.cache_check(std::slice::from_ref(&target.info_hash))).await
            {
                preferred.extend(play_truth.held_by(&target.info_hash));
            }
        }
        let resolve_start = Instant::now();
        let resolved = match timeout_at(resolve_deadline, pool.resolve_preferring(&rt, &preferred)).await {
            Ok(r) => r,
            // The resolve clock ran out before any store answered: named as SCOUT's, because that is whose clock
            // ran out, exactly as the pool names a store that reports the cancellation.
            Err(_) => Err(StoreError::ScoutSide(StoreUnavailableError {
                service: pool.stores.first().map_or(DebridService::TorBox, |s| s.service()),
                reason: "the resolve budget was spent before a store could answer".to_owned(),
                status: 0,
            })),
        };
        if !phases.is_empty() {
            phases.push_str(", ");
        }
        phases.push_str(&den_http::timing("resolve", resolve_start.elapsed()));
        let err = match resolved {
            Ok(link) => return self.serve_play_link(&pool, &rt, &memo_key, link).await,
            Err(err) => err,
        };
        // Queued is not dead. An uncached release is added to the debrid by the Resolve above and then
        // takes minutes to land; answering 404 for that made the client remember a perfectly good release
        // as dead (and every uncached one below it, since they all fail the same way). 202 says "coming",
        // with whatever progress the store actually reports — the client polls this same URL and plays
        // when it turns into the usual 302.
        //
        // A FRESH clock: the resolve above may have failed by exhausting `resolveBudget` (a slow add on
        // an uncached release is exactly that case), and reusing a spent clock would turn every such
        // wait into a 404.
        let outcome = pool.status_outcome(&rt, Some(Self::deadline_in(status_budget))).await;
        if let ProviderStatus::Downloading(status) = &outcome.result {
            return self.write_play_queued(&pending_key, &rt, &rid, start.elapsed(), status);
        }
        // An add scout already sent is not a refusal at all — the release is being fetched, by us, right
        // now. Answering 503 made the client tell the viewer their debrid was refusing AND stop trying
        // other sources, for a release scout had queued moments earlier. 202 is simply what is true.
        if matches!(err, StoreError::AddInFlight) {
            log_limited(
                "play-add-in-flight",
                &play_decision("play", "fallback", "add already in flight", "", start.elapsed(), &rid, &rt),
            );
            return self.write_play_queued(&pending_key, &rt, &rid, start.elapsed(), &StoreStatus::default());
        }
        // TorBox parked the add in its own queue (queued_id, not torrent_id — see errAddQueued). Also
        // "coming", not dead: before this decode existed, the same answer fell through to the final 404
        // below and the client blacklisted a release TorBox was about to fetch.
        if matches!(err, StoreError::AddQueued) {
            // Only TorBox hands back queued_id/errAddQueued at all (see storeCanceller's sibling comment on
            // addMagnet), so naming it here is accurate rather than a guess — and the client needs it to
            // show "Queued at TorBox" rather than a bare, serviceless "queued".
            log_limited(
                "play-add-queued",
                &play_decision(
                    "play",
                    "fallback",
                    "queued behind the account's slot limit",
                    DebridService::TorBox.as_str(),
                    start.elapsed(),
                    &rid,
                    &rt,
                ),
            );
            let queued = StoreStatus {
                state: FETCH_QUEUED.to_owned(),
                service: Some(DebridService::TorBox),
                ..StoreStatus::default()
            };
            return self.write_play_queued(&pending_key, &rt, &rid, start.elapsed(), &queued);
        }
        // A refusal SCOUT made — its hourly allowance — is not the debrid refusing, and must not be
        // reported as one. errScoutSide was added to keep these apart in the refusal memory, and then the
        // route went on saying "torbox" anyway: the app told the viewer the debrid was refusing while
        // TorBox was answering perfectly well. Still a 503, because it is still "not now, try again", but
        // named as ours.
        // A prefetch held back so a viewer pressing Play still has adds. Its own code, because the client
        // treats it differently from a spent budget: a queued download pauses until Retry-After and resumes,
        // and nothing about the release is held against it.
        if is_play_reserve(&err) {
            log_limited(
                "play-reserved",
                &play_decision(
                    "play",
                    "refused",
                    &format!("prefetch refused: {}", scout_side_reason(&err)),
                    "",
                    start.elapsed(),
                    &rid,
                    &rt,
                ),
            );
            return write_unavailable(
                scout_busy_for(&pool, true),
                &json!({"detail": scout_side_reason(&err), "error": "reserved_for_play"}),
            );
        }
        if matches!(err, StoreError::ScoutSide(_)) {
            log_limited(
                "play-scout-busy",
                &play_decision("play", "refused", &scout_side_reason(&err), "", start.elapsed(), &rid, &rt),
            );
            return write_add_budget_spent(
                scout_busy_for(&pool, false),
                &json!({"detail": scout_side_reason(&err), "error": "scout_busy"}),
            );
        }
        // A debrid that throttled or faulted is not a dead release, and answering 404 made the two
        // identical: the client fell through every remaining source collecting the same non-answer, then
        // waited indefinitely on a download nothing had started. 503 says whose problem it is.
        if let Some(unavailable) = as_unavailable(&err) {
            log_limited(
                "play-store-unavailable",
                &play_decision(
                    "play",
                    "refused",
                    &unavailable.reason,
                    unavailable.service.as_str(),
                    start.elapsed(),
                    &rid,
                    &rt,
                ),
            );
            return write_unavailable(
                STORE_REFUSAL_WAIT,
                &json!({"error": "store_unavailable", "service": unavailable.service.as_str()}),
            );
        }
        // The client reads this 404 as "still fetching" and polls for as long as the viewer will sit
        // there, so it is worth saying which of the two happened: no store could add it, and no store
        // admits to downloading it either. A wait with nothing behind it is the one case a spinner
        // cannot distinguish from a slow release.
        log_limited(
            "play-dead",
            &play_decision(
                "play",
                "skipped",
                "no store resolved it and none reports a download",
                "",
                start.elapsed(),
                &rid,
                &rt,
            ),
        );
        error(StatusCode::NOT_FOUND, "dead_link")
    }

    /// servePlayLink answers /play with a link a store has just minted: checked, remembered, redirected.
    ///
    /// A link that fails its check is answered exactly as a release no store could resolve — 404 dead_link —
    /// because to the client it is the same thing: this source will not play, try the next one. It is not
    /// remembered, and anything remembered under the key goes too.
    ///
    /// The check runs on its own clock rather than the resolve clock. A resolve that took most of its
    /// budget would otherwise leave the check nothing, and a check with no time is a check that cannot answer;
    /// linkCheckTimeout bounds it on its own.
    ///
    /// A movie link that serves a file far smaller than the release gets one more try: see relistMovie. What that
    /// try mints is served only if it passes its own check. Otherwise the first link is served as it was before
    /// the release size was known — a collection's film, or a store with no file list to pick from, is still
    /// the file that was asked for, and a size alone is not proof enough to refuse it.
    pub(super) async fn serve_play_link(
        &self,
        pool: &StorePool,
        rt: &ResolveTarget,
        memo_key: &str,
        mut link: String,
    ) -> Resp {
        let mut verdict = self.verify_link(pool, rt, &link).await;
        if verdict == LinkVerdict::WrongFile {
            self.links.forget(memo_key);
            verdict = LinkVerdict::Unverified;
            let (relisted, v) = self.relist_movie(pool, rt).await;
            if v == LinkVerdict::Playable || v == LinkVerdict::Unverified {
                link = relisted;
                verdict = v;
            }
        }
        if verdict == LinkVerdict::Broken {
            self.links.forget(memo_key);
            return error(StatusCode::NOT_FOUND, "dead_link");
        }
        self.links.put(memo_key, &link, verdict == LinkVerdict::Playable, SystemTime::now());
        write_play_redirect(&link)
    }

    /// relistMovie re-resolves a movie whose link served another file in the release, this time with the
    /// torrent's file list, and checks what that mints.
    ///
    /// TorBox resolves a movie without listing files, so the indexer's fileIdx — a position in the torrent — goes
    /// out raw as TorBox's file id, and on a torrent TorBox lists in another order that id names a sample or an
    /// extra: a 522 KB file for a 16 GB remux, with a 302 and no error. Listing on every movie play would cost a
    /// mylist call in front of each warm link, and nearly every movie carries fileIdx 0, so the list is fetched
    /// only here, once the check has shown the raw position was wrong. selectFileID then maps the position
    /// through the list, with its guard against a position that lands on something not feature-sized.
    ///
    /// Read-only (NoAdd) and only against the stores that hold the torrent: the link that failed came from one,
    /// and nothing about a wrong file is a reason to buy the torrent again. The list lands in TorBox's resolve
    /// entry, so later plain resolves of this hash take the fast path with it and pick the right file without
    /// listing again — and the file's size is known from it, which is what lets the check below judge a
    /// collection's film against its own listed size rather than against the whole pack's.
    ///
    /// On its own slice of the clock, not the resolve clock: this runs after a resolve that may have
    /// spent most of that clock, and a re-resolve on an expired clock fails instantly and turns a playable
    /// release into a 404. statusBudget is the same ceiling the other single-torrent reads on this route use; one
    /// mylist and one requestdl fit well inside it.
    ///
    /// Any failure is linkBroken, on which servePlayLink serves the first link as it did before this existed.
    pub(super) async fn relist_movie(&self, pool: &StorePool, rt: &ResolveTarget) -> (String, LinkVerdict) {
        let start = Instant::now();
        let rid = String::new();
        let listed = ResolveTarget { no_add: true, list_files: true, ..rt.clone() };
        let resolved = match timeout(
            self.status_budget(),
            pool.resolve_cached_only(&listed, &pool.holding_services(rt)),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => Err(StoreError::Cancelled),
        };
        let link = match resolved {
            Ok(link) => link,
            Err(e) => {
                log_limited(
                    "play-relist-failed",
                    &play_decision(
                        "play",
                        "fallback",
                        &format!("re-resolve with a file list failed: {e}"),
                        "",
                        start.elapsed(),
                        &rid,
                        rt,
                    ),
                );
                return (String::new(), LinkVerdict::Broken);
            }
        };
        let verdict = self.verify_link(pool, rt, &link).await;
        if verdict == LinkVerdict::Playable || verdict == LinkVerdict::Unverified {
            log_limited(
                "play-relist-served",
                &play_decision(
                    "play",
                    "served",
                    "re-resolve with a file list picked another file",
                    "",
                    start.elapsed(),
                    &rid,
                    rt,
                ),
            );
        }
        (link, verdict)
    }

    /// Go `(*handler).verifyLink`: whether a freshly minted link is the file it should be.
    pub(super) async fn verify_link(&self, pool: &StorePool, rt: &ResolveTarget, link: &str) -> LinkVerdict {
        let doer = self.probe_doer();
        crate::linkcheck::verify_link(doer.as_deref(), known_file_size(pool, rt), rt, link).await
    }

    /// The client the link check and token validation go through: `Deps::probe_doer` when a test set one, else the
    /// probe client.
    pub(super) fn probe_doer(&self) -> Option<Arc<dyn Doer>> {
        self.deps
            .probe_doer
            .clone()
            .or_else(|| self.deps.probe_client.clone().map(|c| Arc::new(ReqwestDoer(c)) as Arc<dyn Doer>))
    }

    /// handleProbe answers "can this play yet?" without changing anything. Same vocabulary as /play so the
    /// client reads one set of statuses: 202 while downloading, 200 once the store holds it, 404 when neither
    /// is true — which here means "nothing has been queued", not "this release is dead".
    pub(super) async fn handle_probe(
        &self,
        config: &Config,
        pool: &StorePool,
        info_hash: &str,
        rt: &ResolveTarget,
        rid: &str,
        deadline: TokioInstant,
    ) -> Resp {
        let start = Instant::now();
        let outcome = pool.status_outcome(rt, Some(deadline)).await;
        if let ProviderStatus::Downloading(status) = &outcome.result {
            return write_queued(rt, rid, start.elapsed(), status);
        }
        let unknown = !outcome.unresolved.is_empty();
        // "Ready" has to mean the ACCOUNT can serve it without queueing anything, which a cache check cannot
        // establish on its own: TorBox's checkcached reports what TorBox has, not what this account has. So
        // the claim is settled by a NoAdd resolve — the stores refuse rather than queue — and a cached-but-
        // not-held release correctly falls through to the "nothing queued" answer instead of promising a
        // playback that would start with a download.
        let (probe_truth, truth_ok) = match timeout_at(deadline, pool.cache_check(&[info_hash.to_owned()]))
            .await
        {
            Ok(answer) => answer,
            Err(_) => (CacheTruth { holders: HashMap::new(), known: HashMap::new(), complete: false }, false),
        };
        let mut refused_us: Option<StoreUnavailableError> = None;
        // Cached OR already held. The cache check alone was the wrong question, and asking it alone left this
        // enquiry unreachable on a Real-Debrid install: RD publishes no cache API and answers all-false by
        // design, so HeldBy is always empty and ResolveCachedOnly — which refuses outright with no holders —
        // was never given anything to ask. The route then answered 404 "not queued" for a release /play
        // served a 302 for, from the same cache entry.
        //
        // HoldingServices is a cache read per store, so this costs nothing where it changes nothing.
        let mut holders = probe_truth.held_by(info_hash);
        holders.extend(pool.holding_services(rt));
        if !holders.is_empty() {
            let read_only = ResolveTarget { no_add: true, ..rt.clone() };
            let resolved = match timeout_at(deadline, pool.resolve_cached_only(&read_only, &holders)).await {
                Ok(r) => r,
                Err(_) => Err(StoreError::Cancelled),
            };
            match resolved {
                Ok(_) => {
                    log_limited(
                        "probe-ready",
                        &play_decision("probe", "served", "ready", "", start.elapsed(), rid, rt),
                    );
                    return json_response(StatusCode::OK, &json!({"state": "ready"}));
                }
                // The probe DOES discover refusals — it just used to throw them away, inspecting only `err ==
                // nil` and then relying on the backoff cache to have been written by someone else. Any refusal
                // that reaches here unrecorded fell through to the 404 below, which this file calls a claim
                // rather than a shrug: `errNoFileList` is never recorded at all, and TorBox's warm-entry fast
                // path returns a refusal without recording it, so during a throttle the very URL the client
                // polls to draw its progress bar said "nothing is queued" while /play said the debrid was
                // refusing. Reading the error we already have needs no cache to have been written first.
                Err(err) => refused_us = as_unavailable(&err).cloned(),
            }
        }
        // An add of OURS that is out and unanswered means the release is being fetched right now, and it
        // outranks every refusal below — the same precedence /play gives errAddInFlight, because it is a
        // fact about us rather than about a service.
        //
        // The probe cannot learn it any other way: it never resolves, and all three stores answer a NoAdd
        // target before consulting the marker, so without this branch the state /play reports as 202 reached
        // this route as nothing and fell through to the 404 below — the single failure this route exists to
        // prevent, on the URL the client polls for its progress bar. Real-Debrid and Premiumize have no
        // Status to clear it, so the disagreement stood the full 90s addAttemptTTL.
        //
        // It outranks a rejected key ACROSS stores too — the order ResolvePreferring uses,
        // where `coming` is returned before `refused` so store order cannot decide the verdict.
        //
        // The per-store rule points the other way (a dead key is not a wait, and every store's Resolve puts
        // accountBackedOff above its own addInFlight), and an earlier version of this route applied that rule
        // at pool level. That made a dead key on one account outrank a live add on another: with TorBox's key
        // expired and Real-Debrid fetching, the probe answered 503 naming torbox while /play answered 202
        // downloading. AddInFlight now applies the per-store rule per store, so both hold at once.
        if pool.add_in_flight(info_hash) {
            log_limited(
                "probe-add-in-flight",
                &play_decision("probe", "fallback", "add already in flight", "", start.elapsed(), rid, rt),
            );
            return write_queued(rt, rid, start.elapsed(), &StoreStatus::default());
        }
        // No store with a usable key is fetching it, so a rejected key is now the best explanation there is.
        //
        // Account-level only, deliberately. The per-release backoff belongs below, where RecentRefusal reads
        // it, because that one is an add-path guard a read-only caller is exempt from.
        if let Some((svc, reason)) = pool.account_refusal() {
            log_limited(
                "probe-account-refused",
                &play_decision(
                    "probe",
                    "refused",
                    &format!("account refused: {reason}"),
                    svc.as_str(),
                    start.elapsed(),
                    rid,
                    rt,
                ),
            );
            return write_unavailable(
                STORE_REFUSAL_WAIT,
                &json!({"error": "store_unavailable", "service": svc.as_str()}),
            );
        }
        // A refusal SCOUT made, above the store refusals below because it is ours rather than a service's.
        //
        // NOT identical to /play's ranking, and an earlier note here claiming it was is corrected: /play
        // takes the FIRST StoreUnavailableError in store order, scout-side or not, so with every budget
        // spent AND a per-release store refusal already on record the two routes name different causes —
        // probe scout_busy, /play store_unavailable. Both are 503 and the client retries either way, so the
        // difference is in the detail a human reads, not in behaviour. Left as it is: naming scout's own
        // ceiling is the more useful answer of the two, and reproducing /play's first-error-wins ordering
        // here would mean resolving, which this route must not do.
        //
        // The probe had no channel for it at all — recordRefusal excludes
        // errScoutSide by design, so the backoff memory this route reads stays empty and the request fell
        // through every "could not ask" guard to 404 "not queued". Measured with the hourly allowance spent:
        // /play answered 503 scout_busy while ?probe=1 answered 404, for the same release at the same
        // instant, for the rest of the rolling hour.
        if pool.every_add_refused_by_scout() {
            log_limited(
                "probe-scout-busy",
                &play_decision(
                    "probe",
                    "refused",
                    "scout's hourly add allowance is spent",
                    "",
                    start.elapsed(),
                    rid,
                    rt,
                ),
            );
            return write_add_budget_spent(
                scout_busy_for(pool, false),
                &json!({
                    "detail": "scout's own hourly add budget for this account is spent",
                    "error": "scout_busy",
                }),
            );
        }
        if let Some(refused) = refused_us {
            log_limited(
                "probe-refused",
                &play_decision(
                    "probe",
                    "refused",
                    &refused.reason,
                    refused.service.as_str(),
                    start.elapsed(),
                    rid,
                    rt,
                ),
            );
            return write_unavailable(
                STORE_REFUSAL_WAIT,
                &json!({"error": "store_unavailable", "service": refused.service.as_str()}),
            );
        }
        // A refusal the store recorded moments ago outranks "nothing queued". Without this the probe reports
        // a throttled account as an absence, which reads as a release nobody can deliver — and the client
        // condemns a perfectly good one. This still matters for a refusal recorded by an EARLIER poll, whose
        // store the probe may not reach again.
        if let Some((svc, reason)) = pool.recent_refusal(info_hash) {
            log_limited(
                "probe-refused",
                &play_decision("probe", "refused", &reason, svc.as_str(), start.elapsed(), rid, rt),
            );
            return write_unavailable(
                STORE_REFUSAL_WAIT,
                &json!({"error": "store_unavailable", "service": svc.as_str()}),
            );
        }
        // With the cache check down we do not know whether anything is queued, and 404 "not_queued" is a
        // claim, not a shrug — the client reads it as a release nobody has. Say the store could not be asked.
        if !truth_ok && has_cache_truth(config) {
            log_limited(
                "probe-cache-check-down",
                &play_decision("probe", "degraded", "cache check unavailable", "", start.elapsed(), rid, rt),
            );
            return write_unavailable(STORE_UNANSWERED_WAIT, &json!({"error": "cache_check_unavailable"}));
        }
        // Same rule one step further out: a store that could not answer is not a store saying nothing is
        // queued. 404 here is what makes a client blacklist a release, which is the single failure this route
        // exists to prevent, so an indeterminate read gets the "ask again" answer rather than the claim.
        if unknown {
            log_limited(
                "probe-status-unknown",
                &play_decision("probe", "degraded", "a store could not answer", "", start.elapsed(), rid, rt),
            );
            return write_unavailable(STORE_UNANSWERED_WAIT, &json!({"error": "status_unavailable"}));
        }
        log_limited(
            "probe-not-queued",
            &play_decision("probe", "skipped", "not queued", "", start.elapsed(), rid, rt),
        );
        error(StatusCode::NOT_FOUND, "not_queued")
    }

    // --- cancel -----------------------------------------------------------------------------------------

    /// handleCancel is DELETE /p/<ticket> (and, for parity, DELETE /<config>/play/<token>) — den#204. Both
    /// routes open to the same target as their GET siblings and share resolveCancel.
    pub(super) async fn handle_cancel(self: &Arc<Self>, req: &Request<Bytes>, path: &str) -> Resp {
        let parts = split_path(path);
        if parts.len() == 2 && parts[0] == TICKET_ROUTE && self.tickets.is_some() {
            return self.handle_ticket_cancel(req, parts[1]).await;
        }
        if parts.len() == 3 && parts[1] == "play" {
            return self.handle_legacy_cancel(req, parts[0], parts[2]).await;
        }
        method_not_allowed("GET, HEAD")
    }

    pub(super) async fn handle_ticket_cancel(&self, req: &Request<Bytes>, ticket: &str) -> Resp {
        match self.tickets.as_ref().map(|t| t.open(ticket, SystemTime::now())) {
            Some(Err(TicketError::Expired)) => error(StatusCode::GONE, "ticket_expired"),
            Some(Ok((config, target))) if self.admit_install(&config) => {
                self.resolve_cancel(req, &config, &target).await
            }
            _ => error(StatusCode::BAD_REQUEST, "bad_ticket"),
        }
    }

    pub(super) async fn handle_legacy_cancel(
        &self,
        req: &Request<Bytes>,
        config_blob: &str,
        token: &str,
    ) -> Resp {
        if self.tickets.is_some()
            && self.deps.legacy_play_until.is_some_and(|until| SystemTime::now() >= until)
        {
            return error(StatusCode::FORBIDDEN, "legacy_play_closed");
        }
        let Some(target) = decode_play_token(token) else {
            return error(StatusCode::BAD_REQUEST, "bad_token");
        };
        let Some(config) = self.open_config(config_blob) else {
            return error(StatusCode::BAD_REQUEST, "bad_config");
        };
        if let Some(refused) = self.refuse_scoped(req, &config) {
            return refused;
        }
        self.resolve_cancel(req, &config, &target).await
    }

    /// resolveCancel is the cancel route's work once a target and its config are admitted. It asks every
    /// configured debrid account that can cancel, in configured order, and answers with the first one that has
    /// an opinion about this hash — see storeCanceller's own comment for why only TorBox implements it.
    pub(super) async fn resolve_cancel(
        &self,
        req: &Request<Bytes>,
        config: &Config,
        target: &PlayTarget,
    ) -> Resp {
        let start = Instant::now();
        let rid = rid(req);
        let rt = ResolveTarget {
            info_hash: target.info_hash.clone(),
            file_idx: target.file_idx,
            season: target.season,
            episode: target.episode,
            title: target.title.clone(),
            imdb: target.imdb.clone(),
            ..Default::default()
        };
        let reannounce = query_is(req, "op", "reannounce");
        let deadline = Self::deadline_in(self.status_budget());
        let stores = (self.deps.make_stores)(config);
        let mut saw_canceller = false;
        for st in &stores {
            let answered = match timeout_at(deadline, st.cancel(&target.info_hash, reannounce)).await {
                Ok(None) => continue,
                Ok(Some(answer)) => answer,
                Err(_) => Err(StoreError::Cancelled),
            };
            saw_canceller = true;
            let outcome = match answered {
                Ok(outcome) => outcome,
                Err(err) => {
                    if let Some(unavailable) = as_unavailable(&err) {
                        log_limited(
                            "cancel-store-unavailable",
                            &play_decision(
                                "cancel",
                                "refused",
                                &unavailable.reason,
                                unavailable.service.as_str(),
                                start.elapsed(),
                                &rid,
                                &rt,
                            ),
                        );
                        return write_unavailable(
                            STORE_REFUSAL_WAIT,
                            &json!({"error": "store_unavailable", "service": unavailable.service.as_str()}),
                        );
                    }
                    log_limited(
                        "cancel-error",
                        &play_decision(
                            "cancel",
                            "degraded",
                            &err.to_string(),
                            "",
                            start.elapsed(),
                            &rid,
                            &rt,
                        ),
                    );
                    return write_unavailable(STORE_REFUSAL_WAIT, &json!({"error": "store_unavailable"}));
                }
            };
            if outcome.reason == "not_queued" {
                // This account has no opinion; the hash may still belong to another configured account.
                continue;
            }
            return write_cancel_outcome(&rt, &rid, start, &outcome);
        }
        if !saw_canceller {
            log_limited(
                "cancel-unsupported",
                &play_decision(
                    "cancel",
                    "skipped",
                    "no configured account supports it",
                    "",
                    start.elapsed(),
                    &rid,
                    &rt,
                ),
            );
            return error(StatusCode::NOT_IMPLEMENTED, "unsupported");
        }
        log_limited(
            "cancel",
            &play_decision("cancel", "skipped", "nothing queued", "", start.elapsed(), &rid, &rt),
        );
        error(StatusCode::NOT_FOUND, "not_queued")
    }
}

pub(super) fn write_play_redirect(link: &str) -> Resp {
    let mut response = Response::new(full(""));
    *response.status_mut() = StatusCode::FOUND;
    set_header(&mut response, http::header::LOCATION, link);
    response.headers_mut().insert(CACHE_CONTROL, HeaderValue::from_static(NO_STORE));
    response.headers_mut().insert(http::header::CONTENT_LENGTH, HeaderValue::from_static("0"));
    response
}

pub(super) fn write_cancel_outcome(
    target: &ResolveTarget,
    request_id: &str,
    started: Instant,
    outcome: &crate::stores::CancelOutcome,
) -> Resp {
    match outcome.reason.as_str() {
        "finished" | "not_ours" => {
            log_limited(
                "cancel",
                &play_decision(
                    "cancel",
                    "refused",
                    &outcome.reason,
                    "",
                    started.elapsed(),
                    request_id,
                    target,
                ),
            );
            return error(StatusCode::CONFLICT, &outcome.reason);
        }
        _ => {}
    }
    if outcome.reannounced {
        log_limited(
            "cancel",
            &play_decision("cancel", "served", "reannounced", "", started.elapsed(), request_id, target),
        );
        return write_queued_body(&outcome.status);
    }
    log_limited(
        "cancel",
        &play_decision("cancel", "served", "cancelled", "", started.elapsed(), request_id, target),
    );
    let mut response = Response::new(full(""));
    *response.status_mut() = StatusCode::NO_CONTENT;
    response
}
