//! Alignment workflow: cache policy, flight settlement, and sync ladder execution.

use super::*;

pub(super) struct SyncWorkflowInput<'a> {
    pub(super) cache_key: String,
    pub(super) target: Option<String>,
    pub(super) translated_body_key: Option<&'a str>,
    pub(super) ref_id: Option<i64>,
    pub(super) resync_url: Option<String>,
    pub(super) resync_audio: Option<crate::sync::AudioStream>,
    pub(super) reservation: Option<SyncReservation<'a>>,
    pub(super) flight: Option<InFlightGuard>,
    pub(super) what: &'a str,
    pub(super) settled: bool,
    pub(super) log: ServeCtx<'a>,
    pub(super) sparse_fallback: bool,
}

/// Explicit workflow result so serving policy remains outside the alignment implementation when
/// callers need to add endpoint-specific timing, validators, or degraded headers.
pub(super) struct SyncWorkflowOutput {
    pub(super) response: Response<Body>,
}

pub(super) async fn run_alignment_workflow<'a>(
    state: &Arc<AppState>,
    client: &opensubtitles::Client<'_>,
    input: SyncWorkflowInput<'a>,
) -> SyncWorkflowOutput {
    let SyncWorkflowInput {
        cache_key,
        target,
        translated_body_key,
        ref_id,
        resync_url,
        resync_audio,
        mut reservation,
        flight,
        what,
        settled,
        log,
        // Overrides a successful `served` line's reason to `sparse_fallback` — the owner's call
        // that a real-but-sparse track beats none when it's all a language has.
        sparse_fallback,
    } = input;
    // A sync that just failed is not retried on every request — a binary spawn, or a 90s Tier-2
    // timeout, would be paid again per request. The marker is separate from `cache_key` so that key
    // never holds anything but a settled answer.
    //
    // TWO markers, because there are two kinds of failure here and they have different scopes. A
    // subprocess that failed or timed out is a fact about the work: the same alignment will fail for
    // everyone, so that is remembered globally and one install's attempt spares the rest. A
    // reference download refused as `Unavailable` is a fact about ONE credential — an exhausted
    // daily quota, a revoked key, that install's own rate limit — and remembering it globally let
    // an install that had run out of downloads serve every other install the unaligned body for ten
    // minutes, re-armed each time it polled. `subtitle_srt` already files that failure under
    // `unavailable_file_key`, keyed by the credential; this is the same fact one level up, and it
    // is keyed the same way for the same reason: two installs sharing an OpenSubtitles key really
    // do share the quota.
    let alignment = crate::cache_policy::AlignmentKind::from_inputs(resync_url.is_some(), ref_id.is_some());
    let wanted_sync = alignment.wanted();
    let (shared_marker, mine_marker) =
        crate::cache_policy::sync_failure_markers(SYNCFAIL, &cache_key, os_download_identity(client));
    let backed_off = || state.cache.get(&shared_marker).is_some() || state.cache.get(&mine_marker).is_some();
    // One tier binary per key at a time. Concurrent requests for the same alignment were each
    // spawning their own — a 90s Tier-2 run against the same stream, on a runtime with one thread —
    // and the scratch-file tag below only made that safe, never rare.
    let _flight = if wanted_sync {
        let guard = match flight {
            Some(guard) => guard,
            None => state.inflight.acquire(&cache_key).await,
        };
        // Settled while we waited: the alignment we were about to run has already been run.
        if let Some(hit) = state.cache.get_shared(&cache_key) {
            log_serve_cached(state, log, "served", "settled_while_waiting", &hit);
            let resp = with_cached_subtitle_offset(
                state,
                &cache_key,
                httputil::srt_shared(hit.to_bytes(), hit.etag()),
            );
            return SyncWorkflowOutput { response: httputil::add_timing(resp, CACHE_HIT) };
        }
        // And it may have failed while we waited, in which case re-running it now is the retry the
        // marker exists to prevent.
        if backed_off() {
            let Some(target) = retained_sync_target(state, target, translated_body_key) else {
                log_serve(state, log, "refused", "translation_unavailable", None, "miss", None);
                return SyncWorkflowOutput {
                    response: httputil::error(StatusCode::SERVICE_UNAVAILABLE, "translation_unavailable"),
                };
            };
            log_serve(state, log, "degraded", "sync_backoff", None, "miss", Some(&target));
            return SyncWorkflowOutput {
                response: httputil::degraded(httputil::srt_provisional(target), "sync_failed"),
            };
        }
        Some(guard)
    } else {
        None
    };

    // Translations are durable before alignment. Their caller deliberately drops its String before
    // entering this function; only the exact-key leader reserves body capacity and reloads it.
    if wanted_sync && reservation.is_none() {
        let tier = if resync_url.is_some() { SyncTier::Tier2 } else { SyncTier::Tier1 };
        reservation = Some(state.sync_admission.reserve(tier).await);
    }
    let Some(target) = retained_sync_target(state, target, translated_body_key) else {
        log_serve(state, log, "refused", "translation_unavailable", None, "miss", None);
        return SyncWorkflowOutput {
            response: httputil::error(StatusCode::SERVICE_UNAVAILABLE, "translation_unavailable"),
        };
    };

    // Which step of the sync ladder is about to run, named before `resync_url`/`ref_id` are moved
    // into the branch below — the final `serve` line's `reason` says which one SUCCEEDED; a failure
    // along the way is reported by `log_sync` at the point it happens.
    // `sparse_fallback` overrides whichever tier actually ran: the sync ladder still aligns the body
    // normally (a sparse track is still worth timing correctly), but the serve line should say WHY
    // this body was accepted at all, not just how it was synced.
    let tier_label = alignment.serve_reason(sparse_fallback);
    let started = Instant::now();
    let sync_timing = || format!("sync;dur={}", started.elapsed().as_millis());
    let tag = scratch_tag(&cache_key, SYNC_SEQ.fetch_add(1, Ordering::Relaxed));
    // A permit, held only around work that actually spawns a binary.
    //
    // The single-flight guard above collapses two requests for the SAME alignment; twenty different
    // ones — the picker hands back a URL per subtitle — are twenty keys and were twenty concurrent
    // subprocesses on a one-thread runtime. Acquired inside these branches rather than above the
    // dispatch: the semaphore is FIFO with two permits, so taking it on the no-sync path made a
    // plain cache-miss fetch queue behind every pending alignment for work it was never going to do.
    // `mine_only` says the failure belongs to this credential rather than to the work — see the two
    // markers above. Only the reference download can set it.
    let mut mine_only = false;
    let synced: Option<(String, Option<i64>)> = if let Some(url) = resync_url {
        // Tier 2 — partial audio VAD against the playing stream (opt-in).
        // ffmpeg is handed a loopback relay, never `url`: it would re-resolve the name and follow
        // redirects on its own (see `resync.rs`). The relay follows the redirect chain here, before
        // the permit, for the reason Tier 1 fetches its reference first — it is network work.
        let aligned = match resync::Relay::open(
            &url,
            &state.cfg.scout_origins,
            &state.cfg.scout_aliases,
            state.local_scout.get(),
        )
        .await
        {
            Ok(relay) => {
                let prepared = reservation.take().expect("Tier-2 sync was reserved before retaining bodies");
                let mut slot = state.sync_admission.acquire(prepared).await;
                let result = state.sync.sync_to_audio(&target, &relay.url(), resync_audio, &tag).await;
                slot.finish(sync_outcome(&result));
                result
            }
            Err(e) => Err(e),
        };
        match aligned {
            Ok(s) => Some((s.body, Some(s.shift_ms))),
            Err(e) => {
                if SYNC_FAILED.allow() {
                    eprintln!("sync: resync of {what} failed: {e}");
                    eprintln!("{}", log_sync(state, log, "tier2", &e, None));
                }
                None
            }
        }
    } else if let Some(r) = ref_id {
        // Tier 1 — reference-align against the hash-matched sub (no audio needed). The reference is
        // fetched BEFORE taking a permit: that is a network download, and a permit meant to bound
        // subprocesses should not be spent waiting on OpenSubtitles.
        // The reference's language is not known here; its encoding is detected unhinted.
        match subtitle_srt(state, client, r, None, false, None, false).await {
            Ok(reference) => {
                let aligned = {
                    let prepared =
                        reservation.take().expect("Tier-1 sync was reserved before retaining bodies");
                    let mut slot = state.sync_admission.acquire(prepared).await;
                    let result = state.sync.sync_to_reference(&target, &reference, &tag).await;
                    slot.finish(sync_outcome(&result));
                    result
                };
                match aligned {
                    Ok(s) => Some((s, None)),
                    Err(e) => {
                        if SYNC_FAILED.allow() {
                            eprintln!("sync: aligning {what} to {r} failed: {e}");
                            eprintln!("{}", log_sync(state, log, "tier1", &e, Some(r)));
                        }
                        None
                    }
                }
            }
            Err(e) => {
                // `Unavailable` is this credential's own trouble — quota gone, key revoked, its own
                // rate limit. `Gone` and `Suspect` are about the file, so they stay shared.
                mine_only = matches!(e, opensubtitles::DownloadError::Unavailable(_));
                let tag = e.reason_tag();
                if DOWNLOAD_FAILED.allow() {
                    eprintln!("sync: reference {r} for {what} unavailable: {}", e.message());
                    eprintln!("{}", log_sync(state, log, "tier1", &format!("reference_{tag}"), Some(r)));
                }
                None
            }
        }
    } else {
        None
    };

    // A sync that was ASKED FOR and did not happen is not the answer to this key — it is the
    // unaligned body standing in. Caching it for 60 days makes one broken afternoon permanent: the
    // URL is deterministic, so every later request is a cache hit and the sync never runs again.
    // Remember it briefly so a retry loop doesn't re-spawn the binary per request, and let it heal.
    let response = match synced {
        // The alignment happened: this body IS the answer to this key. `tier_label` says which step
        // of the ladder produced it.
        Some((body, offset_ms)) => {
            let offset_key = offset_ms.map(|_| subtitle_offset_key(&cache_key));
            state.cache.put(cache_key, body.clone(), SYNC_RESULT_TTL);
            if let (Some(offset_key), Some(offset_ms)) = (offset_key, offset_ms) {
                state.cache.put(offset_key, offset_ms.to_string(), SYNC_RESULT_TTL);
            }
            log_serve(state, log, "served", tier_label, None, "miss", Some(&body));
            let resp = with_subtitle_offset(httputil::srt(body), offset_ms);
            httputil::add_timing(resp, &sync_timing())
        }
        // Asked for and didn't happen. The unaligned body stands in, and is NOT written to
        // `cache_key` — caching it there made one broken afternoon permanent, since the URL is
        // deterministic and every later request became a hit that never retried. Only the "don't
        // re-spawn for a moment" marker is remembered; the unaligned body is already cached under
        // its own base key.
        None if wanted_sync => {
            let marker = match mine_only {
                true => mine_marker,
                false => shared_marker,
            };
            state.cache.put(marker, "1".into(), SYNC_RETRY_TTL);
            log_serve(state, log, "degraded", "sync_failed", None, "miss", Some(&target));
            let resp = httputil::add_timing(httputil::srt_provisional(target), &sync_timing());
            httputil::degraded(resp, "sync_failed")
        }
        // No sync was asked for, so the body we have is the answer, and it is already cached —
        // unless the caller could not determine whether an alignment was owed at all, in which case
        // this is a stand-in and must revalidate rather than being pinned for a year.
        None if settled => {
            log_serve(state, log, "served", tier_label, None, "miss", Some(&target));
            httputil::srt(target)
        }
        None => {
            log_serve(state, log, "degraded", "anchor_unknown", None, "miss", Some(&target));
            httputil::srt_provisional(target)
        }
    };
    SyncWorkflowOutput { response }
}

pub(super) fn retained_sync_target(
    state: &AppState,
    target: Option<String>,
    translated_body_key: Option<&str>,
) -> Option<String> {
    target.or_else(|| cached_translation(state, translated_body_key?).map(|body| body.srt))
}

pub(super) fn sync_outcome<T>(result: &Result<T, String>) -> SyncOutcome {
    match result {
        Ok(_) => SyncOutcome::Completed,
        Err(e) if e.ends_with(" timed out") => SyncOutcome::TimedOut,
        Err(_) => SyncOutcome::Failed,
    }
}

pub(super) const X_DEN_SUBTITLE_OFFSET_MS: &str = "x-den-subtitle-offset-ms";

pub(super) fn subtitle_offset_key(cache_key: &str) -> String {
    format!("subtitle-offset-ms:{cache_key}")
}

pub(super) fn with_subtitle_offset(mut resp: Response<Body>, offset_ms: Option<i64>) -> Response<Body> {
    if let Some(offset_ms) = offset_ms {
        if let Ok(value) = HeaderValue::from_str(&offset_ms.to_string()) {
            resp.headers_mut().insert(X_DEN_SUBTITLE_OFFSET_MS, value);
        }
    }
    resp
}
