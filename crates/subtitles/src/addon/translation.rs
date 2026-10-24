//! Translation workflow, paid-work accounting, refusal policy, and provider execution.

use super::*;

pub(super) fn cached_translation(state: &AppState, key: &str) -> Option<TranslatedBody> {
    state.cache.get(key).map(|raw| {
        // Existing plain-SRT entries remain usable, but their source is unknown. Align them when
        // an anchor exists instead of guessing from the mutable title pin or buying them again.
        serde_json::from_str(&raw).unwrap_or(TranslatedBody { source: None, srt: raw })
    })
}

/// The subtitle to translate FROM: English for preference, best-quality otherwise.
///
/// Judged as text, so the pick does not depend on which encode is playing — see
/// `opensubtitles::text_score` for why that matters to the bill.
///
/// The English preference is a bonus rather than a filter-then-fall-back, because "the best English
/// sub, else anything" picks a machine-translated English one over a good human Spanish one. Nothing
/// downstream can tell that it is translating a translation, and the errors compound instead of
/// cancelling.
pub(super) fn translation_source(subs: &[opensubtitles::Subtitle]) -> Option<&opensubtitles::Subtitle> {
    // Enough to prefer any English source over an equally good one in another language, and nowhere
    // near enough to rescue a machine-translated one: `text_score` sinks those by two million.
    const ENGLISH: i64 = 10_000;
    subs.iter().min_by_key(|s| {
        let mut score = opensubtitles::text_score(s);
        if s.lang.eq_ignore_ascii_case("en") {
            score += ENGLISH;
        }
        // Highest score wins, and `file_id` breaks a tie. Taking "the first of equal scores" was not
        // good enough: first means first IN THE LIST, and the list arrives in an order OpenSubtitles
        // chooses. An order-dependent pick moves the pin between two sources of one title, so each
        // move is another metered download — and the two encodes of a film a viewer switches
        // between would keep taking it off each other.
        (std::cmp::Reverse(score), s.file_id)
    })
}

/// Translations one install may START in a day.
///
/// A viewer watches a film, or an evening of episodes. This sits far above that and far below what
/// an install URL in the wrong hands could spend — and it is the viewer's own provider account that
/// gets spent, which is why the ceiling exists at all. Every other bound here caps ONE run:
/// `MAX_CUES` its size, `RUN_DEADLINE` its wall clock, the ten-minute marker its retries. None of
/// them bounds breadth — distinct titles times distinct languages, each one a fresh paid film.
pub(super) const DAILY_TRANSLATIONS: u64 = 50;
/// Two days, so a day's counter falls out of the store on its own rather than needing a sweep.
pub(super) const QUOTA_TTL: Duration = Duration::from_secs(2 * 24 * 60 * 60);

/// Key for one install's translation count today. The config segment is a bearer secret and this key
/// becomes a filename in the disk tier, so the segment is hashed rather than named.
pub(super) fn quota_key(config: &str, now: std::time::SystemTime) -> String {
    let day = now.duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs() / 86_400).unwrap_or(0);
    format!("quota:{}:{day}", short_hash(config))
}

/// Holds a charged allowance slot until the run it paid for finishes on its own terms.
///
/// It exists for the one ending that produces no `Err` to inspect: the client hanging up mid-run,
/// which drops the whole future. Disarmed as soon as the run returns, so on every ordinary path the
/// explicit decision at the call site is the only one made.
pub(super) struct ChargeGuard<'a> {
    pub(super) state: &'a Arc<AppState>,
    pub(super) config: &'a str,
    pub(super) spend: &'a translate::Spend,
    pub(super) source_was_cached: bool,
    pub(super) armed: bool,
}

impl Drop for ChargeGuard<'_> {
    fn drop(&mut self) {
        // DISPATCHED, not billed — the failure path's question is the wrong one here.
        //
        // A finished run has seen every answer, so it can ask whether the viewer was charged, and a
        // refusal that charged nothing honestly earns its slot back. A cancelled run has seen
        // nothing: the request it left on the wire is charged for whether or not we are still
        // listening. Asking `billed` read as "nothing was spent" for every cancellation before the
        // first response — and a real call may take LLM_TIMEOUT, so hanging up inside 100ms wins
        // that race every time. The slot came back on each attempt while each attempt still sent a
        // glossary call carrying the film's dialogue, so the daily ceiling never moved and the whole
        // guard was a way to spend without being counted.
        if self.armed && !self.spend.was_dispatched() && self.source_was_cached {
            refund_translation(self.state, self.config);
        }
    }
}

/// Give a slot back, for a run that was charged and then spent nothing at all.
///
/// "Nothing at all" is both resources, not just the model: a request that bought a metered
/// OpenSubtitles credit keeps its slot even though no token was sent, because the daily allowance is
/// what bounds how many distinct titles can spend credits. The caller decides — see the two
/// conditions at the one call site.
pub(super) fn refund_translation(state: &Arc<AppState>, config: &str) {
    let key = quota_key(config, std::time::SystemTime::now());
    let used: u64 = state.cache.get(&key).and_then(|v| v.parse().ok()).unwrap_or(0);
    state.cache.put(key, used.saturating_sub(1).to_string(), QUOTA_TTL);
}

/// "This install's provider credential was refused." Install-, provider- and model-scoped, because
/// that is the scope of the fact: the same key will refuse the next title identically.
///
/// Without it, a wrong or expired LLM key — the likeliest misconfiguration in a BYOK product, since
/// the key rides in the install URL — cost a metered subtitle download per title browsed and would
/// have drained the whole daily allowance, all for runs that sent no tokens at all. The
/// title-scoped marker cannot help: it is a different key for every film.
pub(super) fn credential_refused_key(config: &str, llm: &LlmConfig) -> String {
    format!("{SYNCFAIL}llm:{:016x}:{}:{}", short_hash(config), llm.provider.tag(), llm.model)
}

/// Which title last hit an AMBIGUOUS refusal on this credential — a 400 or a 403 that might be the
/// key and might be this film's dialogue tripping a content filter.
///
/// Two such refusals on two DIFFERENT titles is the thing that tells them apart: a content filter is
/// a fact about one film, a dead key is not. Guessing per provider got it wrong twice in two rounds
/// in both directions, so this measures it instead. One title alone never arms the install-wide
/// block, so a filtered series cannot take the rest of the library down with it.
pub(super) fn ambiguous_refusal_key(config: &str, llm: &LlmConfig) -> String {
    format!("{SYNCFAIL}llm-amb:{:016x}:{}:{}", short_hash(config), llm.provider.tag(), llm.model)
}

/// One film or one episode, as anything counting distinct titles has to count them.
///
/// `parse_id` splits `tt123:2:5` into three values and the first is the SERIES, so building this
/// from `imdb` alone makes every episode of a show one title — which is how the escalation below
/// came to be unable to fire across a season at all. A named function because that mistake was
/// invisible at the call site and, spelled inline, no test could catch its return.
pub(super) fn title_id(imdb: &str, season: Option<i64>, episode: Option<i64>) -> String {
    format!("{imdb}:{}:{}", season.unwrap_or(0), episode.unwrap_or(0))
}

/// "This series keeps being refused." Between the per-episode marker and the install-wide one.
pub(super) fn series_refused_key(config: &str, llm: &LlmConfig, imdb: &str) -> String {
    format!("{SYNCFAIL}llm-series:{:016x}:{}:{}:{imdb}", short_hash(config), llm.provider.tag(), llm.model)
}

/// How widely a credential-class refusal should be remembered.
///
/// Three scopes because there are three situations, and the middle one had no home. A dead key is
/// the install. A content filter is the SERIES — that is where it recurs, since the thing it objects
/// to is the show's dialogue — and giving it the install was collateral while giving it only the
/// episode was no throttle at all: every next episode paid a metered download, an allowance slot and
/// half a film in tokens before being refused, which is worse than the block it replaced.
pub(super) enum RefusalScope {
    Install,
    Series,
    TitleOnly,
}

pub(super) fn refusal_scope(
    state: &Arc<AppState>,
    config: &str,
    llm: &LlmConfig,
    title: &str,
    key_certain: bool,
    spent: bool,
) -> RefusalScope {
    if key_certain {
        return RefusalScope::Install;
    }
    // Billed work in this same run is proof the provider accepted this credential, so whatever it
    // has now objected to is the content. Never the install; the series is where it will recur.
    if spent {
        return RefusalScope::Series;
    }
    // Nothing billed and the status is ambiguous, so this rests on corroboration — and `spent` is a
    // weaker witness here than it looks. A content filter that objects to a show's dialogue objects
    // to the GLOSSARY sample too, which is drawn from that same dialogue, so the filtered case often
    // reaches here having billed nothing at all rather than arriving above.
    match ambiguous_refusal_escalates(state, config, llm, title) {
        Corroboration::Unrelated => RefusalScope::Install,
        Corroboration::SameSeries => RefusalScope::Series,
        Corroboration::None => RefusalScope::TitleOnly,
    }
}

/// Should an ambiguous refusal block the whole install? Only once a SECOND, different title has
/// refused the same way inside the window.
///
/// The title is the episode, not the series. Keyed on the series id, every episode of one show read
/// as the same title and the escalation could never fire — while each episode still resolved its own
/// source and spent its own metered download, so a dead key on a season was unbounded. That is the
/// same browsing pattern the ambiguity itself is modelled on, and it needs to cut both ways: one
/// film's dialogue must not block the library, and a key that fails across two episodes must.
///
/// The memory of the last refusal deliberately outlives the block it can arm. Sharing a lifetime
/// with it meant that every time the block lapsed the evidence had lapsed too, so re-arming took two
/// fresh titles and two more metered downloads, every window.
///
/// What the second title IS decides how far it reaches. Two episodes of one show corroborate
/// nothing about the key — a content filter objects to a show, and a season is the browsing pattern
/// that produces two refusals fastest — so they arm the series. It takes an unrelated title to
/// implicate the credential. Reading any second title as the install let a filtered series take the
/// library down ten minutes at a time, which is what the doc above says must not happen; reading a
/// season as one title, which the previous shape did, left it with no throttle at all.
pub(super) fn ambiguous_refusal_escalates(
    state: &Arc<AppState>,
    config: &str,
    llm: &LlmConfig,
    title: &str,
) -> Corroboration {
    let seen = ambiguous_refusal_key(config, llm);
    let verdict = match state.cache.get_mem(&seen) {
        // The same title repeating is still one film's dialogue, however many times it repeats.
        Some(prev) if prev == title => Corroboration::None,
        Some(prev) if series_of(&prev) == series_of(title) => Corroboration::SameSeries,
        Some(_) => Corroboration::Unrelated,
        None => Corroboration::None,
    };
    state.cache.put_mem(seen, title.to_string(), 2 * SYNC_RETRY_TTL);
    verdict
}

/// What a second ambiguous refusal corroborates.
pub(super) enum Corroboration {
    /// Nothing yet, or the same title again.
    None,
    /// Another episode of the show that refused last time.
    SameSeries,
    /// A different show or a film — the part that implicates the credential rather than a script.
    Unrelated,
}

/// The series half of a `title_id`. `imdb` ids carry no colon, so the first segment is the show.
pub(super) fn series_of(title: &str) -> &str {
    title.split(':').next().unwrap_or(title)
}

/// Is today's allowance already gone? A read, not a charge.
///
/// Consulted before the source download so a refusal is free. Charging only after the download —
/// which is right, since a source that will not fetch must not cost a slot — meant every request
/// past the fiftieth bought a metered OpenSubtitles credit and then returned 429. Nothing caps the
/// number of distinct titles a viewer browses, so that was unbounded in the dimension that matters,
/// and it emptied the download quota that the plain subtitle picker also depends on.
pub(super) fn allowance_used_up(state: &Arc<AppState>, config: &str) -> bool {
    let key = quota_key(config, std::time::SystemTime::now());
    let used: u64 = state.cache.get(&key).and_then(|v| v.parse().ok()).unwrap_or(0);
    used >= DAILY_TRANSLATIONS
}

/// Take one from today's allowance, or refuse. Charged only where a run actually BEGINS: a cache
/// hit, a resumed batch, and the `.srt` half of the app's own two-request flow are all free, because
/// none of them spends anything.
pub(super) fn charge_translation(state: &Arc<AppState>, config: &str) -> bool {
    let key = quota_key(config, std::time::SystemTime::now());
    let used: u64 = state.cache.get(&key).and_then(|v| v.parse().ok()).unwrap_or(0);
    if used >= DAILY_TRANSLATIONS {
        return false;
    }
    // Read-modify-write, and deliberately not atomic: two runs starting in the same instant can both
    // read `used` and both write `used + 1`, so the count can undershoot by the number of concurrent
    // starts. This is an abuse ceiling, not an accounting record — being a few under on a burst
    // costs nothing, and the alternative is holding a lock over the whole cache for a counter.
    state.cache.put(key, (used + 1).to_string(), QUOTA_TTL);
    true
}

/// The 429 for an install over today's allowance, saying when it comes back.
///
/// `quota_key` counts per UTC day, so the allowance returns at UTC midnight. `RateLimit-Policy` and
/// `RateLimit` are the IETF httpapi draft's names (draft-ietf-httpapi-ratelimit-headers): the policy
/// is the quota `q` over a window `w` of seconds, the state what `r`emains and the seconds `t` until
/// it resets. A draft, so `Retry-After` is the header a client can be counted on to read.
pub(super) fn allowance_refusal() -> Response<Body> {
    let reset = ratelimit::until_utc_midnight(std::time::SystemTime::now());
    let refused = httputil::error(StatusCode::TOO_MANY_REQUESTS, "allowance_exhausted");
    let mut resp = httputil::retry_after(refused, reset);
    let policy = format!("\"daily\";q={DAILY_TRANSLATIONS};w=86400");
    let state = format!("\"daily\";r=0;t={}", reset.as_secs());
    resp.headers_mut().insert("ratelimit-policy", HeaderValue::from_str(&policy).unwrap());
    resp.headers_mut().insert("ratelimit", HeaderValue::from_str(&state).unwrap());
    resp
}

/// A stable digest for values that go into keys which outlive the process — the daily allowance, the
/// per-install failure marker, the resync variant of a cache key. Deliberately not `DefaultHasher`:
/// std may change that algorithm between compiler releases, and a toolchain bump would then reset
/// every allowance and move every marker at once. See `httputil::stable_hash`.
pub(super) fn short_hash(s: &str) -> u64 {
    httputil::stable_hash(s.as_bytes())
}

/// GET /<config>/translate/<type>/<id>[/<extra>]/<lang>.(json|srt) — the app-driven translation
/// Complete translation request after the route adapter has parsed endpoint-specific arguments.
pub(super) struct TranslationWorkflowInput<'a> {
    pub(super) den: Option<&'a den_seal::Addon>,
    pub(super) headers: &'a HeaderMap,
    pub(super) config: &'a str,
    pub(super) id: &'a str,
    pub(super) extra: &'a str,
    pub(super) lang: &'a str,
    pub(super) want_json: bool,
    pub(super) resync_url: Option<String>,
    pub(super) resync_key: Option<String>,
    pub(super) resync_audio: Option<crate::sync::AudioStream>,
}

pub(super) async fn run_translation_workflow<'a>(
    state: &Arc<AppState>,
    input: TranslationWorkflowInput<'a>,
) -> Response<Body> {
    let TranslationWorkflowInput {
        den,
        headers,
        config,
        id,
        extra,
        lang,
        want_json,
        resync_url,
        resync_key,
        resync_audio,
    } = input;
    let Some(cfg) = state.decode_config(den, config) else {
        return httputil::error(StatusCode::BAD_REQUEST, "bad_config");
    };
    // Translation needs the (optional) LLM credential — a subtitles-only install has none.
    let Some(llm) = &cfg.llm else {
        return httputil::error(StatusCode::BAD_REQUEST, "no_llm");
    };
    let Some((imdb, season, episode)) = parse_id(id) else {
        return httputil::error(StatusCode::BAD_REQUEST, "bad_id");
    };
    // The path segment arrives percent-encoded ("Brazilian%20Portuguese"), and nothing decoded it.
    // So the escape sequence went into the cache key AND was interpolated into the prompt of every
    // batch — the model was asked to translate into "Brazilian%20Portuguese", and the encoded and
    // decoded spellings of one language bought the film twice.
    let lang_seg = lang;
    let lang = httputil::percent_decode_path(lang_seg);
    // A language name, not an essay. This lands in a cache key that becomes a filename — the same
    // hazard `search_hash` already bounds `videoHash` against — and it is interpolated into the
    // prompt of every batch, so its length multiplies the bill against the viewer's own key.
    if lang.is_empty() || lang.len() > MAX_LANG {
        return httputil::error(StatusCode::BAD_REQUEST, "bad_lang");
    }
    // Case and spacing are not different languages, but they were different cache keys, and a key
    // here is a whole film's LLM bill. Canonicalized separately from the name we send the model: the
    // prompt reads better with the viewer's own spelling, and the key only has to be stable.
    let lang_key = translate::canonical_lang(&lang);
    // All-whitespace survives the length check and normalizes to nothing, which would file every such
    // request under one shared key.
    if lang_key.is_empty() {
        return httputil::error(StatusCode::BAD_REQUEST, "bad_lang");
    }
    let rid = logging::request_id(headers);
    // Built once and reused at every exit: `file_id` is the only field any one call site has to
    // supply, since which file this title translates from is not known until a source is picked.
    let make_ctx = |file_id: Option<i64>| ServeCtx {
        rid: rid.as_deref(),
        file_id,
        imdb: Some(&imdb),
        season,
        episode,
        lang: Some(&lang),
    };
    // Checked before any network call, and scoped to the title rather than to the source file: a
    // whole film's LLM bill is not something to re-pay on every tap. The `.json` and `.srt` forms are
    // two requests, so the app's own flow retries once by design — and nothing was remembered about a
    // failure, so each attempt paid in full again, with no backoff. Same marker the sync path uses,
    // in the same namespace.
    //
    // Said differently from a fresh failure on purpose: from the outside the two are the same 502,
    // and the difference — "we just tried" versus "we are backing off" — is the first thing you want
    // to know when a translation stops working. `Retry-After` is the backoff left, so a client that
    // reads it comes back when a retry can run rather than polling into the same answer.
    let job_key = translate_fail_key(config, &imdb, season, episode, &lang_key, llm);
    let failed_recently = format!("{SYNCFAIL}{job_key}");
    if let Some(left) = state.cache.ttl(&failed_recently) {
        return httputil::retry_after(httputil::error(StatusCode::BAD_GATEWAY, "translation_backoff"), left);
    }

    let Some(http) = state.http.as_ref() else {
        return httputil::error(StatusCode::SERVICE_UNAVAILABLE, "service_unavailable");
    };
    let client = os_client(state, http, &cfg);

    let hash = search_hash(extra);

    // Which file this title translates FROM is decided once and then pinned.
    //
    // `translation_source` reads download counts, ratings and the trusted flag, and all three drift —
    // one new trusted upload is +400 and flips the pick outright — while the search behind it is only
    // cached for six hours. The pin keeps every language of a film reading from one file, which is
    // one download rather than one per language, and keeps the choice stable rather than following
    // whatever the list looked like that afternoon.
    //
    // It is no longer load-bearing for the LLM bill: the body is keyed by title, so losing the pin
    // costs a re-pick and a download, never a re-translation.
    //
    // READ here, but RESOLVED only if a translation actually has to be produced. Since the body is
    // keyed by title, nothing on the serving path needs a source id except the decision of whether to
    // align — so a pin miss no longer drags a live search, fifty dead-file probes and a blocking pin
    // write onto a request whose translation is already bought and cached, and can no longer refuse
    // one with a 502 when that search fails or a 404 when every candidate is filtered out.
    let pin_key = source_pin_key(&imdb, season, episode);
    let pinned = state.cache.get(&pin_key).and_then(|v| v.parse::<i64>().ok());
    let body_key = translate_body_key(&imdb, season, episode, &lang_key, llm);
    let cached_body = cached_translation(state, &body_key);

    // The hashed list is only worth asking for when there is a hash AND auto-sync is on: without
    // either there is no anchor to find, and the answer would be the list we already have.
    // A failed search here means no anchor, and no anchor means carry on without one: `ref_id`
    // becomes `None`, `sync_cache_key` keys on `body_key` itself, and that IS the honest identity of
    // an unaligned body — not a key an aligned one should have owned. Nothing is mis-filed, and a
    // later request that does find an anchor misses the versioned reference key and aligns then.
    //
    // Two stricter versions of this were both worse. Refusing outright returned 502 for a title
    // already bought and cached; returning early with whatever happened to be cached could not
    // translate a cold title at all during a blip. Neither wrote a backoff, so every client retry
    // was another live search.
    // `anchor_unknown` is the difference between "there is no anchor" and "we could not find out
    // whether there is one". Both produce `ref_id = None` and both key on `body_key`, which is the
    // honest identity of an unaligned body — but only the first is SETTLED. Serving the second as
    // `immutable` pins the client to a mis-timed track for a year at a deterministic URL, which is
    // the failure `srt_provisional` exists for, moved from the server cache to the client's.
    let mut anchor_unknown = false;
    let anchored = match hash.as_deref().filter(|_| cfg.auto_sync) {
        Some(h) => match cached_search(state, &client, config, &imdb, season, episode, Some(h)).await {
            Ok((subs, _)) => subs,
            Err(_) => {
                anchor_unknown = true;
                Vec::new()
            }
        },
        None => Vec::new(),
    };
    // Which alignment a given source needs. A source that is itself hash-matched to this encode is
    // already correctly timed, so the translated body carrying its timings is too.
    //
    // Taken as a function because it has to be answered twice. Before a source is known — a pin
    // miss — the honest answer is "assume not aligned", which is what lets the cached-body probe
    // below happen without resolving a source at all. But answering it that way and then STOPPING
    // there bought real work for nothing: `ref_id` became `Some(R)`, and `run_alignment_workflow` fetches
    // that reference through the one call in the program that spends a metered download credit,
    // then runs a tier binary. When the source turned out to be hash-matched after all, both were
    // no-ops, and the entry they produced was filed under a key the very next request — now pinned,
    // now answering `true` — never asks for again.
    let align_for = |source: Option<i64>| -> Option<i64> {
        let in_sync = source.is_some_and(|id| anchored.iter().any(|s| s.file_id == id && s.hash_match));
        match in_sync {
            true => None,
            false => tier1_reference(&anchored).filter(|&r| Some(r) != source),
        }
    };

    // Tier 2 is a user action against the track itself, so it is only read on the `.srt` form — the
    // `.json` form's job is to warm and hand back a URL. Vetted before it can reach a cache key, for
    // the reason `handle_subtitle_file` gives.
    let resync_url = resync_url.filter(|_| !want_json).and_then(|u| match resync::vet(&u, &state.cfg.scout_origins) {
        Some(url) => Some(url.to_string()),
        // Never the URL itself: a stream target carries the provider's token.
        None => {
            if UNSAFE_RESYNC.allow() {
                eprintln!("translate: refusing resync target for {imdb} (not scout /play at a SCOUT_ORIGINS origin)");
            }
            None
        }
    });
    let install_scope = cfg.iid.as_deref().unwrap_or(config);
    let resync_cache_identity = resync_cache_identity(install_scope, &resync_url, resync_key, resync_audio);
    // The translated body inherits its source's timing, so it needs the same Tier-1 correction the
    // source itself would get from the picker.
    let ref_id = align_for(cached_body.as_ref().and_then(|body| body.source));
    let cache_key = sync_cache_key(&body_key, &resync_cache_identity, ref_id);

    // Read once, not twice. Asking again on the settled path could miss what the first read saw —
    // LRU eviction and TTL expiry both happen between two reads — and answer "not translated" for a
    // translation that exists and had just been confirmed.
    // The `Server-Timing` phase for the translated text: `cache;desc=hit`, or how long the run took.
    let settled = if cache_key == body_key {
        cached_body.as_ref().map(|body| cache::Text::new(body.srt.clone()))
    } else {
        state.cache.get_shared(&cache_key)
    };
    let phase = if let Some(settled) = settled {
        if !want_json {
            let ctx = make_ctx(cached_body.as_ref().and_then(|b| b.source));
            // `immutable` only when this really is the answer. With the anchor merely unknown the
            // body may well be superseded within the thirty seconds the search marker lasts, and a
            // client told `immutable` will not come back for a year.
            let outcome = if anchor_unknown { "degraded" } else { "served" };
            let reason = if anchor_unknown { "anchor_unknown" } else { "cache_hit" };
            log_serve_cached(state, ctx, outcome, reason, &settled);
            let resp = match anchor_unknown {
                true => httputil::degraded(
                    httputil::srt_provisional_shared(settled.to_bytes(), settled.etag()),
                    "upstream_unavailable",
                ),
                false => httputil::srt_shared(settled.to_bytes(), settled.etag()),
            };
            let resp = with_cached_subtitle_offset(state, &cache_key, resp);
            return httputil::add_timing(resp, CACHE_HIT);
        }
        CACHE_HIT.to_string()
    } else {
        // The expensive half: the translated text. `used_source` carries back which file it came
        // from, so the alignment decision can be re-answered from a source that is actually known.
        let mut used_source = cached_body.as_ref().and_then(|body| body.source);
        let mut phase = CACHE_HIT.to_string();
        let translated = match cached_body {
            Some(body) => body.srt,
            None => {
                // One film, one bill. Two devices on the same title — or a second tap during a run
                // that legitimately takes minutes — each used to start their own full translation,
                // because a cache only collapses work that has already finished.
                let _flight = state.inflight.acquire(&body_key).await;
                // The marker was read at the top of this handler, before the wait. Everything queued
                // behind a run that then FAILED would sail past that stale read and each start its
                // own full-price translation, with no backoff — N waiters, N films, on the viewer's
                // own key. The marker exists to stop exactly that, so it is read again now that we
                // know how the wait ended. `run_alignment_workflow` re-checks its marker for this reason.
                if let Some(left) = state.cache.ttl(&failed_recently) {
                    return httputil::retry_after(
                        httputil::error(StatusCode::BAD_GATEWAY, "translation_backoff"),
                        left,
                    );
                }
                match cached_translation(state, &body_key) {
                    // Produced while we waited. This is the branch the whole guard exists for.
                    Some(body) => {
                        used_source = body.source;
                        body.srt
                    }
                    // Only here, with every cache read behind us, is a source actually needed — so
                    // only here is one resolved. A pin miss costs a live search, a probe per
                    // candidate and a blocking pin write, and none of that belongs on a request whose
                    // translation already exists.
                    None => {
                        // Refuse before resolving a source, not just before downloading one.
                        // Resolution is two live searches, a blocking disk probe per candidate — the
                        // list is a page of fifty — and a blocking pin write, all for a request that
                        // is about to be refused, on the one thread serving every connection. The
                        // charge itself still happens after the download, so a source that will not
                        // fetch continues to cost no slot.
                        if allowance_used_up(state, config) {
                            if ALLOWANCE_REFUSED.allow() {
                                eprintln!(
                                    "translate: {imdb} → {lang} refused, install is over its daily allowance"
                                );
                                eprintln!(
                                    "{}",
                                    log_translate(
                                        state,
                                        make_ctx(None),
                                        "refused",
                                        "allowance_exhausted",
                                        None
                                    )
                                );
                            }
                            return allowance_refusal();
                        }
                        // A refused credential is about the install, not this title, so the
                        // title-scoped marker cannot catch it — every film is a fresh key.
                        //
                        // But it belongs HERE, beside the allowance check and behind every cache
                        // read, for the same reason that one does. Checked at the top of the handler
                        // it refused translations that were already bought and cached: serving one
                        // makes no provider call at all, so a dead key was making the install's
                        // entire existing library unavailable, re-armed by every new title browsed.
                        if state.cache.get_mem(&credential_refused_key(config, llm)).is_some() {
                            return httputil::error(StatusCode::BAD_GATEWAY, "llm_key_refused");
                        }
                        // And the series-scoped one, for the refusal that billed work before it
                        // arrived — a content filter, not a credential. Same placement and the same
                        // reason: behind the cache reads, so episodes already bought still serve.
                        if state.cache.get_mem(&series_refused_key(config, llm, &imdb)).is_some() {
                            return httputil::error(StatusCode::BAD_GATEWAY, "llm_title_refused");
                        }
                        let source_id = match pinned {
                            Some(id) => id,
                            None => {
                                // Unhashed deliberately. OpenSubtitles floats hash matches up and
                                // returns one page, so a hashed list is ordered and truncated
                                // differently per encode — choosing from it resolves two encodes of
                                // one film to two sources, so the pin flips whenever the viewer
                                // switches encode and each flip is another metered download.
                                //
                                // A failed search does NOT set the failure marker: a search costs
                                // nothing, and marking it made a blip outlive itself by ten minutes
                                // across every language the viewer tried.
                                let Ok((candidates, _)) =
                                    cached_search(state, &client, config, &imdb, season, episode, None).await
                                else {
                                    return httputil::error(StatusCode::BAD_GATEWAY, "translation_failed");
                                };
                                // Skip what is already known not to download, or the loop has no
                                // exit: a dead source is unpinned, the deterministic re-pick chooses
                                // the same file, pins it again, and refuses again. Same for one too
                                // big to translate — it downloads, so it is a fine subtitle to serve
                                // as-is, but as a source it fails identically every time.
                                let usable: Vec<opensubtitles::Subtitle> = candidates
                                    .into_iter()
                                    .filter(|s| state.cache.get(&dead_file_key(s.file_id)).is_none())
                                    .filter(|s| state.cache.get(&oversized_file_key(s.file_id)).is_none())
                                    .collect();
                                let Some(source) = translation_source(&usable) else {
                                    return httputil::error(StatusCode::NOT_FOUND, "no_source");
                                };
                                state.cache.put(pin_key.clone(), source.file_id.to_string(), SOURCE_PIN_TTL);
                                source.file_id
                            }
                        };
                        used_source = Some(source_id);
                        // A source already known to be failing must not cost an allowance slot to
                        // rediscover that, and one confirmed dead must not stay pinned — otherwise
                        // the ten-minute marker lapses, the same source is tried again, and the title
                        // burns one of the fifty daily translations per attempt.
                        if let Some(e) = remembered_failure(state, &client, source_id, false) {
                            let tag = e.reason_tag();
                            // `Gone` means the API named the id, or two separate occasions confirmed
                            // it. A single `Suspect` is not `Gone`, so a transient cannot unpin.
                            if matches!(e, opensubtitles::DownloadError::Gone(_)) {
                                eprintln!(
                                    "translate: unpinning {imdb} — source {source_id} will not download"
                                );
                                state.cache.remove(&pin_key);
                            }
                            // Backed off like any other failure, or the refusal is free to repeat and
                            // each repeat re-runs the search and the pin write.
                            state.cache.put(failed_recently, "1".into(), SYNC_RETRY_TTL);
                            eprintln!(
                                "{}",
                                log_translate(
                                    state,
                                    make_ctx(Some(source_id)),
                                    "refused",
                                    &format!("source_{tag}"),
                                    Some("opensubtitles"),
                                )
                            );
                            return httputil::error(StatusCode::BAD_GATEWAY, "source_unavailable");
                        }
                        let started = Instant::now();
                        match produce_translation(
                            state, &client, llm, config, source_id, &lang, &body_key, &job_key,
                        )
                        .await
                        {
                            Ok(body) => {
                                phase = format!("translate;dur={}", started.elapsed().as_millis());
                                // Refresh the pin on the path that actually produced a translation, so
                                // its lifetime and mtime do not stay frozen at the first one. Losing it
                                // is no longer expensive — the body is keyed by title, so a re-pick
                                // cannot orphan anything paid for — but a stable pin still keeps every
                                // language of a film reading from one source, which is one download
                                // rather than several. One blocking write per real translation, not per
                                // request, which is what the per-request version cost.
                                state.cache.put(pin_key.clone(), source_id.to_string(), SOURCE_PIN_TTL);
                                body
                            }
                            // Nothing was spent — the allowance was checked at the last moment before
                            // the first token, so this costs neither a slot nor a backoff marker.
                            Err(TranslationFailure::Allowance) => {
                                if ALLOWANCE_REFUSED.allow() {
                                    eprintln!(
                                        "translate: {imdb} → {lang} refused, install is over its daily allowance"
                                    );
                                    eprintln!(
                                        "{}",
                                        log_translate(
                                            state,
                                            make_ctx(Some(source_id)),
                                            "refused",
                                            "allowance_exhausted",
                                            None,
                                        )
                                    );
                                }
                                return allowance_refusal();
                            }
                            // The provider refused the key itself. Remembered install-wide rather than
                            // per title, because that is the scope of the fact — and no per-title
                            // marker, since the title is innocent and will work once the key does.
                            Err(TranslationFailure::Credential { message, key_certain, spent }) => {
                                if CREDENTIAL_REFUSED.allow() {
                                    eprintln!(
                                        "translate: provider refused this install's credential: {message}"
                                    );
                                    eprintln!(
                                        "{}",
                                        log_translate(
                                            state,
                                            make_ctx(Some(source_id)),
                                            "refused",
                                            &format!("credential_refused:{message}"),
                                            Some("translator"),
                                        )
                                    );
                                }
                                // The install-wide block only when the status can ONLY mean the key.
                                // A 400 or a 403 might be this film's dialogue tripping a content
                                // filter, and blocking the install on that lets one series take every
                                // other title down with it, ten minutes at a time, as the viewer works
                                // through the episodes. The per-title marker below covers that case.
                                // Certain: block the install at once. Ambiguous: block it only once a
                                // SECOND title has refused the same way, which is what separates a dead
                                // key from one film's dialogue upsetting a content filter.
                                // A refusal that arrives AFTER a billed batch cannot be about the key:
                                // the provider accepted this credential minutes ago, in this run. That
                                // is a measurement, where `key_certain` is a guess and the two-title
                                // heuristic is a proxy — so it settles the ambiguous case outright.
                                //
                                // It settles it as the SERIES, not as nothing. Demoting it to the
                                // per-title marker alone left a moderation-filtered show with no
                                // throttle at all: every next episode paid a metered download, an
                                // allowance slot and half a film in tokens to rediscover the same
                                // refusal — dearer than the install-wide block it was meant to spare.
                                match refusal_scope(
                                    state,
                                    config,
                                    llm,
                                    &title_id(&imdb, season, episode),
                                    key_certain,
                                    spent,
                                ) {
                                    RefusalScope::Install => state.cache.put_mem(
                                        credential_refused_key(config, llm),
                                        "1".into(),
                                        SYNC_RETRY_TTL,
                                    ),
                                    RefusalScope::Series => state.cache.put_mem(
                                        series_refused_key(config, llm, &imdb),
                                        "1".into(),
                                        SYNC_RETRY_TTL,
                                    ),
                                    RefusalScope::TitleOnly => {}
                                }
                                // The per-title marker too, even though this is meant to be a fact about
                                // the install. `is_credential_refusal` reads a 400/403 as one, and those
                                // are not exclusively about credentials — OpenRouter answers 403 when a
                                // model's moderation trips, and this file already notes that film
                                // dialogue trips content filters routinely. Without a per-title marker
                                // such a title re-arms the install-wide one every ten minutes forever,
                                // and takes every other title down with it each time.
                                state.cache.put(failed_recently, "1".into(), SYNC_RETRY_TTL);
                                return httputil::error(StatusCode::BAD_GATEWAY, "llm_key_refused");
                            }
                            Err(e) => {
                                // Log the detail (no key in these strings); hand the client a generic
                                // message rather than echoing a raw upstream error body.
                                if TRANSLATE_FAILED.allow() {
                                    eprintln!("translate: {imdb} → {lang} failed: {}", e.message());
                                    eprintln!(
                                        "{}",
                                        log_translate(
                                            state,
                                            make_ctx(Some(source_id)),
                                            "failed",
                                            &format!("translate_failed:{}", e.message()),
                                            Some("translator"),
                                        )
                                    );
                                }
                                state.cache.put(failed_recently, "1".into(), SYNC_RETRY_TTL);
                                // Unpin ONLY when the source is what failed. A pin naming a file that
                                // holds no cues would otherwise be honoured forever — the marker expires
                                // after ten minutes, the same pin is read, the same failure follows, and
                                // the title is stuck for every install and language, since the pin is
                                // scoped to neither.
                                //
                                // For everything else the pin is innocent, and dropping it spends a
                                // metered download to learn nothing: a provider timeout or a rate limit
                                // says nothing about the file, so the re-pick lands on the same title
                                // needing the same source fetched again.
                                if matches!(e, TranslationFailure::Source(_)) {
                                    state.cache.remove(&pin_key);
                                }
                                return httputil::error(StatusCode::BAD_GATEWAY, "translation_failed");
                            }
                        }
                    }
                }
            }
        };
        // Re-answered from the source we actually used. The version above was computed before a
        // source was known, so it assumed one was needed; if the source turns out to be hash-matched
        // to this encode, that assumption would spend a metered download on the reference and a tier
        // binary on an alignment that is a no-op by construction — and file the result under a key
        // the next request, now pinned, never asks for.
        let ref_id = align_for(used_source);
        let cache_key = sync_cache_key(&body_key, &resync_cache_identity, ref_id);
        // Do not wait for sync admission while retaining the translated film. Every translated body
        // is durable under `body_key` before it reaches here, so release this request's copy, reserve
        // the appropriate tier, and reload only after it is inside the bounded retention set.
        let wanted_sync = resync_url.is_some() || ref_id.is_some();
        let translated = match wanted_sync {
            true => {
                drop(translated);
                None
            }
            false => Some(translated),
        };
        // The cheap half. Run here even for the `.json` form so the engine's follow-up fetch is a
        // cache hit rather than an aligner spawn with the viewer waiting on it.
        let what = format!("translation of {imdb} → {lang_key}");
        let resp = run_alignment_workflow(
            state,
            &client,
            SyncWorkflowInput {
                cache_key,
                translated_body_key: wanted_sync.then_some(body_key.as_str()),
                target: translated,
                ref_id,
                resync_url,
                resync_audio,
                reservation: None,
                flight: None,
                what: &what,
                // Settled only if we actually know whether an anchor exists. If the hashed search failed
                // we are serving an unaligned body that a working search might have aligned, so the
                // client has to come back rather than caching it for a year.
                settled: !anchor_unknown,
                log: make_ctx(used_source),
                sparse_fallback: false,
            },
        )
        .await
        .response;
        let resp = httputil::add_timing(resp, &phase);
        if !want_json {
            // Served unaligned because the anchor search failed, not because there is no anchor.
            return match anchor_unknown {
                true => httputil::degraded(resp, "upstream_unavailable"),
                false => resp,
            };
        }
        phase
    };

    httputil::add_timing(
        translate_url_response(state, den, headers, config, season, id, extra, lang_seg),
        &phase,
    )
}

/// Failure classes that decide cache/backoff, pin retention, and allowance settlement.
pub(super) enum TranslationFailure {
    Source(String),
    Model(String),
    /// The install has used up today's translations. Distinct because it is the one failure the
    /// client should see as "not now" rather than "something broke".
    Allowance,
    /// The provider refused the credential itself. Distinct because a refusal that really is about
    /// the key will happen identically for the next title, so it can earn a backoff wider than the
    /// one title — see `refusal_scope`, which decides how much wider from the two fields below.
    ///
    /// It is NOT a free failure, and the two fields are there because it used to be treated as one.
    /// The per-title marker is written as well as whatever wider scope is armed, and the allowance
    /// slot comes back only when this request spent nothing at all — neither tokens nor a metered
    /// download.
    ///
    /// `key_certain` says whether the status can only be about the credential. A 401 or an empty
    /// balance can; a 400 or a 403 might instead be this film's dialogue tripping a content filter,
    /// and blocking a whole install on one of those lets a series take every other title down.
    ///
    /// `spent` is the strongest signal of the three and is a measurement rather than a guess: if a
    /// batch was billed before the refusal, the provider accepted this credential in this very run,
    /// so whatever it has now objected to is about the CONTENT, not the key.
    Credential {
        message: String,
        key_certain: bool,
        spent: bool,
    },
}

/// Carry a download failure's own verdict through to the pin.
///
/// Only `Gone` — the API's own 404/410 on the id, or a `Suspect` that has now failed twice and been
/// promoted — is allowed to drop a pin shared by every install and every language. A single
/// `Suspect` is not: an expired one-shot CDN link and an interstitial served as a 200 both look like
/// that, both clear on their own, and unpinning on one re-picks against a drifted candidate list and
/// spends another metered credit on a source that was fine.
pub(super) fn classify_download(e: opensubtitles::DownloadError) -> TranslationFailure {
    match e {
        // The API says the id does not exist, or a repeat has confirmed the file will not download.
        opensubtitles::DownloadError::Gone(m) => TranslationFailure::Source(m),
        // One failed link fetch, or quota, a revoked key, transport. The pin is innocent and must
        // survive all of them: blaming it here would spend a download credit re-picking a source,
        // because the viewer ran out of download credits for the day.
        opensubtitles::DownloadError::Suspect(m) | opensubtitles::DownloadError::Unavailable(m) => {
            TranslationFailure::Model(m)
        }
    }
}

impl TranslationFailure {
    fn message(&self) -> &str {
        match self {
            TranslationFailure::Source(m)
            | TranslationFailure::Model(m)
            | TranslationFailure::Credential { message: m, .. } => m.as_str(),
            TranslationFailure::Allowance => "daily translation allowance used up",
        }
    }
}

/// Translate one source subtitle into `lang` and cache the result under `body_key`. Returns the
/// translated SRT — the caller then runs the sync ladder over it.
#[allow(clippy::too_many_arguments)]
pub(super) async fn produce_translation(
    state: &Arc<AppState>,
    client: &opensubtitles::Client<'_>,
    llm: &LlmConfig,
    config: &str,
    source_file_id: i64,
    lang: &str,
    body_key: &str,
    job_key: &str,
) -> Result<String, TranslationFailure> {
    // Through the cache, not straight at the API. A `/download` call spends one of the viewer's
    // daily OpenSubtitles credits on the CALL, not on the file fetch — and dodging that quota is the
    // reason the proxy-and-cache design exists at all. Going direct re-paid for a file already
    // sitting under `os:{file_id}`: once per retry after the ten-minute backoff, and once more for
    // every additional target language of the same film.
    //
    // Refuse for free BEFORE spending a credit on the source. The charge itself is further down, on
    // purpose — a source that will not fetch must not cost a slot — but with only that check, every
    // request past the fiftieth bought a metered download and then returned 429. Nothing caps how
    // many distinct titles a viewer browses, so that was unbounded, and it drained the same quota
    // the plain subtitle picker spends.
    if allowance_used_up(state, config) {
        return Err(TranslationFailure::Allowance);
    }
    // Whether this request is about to spend a metered OpenSubtitles credit, asked BEFORE the fetch
    // because afterwards the answer is always "it is cached now". Read pessimistically: a source
    // already on disk costs nothing, and anything else is assumed to have cost a credit, which is
    // the safe direction for a quota whose free tier is a handful a day.
    let source_was_cached = state.cache.get(&os_base_key(source_file_id)).is_some();
    // A download failure is NOT charged to the source. An exhausted daily credit and a dead upload
    // look identical from here, and on the free tier the first is an ordinary evening — so unpinning
    // on it would re-pick the source and owe another credit for the replacement, because the viewer
    // ran out of credits.
    let raw = subtitle_srt(state, client, source_file_id, None, false, None, false)
        .await
        .map_err(classify_download)?;
    let cues = srt::parse(&raw);
    if cues.is_empty() {
        // Unreachable in practice — `download` gates on `has_a_cue`, and a differential test pins
        // that gate to agree with `parse` in both directions. Kept because "it parsed to nothing"
        // is a statement about the file either way, and the classification should not depend on
        // which of two agreeing checks happened to run first.
        return Err(TranslationFailure::Source("source subtitle was empty".into()));
    }
    // The size gate, before the charge rather than inside `translate`. It is a refusal, not a run:
    // `translate` returns on it without making a single call, so charging first spent a slot on a
    // film that sent nothing — and the per-title marker throttles that to six an hour rather than
    // stopping it, so anything that kept asking ate the day's allowance. The cue count is known
    // three lines above; this is the same argument the download already gets.
    //
    // `Source`, not `Model`, and both halves of it. Size is a fact about the file that no retry and
    // no other install will change, and `Model` keeps the pin — so an oversized source pinned itself
    // in front of a title for the pin's full 180 days, for every install and every language, while
    // the perfectly good smaller candidates behind it were never reachable. `Source` drops the pin.
    //
    // Dropping the pin is only half an answer, because the re-pick is deterministic and would choose
    // this same file again. So the file is marked as well, which is what actually retires it, and the
    // marker is separate from `dead_file_key`: this file downloads fine and remains a good subtitle
    // to hand over as-is. It is unusable only as something to translate.
    let dialogue: usize = cues.iter().map(|c| c.text.len()).sum();
    let too_big = match (cues.len() > translate::MAX_CUES, dialogue > translate::MAX_DIALOGUE_BYTES) {
        (true, _) => Some(format!("subtitle too large: {} cues (max {})", cues.len(), translate::MAX_CUES)),
        (_, true) => Some(format!(
            "subtitle too large: {dialogue} bytes of dialogue (max {})",
            translate::MAX_DIALOGUE_BYTES
        )),
        _ => None,
    };
    if let Some(why) = too_big {
        state.cache.put(oversized_file_key(source_file_id), "1".into(), SOURCE_PIN_TTL);
        return Err(TranslationFailure::Source(why));
    }
    // Charged HERE — the last point before the first token is spent, and after everything that can
    // fail without spending one.
    //
    // Charged before the download, it took a slot for every attempt whose source would not fetch,
    // having sent nothing. And the guards around it are per file and per title-language, so distinct
    // titles are not gated against each other: an evening browsing ten episodes in two languages
    // burned twenty slots in under a minute, and about twenty-five titles emptied the day — turning
    // an OpenSubtitles quota that resets at midnight into a self-inflicted day-long lockout of the
    // paid feature.
    if !charge_translation(state, config) {
        return Err(TranslationFailure::Allowance);
    }
    // Declared BEFORE the guard below, so it is still alive when the guard's `drop` reads it. The
    // borrow makes that ordering a compile error to get wrong, not a convention.
    let spend = translate::Spend::default();
    // The charge, made refundable however this frame ends — including the way that produces no
    // `Err` at all. A client that hangs up mid-run drops this future, so `map_err` below never runs
    // and the slot it charged was neither refunded nor spent on anything a retry can use. The retry
    // then charged a second slot, and with a client timeout shorter than a run (up to RUN_DEADLINE)
    // a handful of films emptied the day — the same self-inflicted lockout the comment above
    // describes, reached by a different road.
    //
    // The guard gives the slot back only when this run put NOTHING on the wire — see its `Drop`,
    // which explains why that is a different question from the one the failure path asks, and why
    // asking the failure path's question here handed a cancelling client free runs.
    let mut slot = ChargeGuard { state, config, spend: &spend, source_was_cached, armed: true };
    // The cache doubles as the batch store: a run that dies at cue 1100 of 1200 leaves the 1100
    // behind, so the retry the viewer is about to make re-buys only what actually failed.
    //
    // Progress is published under the JOB key rather than the body key, so `.status` can answer a
    // poll from the request alone — deriving the body key needs a search, and a poll happens every
    // second while the expensive thing runs.
    let reporter = state.progress.start(job_key, cues.len());
    let translated = translate::translate(
        client.http,
        llm,
        &cues,
        lang,
        &state.cache,
        &spend,
        &|done, total| reporter.set(done, total),
        &state.llm_pauses,
    )
    .await;
    // Past the await, so this frame is going to finish on its own terms: every path from here either
    // returns the body or classifies the failure, and both decide the refund explicitly below.
    slot.armed = false;
    // Dropped however this ends — returned, failed, or the request cancelled out from under us
    // mid-run. That third case is the one a matching `clear` call could not cover, and it is the one
    // that leaves `.status` insisting a dead run is still working.
    drop(reporter);

    // None of the harness's failures says anything about which file was chosen, so none of them
    // touches the pin.
    let translated = translated.map_err(|e| {
        // Refunded only when this request spent NOTHING — no tokens on the viewer's key and no
        // metered OpenSubtitles credit. Both halves are load-bearing and each was wrong on its own.
        //
        // Asking only about the failure's shape missed the resumed run whose batches all came back
        // from the store and then tripped the ratio gate: a slot for a run that never called the
        // provider. Asking only about tokens gave the slot back for a request that had just bought
        // a metered download — and the daily allowance is the ONLY thing bounding how many distinct
        // titles a viewer can burn credits on, since a `Model` failure arms no install-wide marker
        // the way a credential refusal does. A typo'd model name answers 404 for every title, so a
        // twenty-episode season became twenty credits out of a free tier of a handful, with the
        // counter back where it started each time.
        if !e.spent && source_was_cached {
            refund_translation(state, config);
        }
        match e.credential_refused {
            // A key the provider refuses outright will refuse the next film identically.
            true => TranslationFailure::Credential {
                message: e.message,
                key_certain: e.key_certain,
                spent: e.spent,
            },
            false => TranslationFailure::Model(e.message),
        }
    })?;
    let body = srt::serialize(&translated);
    let stored = TranslatedBody { source: Some(source_file_id), srt: body.clone() };
    state.cache.put(
        body_key.to_string(),
        serde_json::to_string(&stored).expect("translation serializes"),
        CACHE_TTL,
    );
    Ok(body)
}

pub(super) fn type_of(season: Option<i64>) -> &'static str {
    if season.is_some() {
        "series"
    } else {
        "movie"
    }
}
