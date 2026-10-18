//! Session creation: candidate ranking, bounded opening, release selection, and immutable session assembly.

use super::*;

/// `POST /remux/session`: pick and probe a release, choose its audio track, and set up its session.
pub async fn create(
    st: &Arc<AppState>,
    admission: Admission,
    want: &Want<'_>,
    public: bool,
) -> Result<(Arc<Session>, Timing), ApiError> {
    let imdb = want.id;
    // Minted now rather than once a release is chosen, so every decision this call logs while picking one —
    // a skip, a fallback to a conversion — carries the same id as the session it leads to, and one grep over
    // `session {short}` joins them. Reused below as the session's own `sid`.
    let sid = crate::auth::random_id();
    let short = sid[..6].to_string();
    let rid_suffix = want.rid.map(|r| format!(" rid={r}")).unwrap_or_default();
    // Narrowed by the box's own uplink budget, where one is configured, so a copy's fit-check below weighs
    // every other remote session's own need off the same link rather than judging this one in isolation.
    let max_bitrate =
        budgeted_max_bitrate(want.max_bitrate, st.cfg.uplink_bps, other_remote_bps(&st.sessions(), &sid));
    let base = scout_base(st, want.scout)?;
    let by_install = !matches!(admission, Admission::Browser(_));
    let guest = matches!(admission, Admission::Guest(_));
    let (owner, key, share) = match admission {
        Admission::Browser(b) => (b, st.cfg.scout_key.clone(), 1),
        Admission::Install => (crate::auth::install_id(&base), None, st.cfg.max_sessions_per_install),
        Admission::Guest(o) => (o, None, st.cfg.guest_max_sessions),
    };
    let source = scout::ScoutSource { base, key };
    // Checked before any work: a bad install is the browser's mistake, not a reason to probe a release.
    let mut sub_langs: Vec<String> = Vec::new();
    for l in want.subtitle_languages.iter().map(|l| crate::lang::canonical(l)) {
        if !l.is_empty() && !sub_langs.contains(&l) && sub_langs.len() < crate::subs::MAX_LANGUAGES {
            sub_langs.push(l);
        }
    }
    let sub_base = match want.subtitles {
        Some(url) if !sub_langs.is_empty() => Some(crate::config::local(
            &scout::validate_scoped(url, &st.cfg.subtitle_origins).map_err(|why| {
                api(
                    StatusCode::BAD_REQUEST,
                    "bad_subtitles",
                    format!("The subtitles URL was refused: {why}."),
                )
            })?,
            &st.cfg.origin_aliases,
        )),
        _ => None,
    };
    // A browser starting another title is done with the one it was watching; making it wait out the
    // idle timer for its own old session would turn every change of mind into a 429. An install plays
    // `MAX_SESSIONS_PER_INSTALL` at once — a household's people — and past that its oldest gives way. A replacement
    // mid-film keeps the one it replaces until it plays.
    let slot = st.reserve(&owner, share, want.replaces).await.map_err(|refused| match refused {
        // Distinct from `Full`: nothing of this owner's own is live to evict, because its last attempt
        // hasn't finished opening yet. den-edge reads this apart from a genuinely full server.
        Refused::StartingAlready => api(
            StatusCode::TOO_MANY_REQUESTS,
            "starting_already",
            "This browser or install is still finishing its last attempt.",
        ),
        Refused::Full => api(
            StatusCode::TOO_MANY_REQUESTS,
            "too_many_sessions",
            "This browser or install, or the server, has no free session slot.",
        ),
        Refused::NotYours => api(
            StatusCode::FORBIDDEN,
            "not_your_session",
            "The session to replace is not this browser's or install's.",
        ),
        Refused::Pending => api(
            StatusCode::CONFLICT,
            "replacement_pending",
            "A replacement is already starting for this browser or install.",
        ),
    })?;
    let secrets = [source.base.as_str()];
    // Timed for the `Server-Timing` header on the 201 (den-edge#234's step 0): how long scout's list took
    // ("resolve"), and how long picking a release from it took ("open", with how many were tried).
    let resolve_started = Instant::now();
    let list = scout_list(st, &source, imdb, by_install, want.playable).await?;
    let resolve_ms = resolve_started.elapsed().as_millis() as u64;
    let candidates = scout::candidates(
        &list,
        want.filename,
        want.playable.is_some_and(Playable::takes_av1),
        want.playable.is_some_and(Playable::takes_vp9),
    );
    // Why the release the player named was passed over, where it was: `release.requested` in the answer.
    let mut requested_why: Option<String> = None;
    // The containers of what was left out by name, for an answer that ends with nothing to play.
    let mut formats: Vec<&'static str> = Vec::new();
    // Left out of `candidates` by their names, which a person reading the log should see was on purpose.
    for s in list.iter().filter(|s| s.attributes.cached == Some(true) && !s.attributes.three_d) {
        if let Some(ext) = scout::left_out_by_name(s) {
            eprintln!(
                "session {short}: {imdb} skipped \"{}\" unopened: {ext} is not a container this service opens{rid_suffix}",
                s.attributes.label
            );
            if !formats.contains(&ext) {
                formats.push(ext);
            }
            if want.filename == Some(s.filename()) {
                requested_why = Some(format!("{ext} files can't be played here"));
            }
        }
    }
    if candidates.is_empty() {
        return Err(ApiError {
            formats,
            ..api(StatusCode::NOT_FOUND, "no_release", "No cached release in a codec this service can remux.")
        });
    }
    let takes_hevc = player_takes_hevc(want.playable, want.video_codecs);
    // What scout says of each release decides, before any is opened, which are tried first — those that play as
    // they are, then those that play only converted, scout's ranking holding within each — and which aren't
    // opened at all. The named release stays first unless it can't play. Scout has already ranked a list for a
    // browser that sent its report in the same order; this is what orders one for a player that sent none.
    let mut ranked: Vec<(Fit, &scout::Stream)> = Vec::new();
    // A transcode is chosen here, before playback, or not at all: never for a player replacing a session mid-film.
    let may_convert = !want.no_transcode && !want.fits_only;
    for c in &candidates {
        if want.exclude.iter().any(|f| f == c.filename()) {
            eprintln!(
                "session {short}: {imdb} skipped \"{}\" unopened: the player is switching away from it{rid_suffix}",
                c.attributes.label
            );
            continue;
        }
        let remembered = unplayable_key(imdb, c).and_then(|k| st.unplayable().get(&k, unix_now()));
        if let Some(why) = remembered {
            eprintln!(
                "session {short}: {imdb} skipped \"{}\" unopened: remembered: {why}{rid_suffix}",
                c.attributes.label
            );
            if want.filename == Some(c.filename()) {
                requested_why = Some(why);
            }
            continue;
        }
        match fit(&c.attributes, want.playable, takes_hevc) {
            Fit::Never => {
                eprintln!(
                    "session {short}: {imdb} skipped \"{}\" unopened: scout's attributes say it can't play here{rid_suffix}",
                    c.attributes.label
                );
                if want.filename == Some(c.filename()) {
                    requested_why = scouted_verdict(&c.attributes, want.playable, takes_hevc).1;
                }
            }
            // Only a transcode could play it, and none may be started: not worth opening.
            Fit::Convert if !may_convert => {}
            f => ranked.push((f, c)),
        }
    }
    let named_first = usize::from(ranked.first().is_some_and(|(_, c)| want.filename == Some(c.filename())));
    ranked[named_first..].sort_by_key(|(f, _)| *f);
    let mut no_transcode = false;
    let (mut chosen, fallback, mut too_big, open_ms, tried) = {
        let source = &source;
        let started = Instant::now();
        let mut queue = ranked.into_iter().peekable();
        let mut opening = FuturesOrdered::new();
        let mut tried = 0;
        let mut chosen = None;
        // The first release that plays only converted: the last resort, taken once nothing tried plays as it is.
        let mut fallback = None;
        // Releases that play as they are but need more than the player's `maxBitrate`, in rank order.
        let mut too_big: Vec<(&scout::Stream, (scout::Resolved, MediaInfo))> = Vec::new();
        loop {
            while opening.len() < PARALLEL_OPENS && tried < MAX_TRIES && started.elapsed() < PICK_BUDGET {
                // With a converted release in hand, one scout says plays only converted can't do better.
                if fallback.is_some() && queue.peek().is_some_and(|(f, _)| *f == Fit::Convert) {
                    break;
                }
                let Some((_, c)) = queue.next() else { break };
                tried += 1;
                opening.push_back(async move {
                    let opened = tokio::time::timeout(OPEN_TIMEOUT, open(st, source, imdb, c)).await;
                    let late = || OpenFailure::passing(format!("not open after {}s", OPEN_TIMEOUT.as_secs()));
                    (c, opened.unwrap_or_else(|_| Err(late())))
                });
            }
            // In the order they were started, which is rank order.
            let Some((c, opened)) = opening.next().await else { break };
            match opened {
                // Dolby Vision profile 5 has no base layer to fall back to: stripped or transcoded, its picture is
                // green and purple. Only a player that shows profile 5 plays it, and only as it is.
                Ok((_, info)) if no_picture(want.playable, takes_hevc, &info) => {
                    let record = if info.dolby_vision_recordless {
                        " (no Dolby Vision record in the container)"
                    } else {
                        ""
                    };
                    eprintln!(
                        "session {short}: {imdb} skipped \"{}\": {}{record} has no picture without Dolby Vision{rid_suffix}",
                        c.attributes.label,
                        info.dolby_vision.map(|dv| dv.to_string()).unwrap_or_default()
                    );
                    if want.filename == Some(c.filename()) {
                        requested_why = probed_verdict(&info, want.playable, takes_hevc).1;
                    }
                }
                // HEVC the player can't take as it is, and every MPEG-TS and AVI, plays only converted on the GPU: kept
                // as the last resort while the rest are looked through for one that plays untouched — unless the
                // player named this release (another audio track of it), which it keeps.
                Ok((r, info)) if converts_only(want.playable, takes_hevc, &info) => {
                    eprintln!(
                        "session {short}: {imdb} \"{}\" ({} {}) plays here only converted{rid_suffix}",
                        c.attributes.label,
                        info.container,
                        info.codecs.as_deref().unwrap_or(match &info.video {
                            VideoCodec::Other(c) => c,
                            VideoCodec::H264 => "H.264",
                            _ => "HEVC",
                        })
                    );
                    let named = want.filename == Some(c.filename());
                    if named {
                        requested_why = probed_verdict(&info, want.playable, takes_hevc).1;
                    }
                    if fallback.is_none() {
                        fallback = Some((c, (r, info)));
                    }
                    if named {
                        break;
                    }
                }
                // H.264, AV1 or VP9 beyond the player: nothing here makes H.264 smaller, or converts AV1 or VP9 at all.
                Ok((_, info)) if !plays(want.playable, takes_hevc, &info) => {
                    eprintln!(
                        "session {short}: {imdb} skipped \"{}\": {} is beyond this player{rid_suffix}",
                        c.attributes.label,
                        info.codecs.as_deref().unwrap_or("its video")
                    );
                    if want.filename == Some(c.filename()) {
                        requested_why = probed_verdict(&info, want.playable, takes_hevc).1;
                    }
                }
                // A copy needing more than the player's link carries is kept aside while one that fits is looked for —
                // unless the player named it, which is then decided on below as the only one.
                Ok((r, info))
                    if over(
                        max_bitrate,
                        need(&info, &Delivery::of(&info, r.size.or(c.attributes.size_bytes), want)),
                    ) =>
                {
                    eprintln!(
                        "session {short}: {imdb} \"{}\" needs {} — more than the player's {} kbit/s{rid_suffix}",
                        c.attributes.label,
                        need(&info, &Delivery::of(&info, r.size.or(c.attributes.size_bytes), want))
                            .map_or_else(String::new, |n| n.to_string()),
                        max_bitrate.unwrap_or(0) / 1000
                    );
                    let named = want.filename == Some(c.filename());
                    too_big.push((c, (r, info)));
                    if named {
                        break;
                    }
                }
                Ok(found) => {
                    chosen = Some((c, found, false));
                    break;
                }
                Err(failure) => {
                    let why = crate::redact::scrub(&failure.why, &secrets);
                    eprintln!(
                        "session {short}: {imdb} skipped \"{}\": {why}{rid_suffix}",
                        c.attributes.label
                    );
                    if let Some(key) = unplayable_key(imdb, c).filter(|_| failure.lasting) {
                        st.remember_unplayable(key, why);
                    }
                    if want.filename == Some(c.filename()) {
                        requested_why = Some("it could not be opened".into());
                    }
                }
            }
        }
        (chosen, fallback, too_big, started.elapsed().as_millis() as u64, tried)
    };
    // Nothing plays as it is with a head start of `MAX_PREBUFFER` or less. What comes next, in order: a copy that asks
    // at most `LONG_PREBUFFER` of it, a transcode, the copy that starves least — and else a refusal, up front, which a
    // retry would not change.
    // The transcode the link carries at its peak; none fits a link below the floor's, and one that would starve ranks
    // below the copy that starves least — taken only where no copy plays at all.
    let fitting = job::preset_for(max_bitrate);
    let preset = fitting.unwrap_or(job::SD540);
    let fallback_seen = fallback.is_some();
    let delivery = |t: &(&scout::Stream, (scout::Resolved, MediaInfo))| {
        Delivery::of(&t.1 .1, t.1 .0.size.or(t.0.attributes.size_bytes), want)
    };
    let bitrate =
        |t: &(&scout::Stream, (scout::Resolved, MediaInfo))| need(&t.1 .1, &delivery(t)).map(|n| n.bitrate);
    // Waited out at the link with its headroom, as fitting is weighed: a copy that only the whole link would carry
    // is the one that stalls when the link dips, and a wait up front does not make it fit.
    let usable = max_bitrate.map(|m| (m as f64 * LINK_HEADROOM) as u64);
    let head_start = |t: &(&scout::Stream, (scout::Resolved, MediaInfo))| {
        prebuffer(&t.1 .1, &delivery(t), usable).unwrap_or(f64::INFINITY)
    };
    if chosen.is_none() && !want.fits_only {
        let longer = (0..too_big.len())
            .filter(|&i| head_start(&too_big[i]) <= LONG_PREBUFFER)
            .min_by(|&a, &b| head_start(&too_big[a]).total_cmp(&head_start(&too_big[b])));
        if let Some(i) = longer {
            let (c, found) = too_big.swap_remove(i);
            chosen = Some((c, found, false));
        }
    }
    let who = if guest { "a guest" } else { "a member" };
    // Set only on the branch that actually converts, for the end-of-session summary line.
    let mut transcode_reason: Option<String> = None;
    if chosen.is_none() && may_convert && (fitting.is_some() || too_big.is_empty()) {
        // A conversion at the preset the link takes — of the first release that plays only converted, else of the
        // first HEVC copy too big for the link that the preset comes in under.
        let from_too_big = fallback.is_none();
        let nearest = (0..too_big.len()).min_by_key(|&i| bitrate(&too_big[i]).unwrap_or(u64::MAX));
        let why = match (from_too_big, nearest.map(|i| &too_big[i])) {
            (true, Some(t)) => format!(
                "no copy plays within a {LONG_PREBUFFER:.0}s head start on the {} kbit/s link; the nearest, \"{}\", \
                 needs {}",
                max_bitrate.unwrap_or(0) / 1000,
                t.0.attributes.label,
                need(&t.1.1, &delivery(t)).map_or_else(|| "an unknown rate".to_string(), |n| n.to_string())
            ),
            _ => "no release plays in this player as it is".to_string(),
        };
        let convert = fallback.or_else(|| {
            let hevc = |t: &(&scout::Stream, (scout::Resolved, MediaInfo))| {
                t.1 .1.video == VideoCodec::Hevc
                    && bitrate(t).is_some_and(|b| b > preset.bitrate && source_within_reach(b, max_bitrate))
            };
            // The cheapest to read, not the first ranked: a conversion reads the whole source in real time.
            let i = (0..too_big.len())
                .filter(|&i| hevc(&too_big[i]))
                .min_by_key(|&i| bitrate(&too_big[i]).unwrap_or(u64::MAX))?;
            Some(too_big.remove(i))
        });
        if let Some((c, found)) = convert {
            match st.can_transcode() {
                true => {
                    // The GPU is acquired fairly only when the player asks for media; an abandoned session owns none.
                    eprintln!(
                        "session {short}: {imdb} converting \"{}\" for {who} to {}p at {} kbit/s (at most {}){}: {why}{rid_suffix}",
                        c.attributes.label,
                        preset.height,
                        preset.bitrate / 1000,
                        preset.peak / 1000,
                        if fitting.is_none() { ", more than the link carries" } else { "" }
                    );
                    transcode_reason = Some(why.clone());
                    chosen = Some((c, found, true));
                }
                // The GPU is unavailable: it still plays as it is, below.
                false if from_too_big => too_big.push((c, found)),
                false => {
                    no_transcode = true;
                    eprintln!(
                        "session {short}: {imdb} can't play \"{}\" for {who}: it needs converting ({why}), and \
                         transcoding is unavailable{rid_suffix}",
                        c.attributes.label
                    );
                }
            }
        }
    }
    if chosen.is_none() && !want.fits_only {
        // Last, the copy too big for the link that needs the least: a stall now and then beats nothing to play.
        if let Some(i) = (0..too_big.len()).min_by_key(|&i| bitrate(&too_big[i]).unwrap_or(u64::MAX)) {
            let (c, found) = too_big.swap_remove(i);
            chosen = Some((c, found, false));
        }
    }
    let Some((c, (resolved, info), transcode)) = chosen else {
        if want.fits_only {
            return Err(api(
                StatusCode::NOT_FOUND,
                "no_fitting_copy",
                "No other release plays here as it is within what the link carries.",
            ));
        }
        // Only a conversion would have played it: the GPU in use, or none may be started for this request.
        if no_transcode || (fallback_seen && !may_convert) {
            return Err(api(
                StatusCode::NOT_FOUND,
                "no_copy",
                "This can't be played on this device right now: no release plays here as it is, and none can be \
                 converted now.",
            ));
        }
        return Err(ApiError {
            formats,
            ..api(StatusCode::NOT_FOUND, "no_playable_release", "No cached release could be opened.")
        });
    };
    let (audio, audio_reason) = match want.audio_track {
        Some(n) if n < info.audio.len() => (n, crate::lang::AudioReason::Explicit),
        Some(_) => {
            return Err(api(
                StatusCode::BAD_REQUEST,
                "bad_audio_track",
                format!("This release has {} audio tracks.", info.audio.len()),
            ));
        }
        None => crate::lang::pick_audio(&info.audio, want.audio),
    };
    let source_channels = info.audio[audio].channels;

    let exp = unix_now() + (info.duration.ceil() as u64 + SESSION_GRACE_SECS).min(SESSION_MAX_SECS);
    let sig = crate::auth::url_sig(&st.cfg.url_key, &sid, exp);
    let dir = st.cfg.scratch_dir.join(format!("s-{sid}"));
    if let Err(e) = std::fs::create_dir_all(&dir) {
        st.scratch_ok.store(false, Relaxed);
        eprintln!("session {short}: scratch {} is unusable: {e}{rid_suffix}", st.cfg.scratch_dir.display());
        return Err(api(
            StatusCode::SERVICE_UNAVAILABLE,
            "scratch_unavailable",
            "Scratch space is unavailable.",
        ));
    }
    st.scratch_ok.store(true, Relaxed);
    // A resume at or past the end starts the title over.
    let start_at = effective_start(want.start_at, info.duration);
    let segments =
        playlist::segments_for_start(&info.keyframes, info.duration, playlist::TARGET_SECS, start_at);
    let first_segment = start_segment(&segments, start_at);
    let size = resolved.size.or(c.attributes.size_bytes);
    // A player that named a release and got another is told why, where this knows: what it saw of the named one
    // just now, or of an earlier opening.
    let requested = want.filename.filter(|f| *f != c.filename()).and_then(|f| {
        let why = requested_why.or_else(|| {
            let info = st.known().get(&format!("{imdb}/{f}"))?;
            probed_verdict(&info, want.playable, takes_hevc).1
        })?;
        Some(Requested { filename: f.to_string(), why })
    });
    // What language the session's own audio is in, so `plan()` can tell a dub's captions (forced/
    // foreign-parts-only is right) apart from a translation subtitle (the full track is right).
    let audio_lang = info.audio[audio].language.as_deref().map(crate::lang::canonical);
    let head_sum = resolved.head.get(..crate::subs::HASH_CHUNK as usize).map(crate::subs::chunk_sum);
    // den-subtitles' own list for the title, fetched now — before any rendition is promised — so `plan()`
    // offers a `den: true` language only when den-subtitles can actually serve it, never one it merely
    // "may have" (den-remux's phantom-English report: a Swedish/English viewer against a Polish-only
    // Fauda S1E3 release saw "English" in the menu and loaded an empty track). The fetch costs no
    // OpenSubtitles quota — it's the same free list `list_subtitles` would fetch anyway once a rendition
    // is actually opened. A failed or timed-out fetch is read the same as an empty list: the safe default
    // is to drop a den-only rendition, not promise one on a guess.
    let den_list = match &sub_base {
        Some(base) => {
            let secrets = [base.as_str()];
            let release = crate::subs::Release {
                id: imdb,
                input: &resolved.url,
                head_sum,
                size,
                filename: c.filename(),
            };
            crate::subs::fetch_list(&st.scout_http, st.local_scout.get(), &resolved.client, base, &release)
                .await
                .inspect_err(|e| {
                    crate::log_limited("subtitles_list", || {
                        format!(
                            "session {short}: subtitles: {}{rid_suffix}",
                            crate::redact::scrub(e, &secrets)
                        )
                    });
                })
                .unwrap_or_default()
        }
        None => Vec::new(),
    };
    let renditions =
        crate::subs::plan(&sub_langs, sub_base.is_some(), &info.subtitles, audio_lang.as_deref(), &den_list);
    let text_tracks: Vec<usize> = renditions.iter().filter_map(|r| r.own).collect();
    let subs = sub_base.map(|base| crate::subs::Subs { base, head_sum, cache: Default::default() });
    let named: Vec<(String, String)> =
        renditions.iter().map(|r| (r.lang.clone(), crate::lang::name(&r.lang))).collect();
    let opened_key = opened_key(&source.base, c);
    // A transcode's keyframes are its encoder's IDRs, in closed GOPs; a copy's are the release's own.
    let closed_gops = transcode || info.closed_gops;
    let mode = if transcode { PlaybackMode::Transcode(preset) } else { PlaybackMode::Direct };
    let playback = PlaybackPlan::for_release(&info, size, want, usable, &segments, mode, audio);
    let (codecs, resolution, peak, avg, dovi, dv_signal) = playback.output.parts();
    let audio_copy = playback.audio_copy;
    let audio_out = playback.audio_out;
    let shown_label = opened_label(
        &c.attributes.label,
        c.attributes.size_bytes,
        c.attributes.pack_size_bytes,
        resolved.size,
    );
    let session = Arc::new(Session {
        prebuffer: playback.prebuffer,
        need: playback.need,
        max_bitrate: want.max_bitrate,
        demand: playback.demand,
        master: playlist::master(
            codecs,
            dv_signal,
            &match (audio_copy, audio_out) {
                (Some(codec), _) => playlist::Audio::Copy {
                    codec,
                    channels: source_channels,
                    language: info.audio[audio].language.as_deref(),
                },
                (None, job::AudioOut::Surround | job::AudioOut::Surround71) => playlist::Audio::AacSurround {
                    channels: audio_out.channels(source_channels),
                    language: info.audio[audio].language.as_deref(),
                },
                (None, _) => playlist::Audio::Aac,
            },
            peak,
            avg,
            Some(resolution),
            info.frame_rate,
            &named,
            closed_gops,
        ),
        media: playlist::media(&segments, start_at, closed_gops),
        reports: AtomicU8::new(0),
        speed_probes: AtomicU8::new(0),
        sid: sid.clone(),
        sig,
        exp,
        owner,
        public,
        started: Instant::now(),
        rid: want.rid.map(String::from),
        imdb: imdb.to_string(),
        dir,
        release: Release {
            label: shown_label.0,
            filename: c.filename().to_string(),
            size,
            pack_size: shown_label.1,
            requested,
        },
        segments,
        // Only where the file named its own size (`Content-Range`): the door answers ranges of it.
        door: std::sync::OnceLock::from(
            resolved
                .size
                .map(|size| st.doors.open(resolved.client.clone(), &resolved.url, &resolved.head, size)),
        ),
        source,
        opened_key,
        file_key: file_key(&resolved),
        resolve_gate: tokio::sync::Mutex::new(()),
        info,
        audio,
        audio_copy,
        audio_out,
        audio_channels: audio_out.channels(source_channels),
        dovi,
        subs,
        renditions,
        text_tracks,
        own: Default::default(),
        transcoded: transcode,
        transcode_reason: transcode_reason.clone(),
        preset,
        inner: Mutex::new(Inner {
            job: None,
            next_job: 0,
            gops: Vec::new(),
            finished: Vec::new(),
            init: None,
            last_seen: Instant::now(),
            last_heartbeat: None,
            want: first_segment,
            ended: false,
            bytes: 0,
            failures: 0,
            input: resolved.url,
            reresolved: false,
            play_url: c.url.clone(),
            warming: false,
            resolve_failed: None,
            segments_served: 0,
            seconds_served: 0.0,
            subtitle_used: None,
        }),
        producer_gate: tokio::sync::Mutex::new(()),
        wake: Notify::new(),
    });
    // Written once, before the session can be asked for anything: what a process after this one rebuilds it from.
    let record = session.record(start_at, unix_now());
    let (dir, key) = (session.dir.clone(), st.cfg.url_key.clone());
    match tokio::task::spawn_blocking(move || crate::resume::write(&dir, &key, &record)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => eprintln!("session {short}: its restart record could not be written: {e}{rid_suffix}"),
        Err(e) => eprintln!("session {short}: its restart record could not be written: {e}{rid_suffix}"),
    }
    st.insert_session(session.clone());
    st.replacement_started(&slot, &session.sid);
    st.sessions_started.fetch_add(1, Relaxed);
    let dv = session.info.dolby_vision.map(|dv| match session.dovi {
        job::Dovi::Keep => format!(", {dv} kept"),
        _ => format!(", {dv} stripped to its base layer"),
    });
    let player = want.playable.map_or_else(|| "no capability report".to_string(), |p| p.to_string());
    let link = max_bitrate.map(|b| format!(", link {} kbit/s", b / 1000)).unwrap_or_default();
    let client = &want.client;
    eprintln!(
        "session {}: {imdb} \"{}\" ({}, {:?} {}{}{}, {:.0}s, {} keyframes, {} segments, audio {} {}{}; player: {player}{link}; client: {client}){rid_suffix}",
        session.short(),
        session.release.label,
        session.info.container,
        session.info.video,
        codecs,
        dv.unwrap_or_default(),
        match (session.transcoded, tonemaps(&session.info)) {
            (true, true) => format!(" → {}p H.264 SDR on the GPU", session.preset.height),
            (true, false) => format!(" → {}p H.264 on the GPU", session.preset.height),
            (false, true) => ", HDR".to_string(),
            (false, false) => String::new(),
        },
        session.info.duration,
        session.info.keyframes.len(),
        session.segments.len(),
        session.audio,
        session.info.audio[session.audio].language.as_deref().unwrap_or("und"),
        match (session.audio_copy, session.audio_out) {
            (Some(c), _) => format!(" copied as {c}"),
            (None, job::AudioOut::Surround) => " as AAC 5.1".to_string(),
            (None, job::AudioOut::Surround71) => " as AAC 7.1".to_string(),
            (None, _) => String::new(),
        },
    );
    // Answers "does this release have an embedded English track, and why wasn't it used" from the
    // log alone, on the next report — never a URL, just the release's own subtitle streams and
    // `plan()`'s verdict on each.
    eprintln!(
        "session {}: subtitles: {}{rid_suffix}",
        session.short(),
        crate::subs::describe_subtitles(
            &session.info.subtitles,
            &session.renditions,
            session.audio_lang().as_deref()
        )
    );
    eprintln!(
        "{}",
        audio_track_line(
            &short,
            want.rid,
            st.cfg.log_identity,
            session.audio,
            session.info.audio[session.audio].language.as_deref().unwrap_or("und"),
            session.audio_copy.unwrap_or("aac"),
            session.audio_channels,
            &audio_reason,
        )
    );
    tokio::spawn(supervise(st.clone(), session.clone()));
    Ok((session, Timing { resolve_ms, open_ms, tried }))
}
