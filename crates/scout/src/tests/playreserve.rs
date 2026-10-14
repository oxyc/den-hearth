//! Go: `internal/scout/playreserve_test.go` (248 lines). Work package 4.
//!
//! Exercises `addbudget` (work package 3's module) through the handler. Only `TestParsePlayReserve` needs
//! nothing outside package 4 (`deps.rs`); the rest keep their Go bodies in compiled-out `pending` items until
//! package 3 (and package 6's metrics) land.

/// A prefetch stops at the reserve; a viewer waiting spends down to zero; both come back as the window drains.
#[test]
#[ignore = "needs addbudget::AddBudget from package 3"]
fn add_budget_prefetch_stops_at_the_reserve_and_play_spends_to_zero() {
    // Enable when package 3 lands: move `pending` up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        let mut now = chrono_utc(2026, 9, 22, 20, 0, 0);
        let mut b = AddBudget::new(std::time::Duration::from_secs(3600), 10);
        b.reserve = 3;
        // Seven prefetches, a minute apart: the last one leaves exactly the reserve.
        for i in 0..7 {
            assert!(
                b.take_at("acct", true, now),
                "prefetch {} refused with {} left, above the reserve of 3",
                i + 1,
                b.remaining("acct")
            );
            now += Duration::from_secs(60);
        }
        assert!(!b.take_at("acct", true, now), "a prefetch spent into the reserve kept for Play");
        assert_eq!(b.remaining("acct"), 3, "a refusal must spend nothing");
        // The first charge (20:00) drains at 21:00, which frees one prefetch.
        assert_eq!(b.frees_in("acct", true), Duration::from_secs(53 * 60), "prefetch freesIn");
        assert_eq!(
            b.frees_in("acct", false),
            Duration::ZERO,
            "a viewer was told to wait with the reserve still there"
        );
        // The viewer spends the reserve, down to zero, and no further.
        for _ in 0..3 {
            assert!(b.take_at("acct", false, now), "Play refused with {} left", b.remaining("acct"));
        }
        assert!(!b.take_at("acct", false, now), "Play spent past the whole allowance");
        assert!(!b.take_at("acct", true, now), "a prefetch was served from a spent allowance");
        // 21:00:30: the 20:00 charge has drained — one slot, which is the viewer's, not the prefetch's.
        now = chrono_utc(2026, 9, 22, 21, 0, 30);
        assert!(!b.take_at("acct", true, now), "a prefetch took the one slot that freed below the reserve");
        assert!(b.take_at("acct", false, now), "Play did not recover as the window drained");
        // 21:06:30: every prefetch charge has drained; three prefetches may run again, and no more.
        now = chrono_utc(2026, 9, 22, 21, 6, 30);
        for i in 0..3 {
            assert!(b.take_at("acct", true, now), "prefetch {} did not recover as the window drained", i + 1);
        }
        assert!(!b.take_at("acct", true, now), "a recovered prefetch spent into the reserve");
    }
    todo!("pending: playreserve_test.go:TestAddBudget_prefetchStopsAtTheReserveAndPlaySpendsToZero")
}

/// With no flag the reserve does not exist: a viewer's ceiling is the whole allowance, exactly as before.
#[test]
#[ignore = "needs addbudget::AddBudget from package 3"]
fn add_budget_without_the_flag_the_reserve_changes_nothing() {
    // Enable when package 3 lands: move `pending` up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        let mut b = AddBudget::new(std::time::Duration::from_secs(3600), 5);
        b.reserve = 4;
        for i in 0..5 {
            assert!(
                b.take("acct", false),
                "add {} of 5 refused — the reserve leaked into the default intent",
                i + 1
            );
        }
        assert!(!b.take("acct", false), "a sixth add against an allowance of 5");
    }
    todo!("pending: playreserve_test.go:TestAddBudget_withoutTheFlagTheReserveChangesNothing")
}

/// A reserve refusal is its own error: scout-side (never filed as the store's refusal) and apart from a spent
/// budget, so the route can answer it with its own code.
#[test]
#[ignore = "needs addbudget::spend_add, refusal bookkeeping (package 2) and metrics (package 6)"]
fn spend_add_prefetch_refusal_is_the_reserve_not_the_budget() {
    // Enable when packages 2, 3 and 6 land: move `pending` up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        let prev = global_add_budget_swap(AddBudget::new(std::time::Duration::from_secs(3600), 6));
        global_add_budget().reserve = 5;
        let before = metrics().add_refused_prefetch();
        assert!(
            spend_add(DebridService::TorBox, "tok", H, true).is_ok(),
            "the one prefetch add above the reserve was refused"
        );
        let err = spend_add(DebridService::TorBox, "tok", H, true)
            .expect_err("the reserve refuses the second prefetch");
        assert!(
            err.is_play_reserve() && err.is_scout_side(),
            "want errPlayReserve wrapped as scout-side, got {err:?}"
        );
        let cache = MemoryCache::new(1 << 20);
        record_refusal(&cache, DebridService::TorBox, "tok", H, &err);
        assert!(
            backed_off(&cache, DebridService::TorBox, "tok", H).is_none(),
            "the reserve was written into the store's refusal memory"
        );
        assert_eq!(metrics().add_refused_prefetch() - before, 1, "prefetch refusals counted");
        let play_before = metrics().add_refused_play();
        for i in 0..5 {
            assert!(
                spend_add(DebridService::TorBox, "tok", H, false).is_ok(),
                "Play refused with the reserve left (add {i})"
            );
        }
        let err = spend_add(DebridService::TorBox, "tok", H, false).expect_err("a spent budget refuses Play");
        assert!(
            !err.is_play_reserve(),
            "a spent budget must refuse Play as spent, not as the reserve: {err:?}"
        );
        assert_eq!(metrics().add_refused_play() - play_before, 1, "play refusals counted");
        global_add_budget_restore(prev);
    }
    todo!("pending: playreserve_test.go:TestSpendAdd_prefetchRefusalIsTheReserveNotTheBudget")
}

/// Through the route: with the allowance at the reserve, /play?prefetch=1 is refused with its own code and a
/// Retry-After and sends no add, while the same request without the flag still adds.
#[tokio::test]
#[ignore = "needs addbudget and package 1's torBoxStore"]
async fn handle_play_prefetch_is_held_back_and_play_is_served() {
    // Enable when packages 1 and 3 land: move `pending` up and delete the wrapper.
    #[cfg(any())]
    async fn pending() {
        let prev = global_add_budget_swap(AddBudget::new(std::time::Duration::from_secs(3600), 5));
        global_add_budget().reserve = 5; // only the reserve is left: no prefetch may add, a viewer still may
        let adds = Arc::new(AtomicI32::new(0));
        let cache = Arc::new(MemoryCache::new(1 << 20));
        let a = adds.clone();
        let store = TorBoxStore::with_client(
            "tok",
            cache.clone(),
            MockDoer::new(move |r| {
                if is_add_endpoint(r) {
                    a.fetch_add(1, Ordering::SeqCst);
                }
                resp(200, r#"{"data":[]}"#)
            }),
        );
        let c = cache.clone();
        let h = new_handler(test_deps(move |d| {
            d.cache = c.clone();
            d.make_stores = Arc::new(move |_| vec![Box::new(store.clone())]);
        }));
        let path = format!(
            "/{}/play/{}",
            valid_blob(),
            crate::play::encode_play_token(&PlayTarget { info_hash: H.to_owned(), ..Default::default() })
        );
        let rec = get(&h, &format!("{path}?prefetch=1"), &[]).await;
        assert_eq!(rec.status(), 503, "prefetch at the reserve");
        assert!(
            body_str(&rec).contains("\"reserved_for_play\""),
            "the reserve must answer reserved_for_play: {}",
            body_str(&rec)
        );
        assert!(
            !body_str(&rec).contains("scout_busy") && !body_str(&rec).contains("store_unavailable"),
            "the reserve must not read as a spent budget or a refusing store"
        );
        let secs: i64 = hdr(&rec, "retry-after").parse().unwrap_or(0);
        assert!(secs >= 1, "retry-after = {:?}, want a positive number of seconds", hdr(&rec, "retry-after"));
        assert_eq!(adds.load(Ordering::SeqCst), 0, "a held-back prefetch sent adds");
        let rec = get(&h, &path, &[]).await;
        assert!(
            !body_str(&rec).contains("reserved_for_play") && !body_str(&rec).contains("scout_busy"),
            "Play was refused by the budget with the reserve left"
        );
        assert_eq!(adds.load(Ordering::SeqCst), 1, "Play sent adds, want 1 — the reserve is for it");
        global_add_budget_restore(prev);
    }
    todo!("pending: playreserve_test.go:TestHandlePlay_prefetchIsHeldBackAndPlayIsServed")
}

/// /metrics reports the allowance as each intent sees it, and the refusals per intent.
#[test]
#[ignore = "needs addbudget and the metrics render (package 6)"]
fn metrics_report_the_allowance_and_refusals_per_intent() {
    // Enable when packages 3 and 6 land: move `pending` up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        let prev = global_add_budget_swap(AddBudget::new(std::time::Duration::from_secs(3600), 10));
        global_add_budget().reserve = 3;
        let out = metrics().render(-1);
        for want in [
            r#"scout_add_budget_remaining_by_intent{intent="play"} -1"#,
            r#"scout_add_budget_remaining_by_intent{intent="prefetch"} -1"#,
            r#"scout_add_budget_refusals_total{intent="play"}"#,
            r#"scout_add_budget_refusals_total{intent="prefetch"}"#,
        ] {
            assert!(out.contains(want), "before any spend, /metrics lacks {want:?}");
        }
        for _ in 0..8 {
            global_add_budget().take("acct", false);
        }
        let out = metrics().render(-1);
        for want in [
            "scout_add_budget_remaining 2\n",
            "scout_add_budget_remaining_by_intent{intent=\"play\"} 2\n",
            "scout_add_budget_remaining_by_intent{intent=\"prefetch\"} 0\n",
        ] {
            assert!(out.contains(want), "/metrics lacks {want:?}:\n{out}");
        }
        global_add_budget_restore(prev);
    }
    todo!("pending: playreserve_test.go:TestMetrics_reportTheAllowanceAndRefusalsPerIntent")
}

/// Go `TestParsePlayReserve`: zero is a real value; anything that does not parse, or that would leave a prefetch
/// no adds at all, reads as the default.
#[test]
#[ignore = "needs addbudget::DEFAULT_PLAY_RESERVE and ADD_BUDGET_LIMIT from package 3"]
fn parse_play_reserve() {
    // Enable when package 3 lands: move `pending` up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        for (input, want) in [
            ("", DEFAULT_PLAY_RESERVE),
            ("0", 0),
            (" 8 ", 8),
            ("-1", DEFAULT_PLAY_RESERVE),
            ("five", DEFAULT_PLAY_RESERVE),
            // would leave a prefetch nothing at all
            (&ADD_BUDGET_LIMIT.to_string(), DEFAULT_PLAY_RESERVE),
        ] {
            assert_eq!(parse_play_reserve(input), want, "parsePlayReserve({input:?})");
        }
        let s = settings_from_env(|k| if k == "PLAY_RESERVE_ADDS" { "2".to_owned() } else { String::new() });
        assert_eq!(s.play_reserve_adds, 2, "PLAY_RESERVE_ADDS=2");
    }
    todo!("pending: playreserve_test.go:TestParsePlayReserve")
}

/// Go `TestSetPlayReserve`: setting the reserve on the global budget.
#[test]
#[ignore = "needs addbudget::set_play_reserve from package 3"]
fn set_play_reserve() {
    // Enable when package 3 lands: move `pending` up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        let prev = global_add_budget_swap(AddBudget::new(std::time::Duration::from_secs(3600), 10));
        set_play_reserve(7);
        assert_eq!(global_add_budget().play_reserve(), 7, "reserve");
        global_add_budget_restore(prev);
    }
    todo!("pending: playreserve_test.go:TestSetPlayReserve")
}
