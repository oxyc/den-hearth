# Porting den-scout to Rust

Source: `internal/scout` in the Go repository `den-scout` (`origin/main`), 16,741 lines of code in 39 files and
21,509 lines of tests in 49 test files. This crate is the skeleton: one module per Go source file, the
shared types the tests need, and no logic. The port is **tests first**: port a Go test file, watch it
fail against the `todo!()` stubs, then write the module until it passes.

Read the Go with `git -C ~/Projects/Personal/den-scout show origin/main:internal/scout/<file>`.

## Rules for porters

1. **You own your work package's module files and test files, and nothing else.** Do not edit another
   package's module.
2. **Do not edit the shared types.** They are marked `FROZEN` in their doc comments and listed below.
   If you need a field, a variant or a trait method added, write it in your report (name, type, why, the
   Go line that needs it) and carry on with a local workaround; the integrator adds it for everyone.
3. Module bodies still `todo!()` belong to whoever owns that module. A test that needs a module you do not
   own stays red until its owner lands; say which in your report rather than stubbing it yourself.
4. Port Go tests one-to-one. Keep the Go test name as the Rust fn name in snake_case
   (`TestCancel_oursAndUnfinished` becomes `cancel_ours_and_unfinished`) so a failure maps back to the Go.
5. Tests live in `src/tests/<name>.rs`, inside the crate, because the Go tests reach unexported items.
   Make an item `pub(crate)` in your own module when a test needs it.
6. Do not reformat or restructure: the Go comments explain why, often with measurements. Carry them across.

## Conventions the skeleton assumes

- Go `int` is `i64` (`i32` in `Probe`), `*int` is `Option<i64>`, `time.Duration` is `std::time::Duration`,
  a zero `time.Time` is `None`.
- `context.Context` is gone. Cancellation is dropping the future; a deadline is `tokio::time::timeout`
  around it. Where Go passes a context only to carry a value (a request id), pass the value.
- Go's `error` values that callers branch on are the `StoreError` enum (`stores/mod.rs`); sentinel errors
  such as `errWouldAdd` are reason constants (`ERR_WOULD_ADD`).
- Go's optional interfaces (type assertions on a `Store`) are defaulted methods on the `Store` trait that
  return `None` when a store does not implement them.
- HTTP goes through `scrape::Doer`, taking and returning `reqwest` types. A test builds a response with
  `http::Response` and converts it (see `src/tests/mock.rs`, the port of `mock_test.go`).
- Package-level mutable state in Go (`SetPlayReserve`, `EnableIndexerConfigMinting`, `SetLogIdentity`,
  `torboxStatusFlights`, the batchers) becomes `static` with `OnceLock`/`Mutex`, or a field on a struct the
  handler owns; the porter of that module decides, and says so.
- The sample files Go's probe tests read are in `internal/scout/testdata/` (7 files). Copy them to
  `crates/scout/testdata/` when you port `probe_containers` and `probe_mp4`.

## The shared types (FROZEN)

| Type | File | Go source |
|---|---|---|
| `Config`, `Filters`, `DebridAccount`, `DebridService`, `Indexer` and the constants beside them | `config.rs` | `config.go` |
| `Store` (with its optional-interface methods), `StoreError`, `StoreStatus`, `StatusAnswer`, `CancelOutcome`, `ResolveTarget`, `DeadLinkError`, `StoreUnavailableError`, `CacheTruth`, the `FETCH_*` constants | `stores/mod.rs` | `stores.go` |
| `Cache` (with `put_class`, `persistent`), `CacheClass` | `cache.rs` | `cache.go` |
| `Doer`, `Scraper`, `ScrapeQuery` | `scrape.rs` | `scrape.go` |
| `RawStream` | `rank.rs` | `rank.go` |
| `Probe` | `probe.rs` | `probe.go` |
| `TorrentFile` | `season.rs` | `season.go` |
| `PlayTarget` | `play.rs` | `play.go` |
| `StreamId` | `id.rs` | `id.go` |
| `SealKeyring` | `seal.rs` | `seal.go` |
| `Settings` | `deps.rs` | `deps.go` |

Not stubbed, because only their own package uses them: `Deps`, `handler`, `StorePool`'s methods,
`StreamAttributes`, `ClientPlayable`, `cineMeta`, the metric set. `Deps` is work package 4's and every
handler-level test builds one, so it should land early.

`stores.go` (5,142 lines) is split across `stores/` because two packages share it:
`mod.rs` (lines 1-445, shared), `torbox.rs` (446-3770 less the refusal bookkeeping), `refusal.rs`
(780-1560, the cross-store cache keys, add attempts, queued markers and refusal backoff, which `torbox.rs`
calls), `realdebrid.rs` (3770-4316), `premiumize.rs` (4316-4722), `pool.rs` (4722-end, plus the
`StorePool` reporter methods near 1123-1450).

## Work packages

Test lines per package, in parentheses. Total 21,509.

| # | Package | Test lines | Modules (production Go lines, approximate) |
|---|---|---|---|
| 1 | Stores I: TorBox | 3,082 | `stores/torbox.rs` (about 2,550) |
| 2 | Stores II: Real-Debrid, Premiumize, pool, refusals | 3,420 | `stores/{realdebrid,premiumize,pool,refusal}.rs` (about 2,150) |
| 3 | Add budget | 3,782 | `addbudget.rs` (294) |
| 4 | Handler and HTTP surface | 3,734 | `handler.rs` 2,485, `manifest.rs` 71, `deps.rs` 347, `availability.rs` 202, `playwarm.rs` 25, `coverage.rs` 179, `configure.rs` 10 |
| 5 | Ranking, indexers, probing | 3,959 | `rank.rs` 730, `attributes.rs` 387, `label.rs` 165, `season.rs` 566, `client.rs` 185, `probe.rs` 737, `probe_avi.rs` 102, `probe_mp4.rs` 281, `probe_fanout.rs` 415, `scrape.rs` 711, `sources.rs` 153, `indexerconfig.rs` 329, `cinemeta.rs` 149 |
| 6 | Foundations and plumbing | 3,532 | `config.rs` 474, `id.rs` 70, `play.rs` 89, `util.rs` 35, `seal.rs` 145, `ticket.rs` 155, `cache.rs` 107, `diskcache.rs` 348, `linkcheck.rs` 215, `linkmemo.rs` 132, `validate.rs` 174, `hostlimit.rs` 169, `upstreampause.rs` 236, `decisionlog.rs` 187, `metrics.rs` 379, `loglimit.rs` 49, `reqlog.rs` 112 |

Order: **package 6 first lands `src/tests/mock.rs`** (18 lines; every other package waits on it), then
`util`, `config`, `seal`, `cache`. Package 3 and the leaf modules of 5 need nothing else. Packages 1 and 2
need only the frozen types. Package 4 sits on top of everything and is the last to go green.

### Package 1: Stores I (TorBox) - 3,082 test lines

| Go test file | Rust test module | Exercises | Lines |
|---|---|---|---|
| `stores_test.go` | `src/tests/stores.rs` | `stores/torbox.rs` (also the other two stores) | 2,276 |
| `queued_test.go` | `src/tests/queued.rs` | `stores/torbox.rs`; a few go through the handler | 280 |
| `cancel_test.go` | `src/tests/cancel.rs` | `stores/torbox.rs` (`Cancel`) | 272 |
| `account_test.go` | `src/tests/account.rs` | `stores/torbox.rs` (`AccountSummary`) | 157 |
| `token_leak_test.go` | `src/tests/token_leak.rs` | `stores/torbox.rs` (no token in an error) | 97 |

### Package 2: Stores II - 3,420 test lines

| Go test file | Rust test module | Exercises | Lines |
|---|---|---|---|
| `refusal_test.go` | `src/tests/refusal.rs` | `stores/refusal.rs`, `pool.rs`; a third goes through the handler and `diskcache` | 1,563 |
| `stores_pick_test.go` | `src/tests/stores_pick.rs` | file and episode selection in all three stores | 1,211 |
| `stores_failure_test.go` | `src/tests/stores_failure.rs` | `realdebrid.rs`, `premiumize.rs`, failure modes | 352 |
| `stores_preference_test.go` | `src/tests/stores_preference.rs` | `pool.rs` (`ResolvePreferring`) | 294 |

### Package 3: Add budget - 3,782 test lines

| Go test file | Rust test module | Exercises | Lines |
|---|---|---|---|
| `addbudget_test.go` | `src/tests/addbudget.rs` | `addbudget.rs` | 3,782 |

### Package 4: Handler and HTTP surface - 3,734 test lines

| Go test file | Rust test module | Exercises | Lines |
|---|---|---|---|
| `handler_test.go` | `src/tests/handler.rs` | `handler.rs` | 2,429 |
| `availability_test.go` | `src/tests/availability.rs` | `availability.rs` | 300 |
| `playreserve_test.go` | `src/tests/playreserve.rs` | `handler.rs` over `addbudget.rs` | 248 |
| `coverage_test.go` | `src/tests/coverage.rs` | `coverage.rs` | 219 |
| `playwarm_test.go` | `src/tests/playwarm.rs` | `playwarm.rs`, `handler.rs` | 169 |
| `redirect_test.go` | `src/tests/redirect.rs` | `deps.rs` (`RefuseRedirectReplay`) | 132 |
| `deps_test.go` | `src/tests/deps.rs` | `deps.rs` | 128 |
| `remux_test.go` | `src/tests/remux.rs` | `handler.rs` (scoped configs, remux key) | 89 |
| `version_test.go` | `src/tests/version.rs` | `manifest.rs` | 20 |

### Package 5: Ranking, indexers, probing - 3,959 test lines

| Go test file | Rust test module | Exercises | Lines |
|---|---|---|---|
| `probe_fanout_test.go` | `src/tests/probe_fanout.rs` | `probe_fanout.rs` | 618 |
| `indexerconfig_test.go` | `src/tests/indexerconfig.rs` | `indexerconfig.rs` | 429 |
| `extra_test.go` | `src/tests/extra.rs` | `rank.rs`, `cinemeta.rs`, `label.rs` (mixed) | 407 |
| `rank_test.go` | `src/tests/rank.rs` | `rank.rs` | 400 |
| `probe_mp4_test.go` | `src/tests/probe_mp4.rs` | `probe_mp4.rs` | 368 |
| `attributes_web_test.go` | `src/tests/attributes_web.rs` | `attributes.rs`, `probe*.rs` | 305 |
| `scrape_test.go` | `src/tests/scrape.rs` | `scrape.rs` | 267 |
| `shape_test.go` | `src/tests/shape.rs` | `probe.rs`, `probe_mp4.rs`, `client.rs` | 212 |
| `probe_test.go` | `src/tests/probe.rs` | `probe.rs` | 201 |
| `answercache_test.go` | `src/tests/answercache.rs` | `scrape.rs` (`scrapeAllCached`) | 191 |
| `sources_test.go` | `src/tests/sources.rs` | `sources.rs` | 172 |
| `client_test.go` | `src/tests/client.rs` | `client.rs` | 132 |
| `probe_containers_test.go` | `src/tests/probe_containers.rs` | `probe.rs`, `probe_avi.rs` | 101 |
| `rank_show_test.go` | `src/tests/rank_show.rs` | `rank.rs` | 66 |
| `season_bench_test.go` | `src/tests/season_bench.rs` | `season.rs` (benchmarks) | 61 |
| `webready_test.go` | `src/tests/webready.rs` | `client.rs` | 29 |

### Package 6: Foundations and plumbing - 3,532 test lines

| Go test file | Rust test module | Exercises | Lines |
|---|---|---|---|
| `core_test.go` | `src/tests/core.rs` | `config.rs`, `id.rs`, `play.rs`, `cache.rs`; also modules of packages 1 and 5 | 639 |
| `ticket_test.go` | `src/tests/ticket.rs` | `ticket.rs` | 541 |
| `linkcheck_test.go` | `src/tests/linkcheck.rs` | `linkcheck.rs` | 353 |
| `diskcache_test.go` | `src/tests/diskcache.rs` | `diskcache.rs` | 320 |
| `hostlimit_test.go` | `src/tests/hostlimit.rs` | `hostlimit.rs` | 258 |
| `linkmemo_test.go` | `src/tests/linkmemo.rs` | `linkmemo.rs` | 246 |
| `metrics_test.go` | `src/tests/metrics.rs` | `metrics.rs` | 218 |
| `decisionlog_test.go` | `src/tests/decisionlog.rs` | `decisionlog.rs` | 192 |
| `validate_test.go` | `src/tests/validate.rs` | `validate.rs` | 188 |
| `upstreampause_test.go` | `src/tests/upstreampause.rs` | `upstreampause.rs` | 180 |
| `seal_test.go` | `src/tests/seal.rs` | `seal.rs` | 143 |
| `reqlog_test.go` | `src/tests/reqlog.rs` | `reqlog.rs` | 132 |
| `loglimit_test.go` | `src/tests/loglimit.rs` | `loglimit.rs` | 104 |
| `mock_test.go` | `src/tests/mock.rs` | shared test helpers; lands first | 18 |

## Known gaps in the split

- `core_test.go` and `extra_test.go` are grab bags; their tests are assigned to the file's package, not to
  each test's module. Expect some red until the other package lands.
- `refusal_test.go`, `queued_test.go` and `stores_test.go` reach into the handler and the disk cache.
  Their store-level tests are the ones to port and turn green first.
- `configure.html` (embedded by `configure.go`) is not copied. Package 4 brings it across with the module.
