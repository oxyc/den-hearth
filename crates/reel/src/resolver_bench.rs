//! Opt-in measurement of `/direct`'s resolvers on real YouTube ids. Every test here is `#[ignore]`d:
//! `cargo test` never runs them and nothing here touches the network unless asked.
//!
//! One process measures ONE configuration, because CPU and peak memory are process-wide numbers.
//! The resolver is whatever production would read from the environment, so the run is the service's
//! own path, not a model of it:
//!
//! ```text
//! # yt-dlp spawned per resolve (what runs when no worker is configured, or the worker has died)
//! REEL_RESOLVER=ytdlp YTDLP_PATH=… CACHE_DIR=… BENCH_IDS=crates/reel/bench/trailer-ids.txt \
//!   BENCH_OUT=ytdlp.jsonl cargo test -p den-reel --release --lib -- --ignored --nocapture resolver_bench
//! # …plus YTDLP_WORKER=crates/reel/worker/resolve.py YTDLP_ZIP=… for the resident worker
//! # …REEL_RESOLVER=rustypipe (add --features rustypipe; REEL_BOTGUARD_BIN=… for PO tokens)
//! # …REEL_RESOLVER=auto
//! BENCH_A=ytdlp.jsonl BENCH_B=rustypipe.jsonl cargo test … -- --ignored --nocapture resolver_compare
//! ```
//!
//! Knobs: `BENCH_IDS` (one id per line, first word; `#` comments), `BENCH_OUT` (JSON lines),
//! `BENCH_LIMIT`, `BENCH_GAP_MS` (pause between resolves, default 1500 — sequential, never parallel),
//! `BENCH_VERIFY=0` to skip the two one-byte range requests per answer that check the URL really
//! serves (its first byte, and its last — `direct::refuses_its_end`).

use std::io::Write;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::config::Config;
use crate::state::AppState;

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

/// (CPU ms, peak RSS bytes) for this process or its reaped children.
fn usage(who: libc::c_int) -> (f64, u64) {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(who, &mut ru) };
    let tv = |t: libc::timeval| t.tv_sec as f64 * 1000.0 + t.tv_usec as f64 / 1000.0;
    // macOS reports bytes, Linux kilobytes.
    let rss = if cfg!(target_os = "macos") { ru.ru_maxrss as u64 } else { ru.ru_maxrss as u64 * 1024 };
    (tv(ru.ru_utime) + tv(ru.ru_stime), rss)
}

fn cpu_now() -> f64 {
    usage(libc::RUSAGE_SELF).0 + usage(libc::RUSAGE_CHILDREN).0
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let rank = ((p / 100.0) * sorted.len() as f64).ceil().max(1.0) as usize;
    sorted[rank.min(sorted.len()) - 1]
}

fn itag(url: &str) -> Option<String> {
    crate::progressive::media_param(url, "itag")
}

/// The status of a one-byte range request at `at` (a fraction of the file's `clen`; 0 = the first
/// byte, 1 = the last). 0 when the request itself failed or the URL names no length.
async fn status_at(http: &reqwest::Client, url: &str, at: f64) -> u16 {
    let Some(clen) = crate::progressive::media_param(url, "clen").and_then(|c| c.parse::<u64>().ok()) else {
        return 0;
    };
    let byte = ((clen.saturating_sub(1)) as f64 * at) as u64;
    let asked = http
        .get(url)
        .header(hyper::header::RANGE.as_str(), format!("bytes={byte}-{byte}"))
        .timeout(Duration::from_secs(8))
        .send()
        .await;
    asked.map(|r| r.status().as_u16()).unwrap_or(0)
}

#[tokio::test]
#[ignore = "network: resolves real YouTube ids; see the module doc"]
async fn resolver_bench() {
    let ids_path = env("BENCH_IDS").expect("BENCH_IDS: a file of YouTube ids");
    let out_path = env("BENCH_OUT").expect("BENCH_OUT: where to write JSON lines");
    let gap = Duration::from_millis(env("BENCH_GAP_MS").and_then(|v| v.parse().ok()).unwrap_or(1500));
    let limit: usize = env("BENCH_LIMIT").and_then(|v| v.parse().ok()).unwrap_or(usize::MAX);
    let verify = env("BENCH_VERIFY").as_deref() != Some("0");

    let ids: Vec<String> = std::fs::read_to_string(&ids_path)
        .expect("read BENCH_IDS")
        .lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .filter_map(|l| l.split_whitespace().next())
        .filter(|id| crate::is_valid_vid(id))
        .take(limit)
        .map(str::to_string)
        .collect();

    let cfg = Config::from_env();
    let (mode, format) = (format!("{:?}", cfg.resolver), cfg.ytdlp_format.clone());
    let worker = cfg.ytdlp_worker.is_some();
    let st: std::sync::Arc<AppState> = AppState::new(cfg);
    let mut out = std::fs::File::create(&out_path).expect("create BENCH_OUT");

    let (mut durs, mut cold, mut ok_n, mut fallbacks) = (Vec::<f64>::new(), None::<f64>, 0usize, 0usize);
    let (mut reasons, mut itags) = (
        std::collections::BTreeMap::<String, usize>::new(),
        std::collections::BTreeMap::<String, usize>::new(),
    );
    let (mut start_bad, mut end_bad) = (0usize, 0usize);
    let cpu_at_start = cpu_now();
    let wall = Instant::now();

    for (i, id) in ids.iter().enumerate() {
        if i > 0 {
            tokio::time::sleep(gap).await;
        }
        let cpu0 = cpu_now();
        let t0 = Instant::now();
        let r = crate::direct::extract(&st, id, &format).await;
        let dur = t0.elapsed().as_secs_f64() * 1000.0;
        let cpu = cpu_now() - cpu0;

        let mut rec = json!({
            "i": i, "id": id, "mode": mode, "worker": worker, "resolver": r.resolver.tag(),
            "dur_ms": dur, "cpu_ms": cpu, "fell_back": r.fallback.is_some(),
            "fallback_reason": r.fallback.as_ref().map(|e| e.reason.clone()),
            "fallback_detail": r.fallback.as_ref().map(|e| e.detail.chars().take(160).collect::<String>()),
        });
        if r.fallback.is_some() {
            fallbacks += 1;
        }
        match &r.answer {
            Ok(d) => {
                ok_n += 1;
                durs.push(dur);
                cold.get_or_insert(dur);
                let (vi, ai) = (itag(&d.video), d.audio.as_deref().and_then(itag));
                *itags
                    .entry(format!("{}+{}", vi.clone().unwrap_or_default(), ai.clone().unwrap_or_default()))
                    .or_default() += 1;
                let (mut start_ok, mut end_ok) = (None, None);
                if verify {
                    let (s, m, e) = (
                        status_at(&st.http, &d.video, 0.0).await,
                        status_at(&st.http, &d.video, 0.5).await,
                        status_at(&st.http, &d.video, 1.0).await,
                    );
                    // The end is judged exactly as production judges it: a 403 and nothing else.
                    let (s_ok, e_ok) = (matches!(s, 200 | 206), e != 403);
                    (start_bad, end_bad) = (start_bad + !s_ok as usize, end_bad + !e_ok as usize);
                    (start_ok, end_ok) = (Some(s_ok), Some(e_ok));
                    let o = rec.as_object_mut().unwrap();
                    o.insert("range_status".into(), json!({"start": s, "mid": m, "end": e}));
                    o.insert("client".into(), json!(crate::progressive::media_param(&d.video, "c")));
                    // Which parameters the URL carries (names only): the difference between two
                    // resolvers' URLs for the same video is usually one of them.
                    let keys: Vec<&str> = d
                        .video
                        .split_once('?')
                        .map(|(_, q)| q)
                        .unwrap_or("")
                        .split('&')
                        .filter_map(|p| p.split('=').next())
                        .collect();
                    o.insert("url_params".into(), json!(keys.join(",")));
                }
                if let Some(path) = env("BENCH_URLS") {
                    let mut f =
                        std::fs::OpenOptions::new().create(true).append(true).open(path).expect("BENCH_URLS");
                    writeln!(f, "{id} {} {}", d.video, d.audio.as_deref().unwrap_or("-")).unwrap();
                }
                let o = rec.as_object_mut().unwrap();
                o.insert("ok".into(), json!(true));
                o.insert("video_itag".into(), json!(vi));
                o.insert("audio_itag".into(), json!(ai));
                o.insert("height".into(), json!(d.height));
                o.insert("has_audio".into(), json!(d.audio.is_some()));
                o.insert("has_hls".into(), json!(d.hls.is_some()));
                o.insert("expires".into(), json!(d.expires / 1000));
                o.insert("start_ok".into(), json!(start_ok));
                o.insert("end_ok".into(), json!(end_ok));
            }
            Err(e) => {
                *reasons.entry(e.reason.clone()).or_default() += 1;
                let o = rec.as_object_mut().unwrap();
                o.insert("ok".into(), json!(false));
                o.insert("reason".into(), json!(e.reason));
                o.insert("detail".into(), json!(e.detail.chars().take(200).collect::<String>()));
            }
        }
        writeln!(out, "{rec}").unwrap();
        eprintln!(
            "[{}/{}] {id} {} {dur:.0} ms",
            i + 1,
            ids.len(),
            if rec["ok"] == true { "ok" } else { "FAIL" }
        );
    }

    // The resident worker is never reaped until it exits, so its CPU and memory only show up in the
    // children's totals once it has been told to go.
    st.worker.shutdown().await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    let (self_cpu, self_rss) = usage(libc::RUSAGE_SELF);
    let (child_cpu, child_rss) = usage(libc::RUSAGE_CHILDREN);
    let n = ids.len().max(1) as f64;
    durs.sort_by(f64::total_cmp);
    // Nothing is measured of the very first answer but its own latency: it pays for the client, the
    // deobfuscation script, or the interpreter, once per process.
    let mut warm = durs.clone();
    if let Some(c) = cold {
        if let Some(p) = warm.iter().position(|d| *d == c) {
            warm.remove(p);
        }
    }
    println!("\n== resolver_bench: REEL_RESOLVER={mode} worker={worker} ==");
    println!(
        "resolves {}  ok {}  success {:.1}%  fell_back {}",
        ids.len(),
        ok_n,
        100.0 * ok_n as f64 / n,
        fallbacks
    );
    println!(
        "latency ms (ok only)  p50 {:.0}  p95 {:.0}  mean {:.0}  first(cold) {:.0}",
        pct(&durs, 50.0),
        pct(&durs, 95.0),
        durs.iter().sum::<f64>() / durs.len().max(1) as f64,
        cold.unwrap_or(f64::NAN)
    );
    println!("latency ms (ok, minus the first)  p50 {:.0}  p95 {:.0}", pct(&warm, 50.0), pct(&warm, 95.0));
    println!(
        "cpu ms per resolve (this process + reaped children, incl. worker)  {:.0}",
        (self_cpu + child_cpu - cpu_at_start) / n
    );
    println!(
        "peak rss  this process {:.1} MiB   largest child {:.1} MiB",
        self_rss as f64 / 1048576.0,
        child_rss as f64 / 1048576.0
    );
    println!(
        "wall {:.0}s over {} resolves incl. {}ms gaps",
        wall.elapsed().as_secs_f64(),
        ids.len(),
        gap.as_millis()
    );
    println!("verify: first byte refused {start_bad}, last byte refused {end_bad} (of {ok_n})");
    println!("failure reasons {reasons:?}");
    println!("video+audio itags {itags:?}");
}

fn read_jsonl(path: &str) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read {path}: {e}"))
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}

/// Two runs over the same ids: where both answered, did they choose the same streams?
#[test]
#[ignore = "reads two BENCH_OUT files; see the module doc"]
fn resolver_compare() {
    let a = read_jsonl(&env("BENCH_A").expect("BENCH_A"));
    let b = read_jsonl(&env("BENCH_B").expect("BENCH_B"));
    let by_id: std::collections::HashMap<&str, &Value> =
        b.iter().filter_map(|r| Some((r["id"].as_str()?, r))).collect();
    let (mut both, mut same_video, mut same_audio, mut same_height) = (0, 0, 0, 0);
    let (mut only_a, mut only_b, mut neither) = (0, 0, 0);
    let mut differ = Vec::new();
    for ra in &a {
        let Some(id) = ra["id"].as_str() else { continue };
        let Some(rb) = by_id.get(id) else { continue };
        match (ra["ok"] == true, rb["ok"] == true) {
            (true, true) => {
                both += 1;
                same_video += (ra["video_itag"] == rb["video_itag"]) as usize;
                same_audio += (ra["audio_itag"] == rb["audio_itag"]) as usize;
                same_height += (ra["height"] == rb["height"]) as usize;
                if ra["video_itag"] != rb["video_itag"] || ra["audio_itag"] != rb["audio_itag"] {
                    differ.push(format!(
                        "{id}: {}+{} vs {}+{}",
                        ra["video_itag"], ra["audio_itag"], rb["video_itag"], rb["audio_itag"]
                    ));
                }
            }
            (true, false) => only_a += 1,
            (false, true) => only_b += 1,
            (false, false) => neither += 1,
        }
    }
    println!("both answered {both}: same video itag {same_video}, same audio itag {same_audio}, same height {same_height}");
    println!("only A answered {only_a}, only B answered {only_b}, neither {neither}");
    for d in differ {
        println!("  differs: {d}");
    }
}
