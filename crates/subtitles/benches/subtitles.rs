//! den-subtitles hot paths, with allocation counts. Run through `bench/micro.sh`:
//!
//!     cargo bench -p den-subtitles --features bench

use std::time::Duration;

use den_subtitles::bench_api::{self as subs, Subtitle};
use divan::counter::BytesCount;
use divan::{black_box, Bencher};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

/// A feature film's subtitles: `n` cues, one or two lines each, with the markup real files carry.
fn srt(n: usize) -> String {
    let mut out = String::new();
    let mut ms = 5_000u64;
    for i in 1..=n {
        let end = ms + 1_500 + (i as u64 % 7) * 300;
        out.push_str(&format!("{i}\n{} --> {}\n", ts(ms, ','), ts(end, ',')));
        match i % 5 {
            0 => out.push_str("<i>I told you, it was never about the money.</i>\n"),
            1 => out.push_str("- Where are you going?\n- Out.\n"),
            2 => out.push_str("{\\an8}Previously on the show...\n"),
            3 => out.push_str(
                "<font color=\"#ffff00\">Don't &amp; shouldn't <b>ever</b> do that again.</font>\n",
            ),
            _ => out.push_str("Hej då, vi ses i morgon.\n"),
        }
        out.push('\n');
        ms = end + 800 + (i as u64 % 11) * 200;
    }
    out
}

fn ts(ms: u64, sep: char) -> String {
    format!("{:02}:{:02}:{:02}{sep}{:03}", ms / 3_600_000, ms / 60_000 % 60, ms / 1000 % 60, ms % 1000)
}

#[divan::bench(args = [500, 2000])]
fn srt_parse(bencher: Bencher, cues: usize) {
    let text = srt(cues);
    assert_eq!(subs::parse(&text).len(), cues);
    bencher.counter(BytesCount::new(text.len())).bench(|| subs::parse(black_box(&text)));
}

#[divan::bench(args = [500, 2000])]
fn srt_serialize(bencher: Bencher, cues: usize) {
    let parsed = subs::parse(&srt(cues));
    bencher.bench(|| subs::serialize(black_box(&parsed)));
}

#[divan::bench(args = [500, 2000])]
fn srt_serialize_vtt(bencher: Bencher, cues: usize) {
    let parsed = subs::parse(&srt(cues));
    bencher.bench(|| subs::serialize_vtt(black_box(&parsed)));
}

#[divan::bench]
fn srt_has_a_cue(bencher: Bencher) {
    let text = srt(2000);
    bencher.bench(|| subs::has_a_cue(black_box(&text)));
}

#[divan::bench]
fn srt_stats_and_incomplete(bencher: Bencher) {
    let parsed = subs::parse(&srt(2000));
    bencher
        .bench(|| (subs::stats(black_box(&parsed)), subs::looks_incomplete(black_box(&parsed), Some(1800))));
}

/// A download in a legacy encoding: charset detection plus a transcode, on every cache miss.
#[divan::bench(args = ["windows-1252", "utf-8"])]
fn decode_download(bencher: Bencher, encoding: &str) {
    let text = srt(2000);
    let bytes: Vec<u8> = if encoding == "utf-8" {
        text.into_bytes()
    } else {
        text.chars().map(|c| if c == 'å' { 0xE5 } else { c as u8 }).collect()
    };
    bencher
        .counter(BytesCount::new(bytes.len()))
        .with_inputs(|| bytes.clone())
        .bench_local_values(|b| subs::subtitle_text(black_box(b), Some("sv")));
}

fn candidates(n: usize) -> Vec<Subtitle> {
    (0..n)
        .map(|i| Subtitle {
            file_id: i as i64,
            lang: ["en", "sv", "es", "fr"][i % 4].to_owned(),
            hash_match: i % 17 == 0,
            downloads: (i as i64 * 7919) % 100_000,
            release: format!(
                "Dune.Part.Two.2024.{}.WEB-DL.DDP5.1.H.264-GRP{}",
                ["1080p", "2160p", "720p"][i % 3],
                i % 13
            ),
            hd: i % 2 == 0,
            fps: 23.976,
            from_trusted: i % 5 == 0,
            machine_translated: i % 23 == 0,
            ai_translated: i % 29 == 0,
            foreign_parts_only: i % 31 == 0,
            hearing_impaired: i % 6 == 0,
            ratings: (i % 11) as f64,
        })
        .collect()
}

/// Ranking one search result against the playing file's name.
#[divan::bench(args = [20, 100, 400])]
fn rank_candidates(bencher: Bencher, n: usize) {
    let subs_in = candidates(n);
    bencher.with_inputs(|| subs_in.clone()).bench_local_refs(|list| {
        subs::rank(black_box(list), Some("Dune.Part.Two.2024.1080p.WEB-DL.DDP5.1.H.264-GRP3.mkv"))
    });
}

#[divan::bench]
fn manifest(bencher: Bencher) {
    bencher.bench(subs::manifest);
}

#[divan::bench]
fn cache_get_mem_hit(bencher: Bencher) {
    let cache = subs::Cache::new(64 << 20, None);
    let body = srt(2000);
    for i in 0..64 {
        cache.put(format!("dl:{i}"), body.clone(), Duration::from_secs(3600));
    }
    let mut i = 0usize;
    bencher.counter(BytesCount::new(body.len())).bench_local(|| {
        let hit = cache.get(black_box(&format!("dl:{}", i & 63)));
        i += 1;
        hit
    });
}

/// The serve path's read: a refcounted view of the cached body, no copy.
#[divan::bench]
fn cache_get_shared_hit(bencher: Bencher) {
    let cache = subs::Cache::new(64 << 20, None);
    let body = srt(2000);
    for i in 0..64 {
        cache.put(format!("dl:{i}"), body.clone(), Duration::from_secs(3600));
    }
    let mut i = 0usize;
    bencher.counter(BytesCount::new(body.len())).bench_local(|| {
        let hit = cache.get_shared(black_box(&format!("dl:{}", i & 63)));
        i += 1;
        hit
    });
}

/// What a settled-subtitle hit did before bodies were shared: copy the body out of the cache, parse
/// it again for the serve log's cue stats, and hand the copy to the response.
#[divan::bench]
fn serve_hit_copy_and_reparse(bencher: Bencher) {
    let cache = subs::Cache::new(64 << 20, None);
    let body = srt(2000);
    cache.put("dl:0".into(), body.clone(), Duration::from_secs(3600));
    bencher.counter(BytesCount::new(body.len())).bench_local(|| {
        let hit = cache.get(black_box("dl:0")).unwrap();
        let stats = subs::stats(&subs::parse(&hit));
        (stats.map(|s| s.count), subs::srt_response(hit))
    });
}

/// The same hit now: a shared view of the cached body, stats measured once per body, and the
/// response payload sharing the cached allocation.
#[divan::bench]
fn serve_hit_shared(bencher: Bencher) {
    let cache = subs::Cache::new(64 << 20, None);
    let body = srt(2000);
    cache.put("dl:0".into(), body.clone(), Duration::from_secs(3600));
    bencher.counter(BytesCount::new(body.len())).bench_local(|| {
        let hit = cache.get_shared(black_box("dl:0")).unwrap();
        let stats = hit.stats();
        (stats.map(|s| s.count), subs::srt_response_shared(hit.to_bytes(), hit.etag()))
    });
}

/// `serve_hit_shared` with the ETag hashed from the body on every hit, as it was before the validator
/// was kept next to the cached body.
#[divan::bench]
fn serve_hit_shared_rehash(bencher: Bencher) {
    let cache = subs::Cache::new(64 << 20, None);
    let body = srt(2000);
    cache.put("dl:0".into(), body.clone(), Duration::from_secs(3600));
    bencher.counter(BytesCount::new(body.len())).bench_local(|| {
        let hit = cache.get_shared(black_box("dl:0")).unwrap();
        let stats = hit.stats();
        let bytes = hit.to_bytes();
        let etag = subs::etag_of(&bytes);
        (stats.map(|s| s.count), subs::srt_response_shared(bytes, &etag))
    });
}

/// The validator alone, over a feature film's subtitle.
#[divan::bench]
fn etag_of_body(bencher: Bencher) {
    let body = srt(2000);
    bencher.counter(BytesCount::new(body.len())).bench(|| subs::etag_of(black_box(body.as_bytes())));
}

/// A changed body on each put, so the disk tier has a write to do on every iteration.
#[divan::bench]
fn cache_put_disk(bencher: Bencher) {
    let dir = tempfile::tempdir().unwrap();
    // A disk budget the run cannot reach, so it measures the write and not the sweep a burst past the budget starts.
    let cache = subs::Cache::new(1 << 30, Some(dir.path().to_path_buf()));
    let body = srt(2000);
    let mut i = 0usize;
    bencher.counter(BytesCount::new(body.len())).bench_local(|| {
        cache.put(format!("dl:{}", i & 127), black_box(format!("{i}{body}")), Duration::from_secs(3600));
        i += 1;
    });
}

/// What the full 64 MiB memory tier holds: a household's subtitle bodies, or the small markers and counters that
/// outnumber them. (entries, bytes per entry), each about 60 MiB in all.
const FULL: [(usize, usize); 2] = [(900, 70_000), (30_000, 2_000)];

fn full_cache(entries: usize, size: usize) -> subs::Cache {
    let cache = subs::Cache::new(64 << 20, None);
    let value = "x".repeat(size);
    for i in 0..entries {
        cache.put(format!("os:{i}"), value.clone(), Duration::from_secs(3600));
    }
    cache
}

/// A hit in a cache at its budget: 0 is 900 bodies, 1 is 30,000 markers.
#[divan::bench(args = [0, 1])]
fn cache_get_hit_full(bencher: Bencher, shape: usize) {
    let (entries, size) = FULL[shape];
    let cache = full_cache(entries, size);
    let mut x = 0x9E37_79B9usize;
    bencher.bench_local(|| {
        x = x.wrapping_mul(2_654_435_761).wrapping_add(1) % entries;
        cache.get_shared(black_box(&format!("os:{x}")))
    });
}

/// A put into a cache at its budget, so each one evicts: the cost of making room.
#[divan::bench(args = [0, 1])]
fn cache_put_evicting(bencher: Bencher, shape: usize) {
    let (entries, size) = FULL[shape];
    // Ten percent more than fits, so the tier is evicting before the first measured put.
    let entries = entries * 11 / 10;
    let cache = full_cache(entries, size);
    let value = "x".repeat(size);
    let mut i = entries;
    bencher.bench_local(|| {
        cache.put(format!("os:{i}"), black_box(value.clone()), Duration::from_secs(3600));
        i += 1;
    });
}
