//! den-remux hot paths, with allocation counts. Run through `bench/micro.sh`:
//!
//!     cargo bench -p den-remux --features bench

use den_remux::bench_api::{self as remux, Audio};
use divan::counter::BytesCount;
use divan::{black_box, Bencher};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

/// Keyframe times of a feature film: a GOP of about 2.4 s with the jitter a real encode has.
fn keyframes(duration_s: f64) -> Vec<f64> {
    let mut out = Vec::new();
    let mut t = 0.0;
    let mut i = 0u32;
    while t < duration_s {
        out.push(t);
        t += 2.0 + f64::from(i % 5) * 0.25;
        i += 1;
    }
    out
}

#[divan::bench(args = [45 * 60, 120 * 60, 240 * 60])]
fn playlist_segments(bencher: Bencher, seconds: usize) {
    let kf = keyframes(seconds as f64);
    bencher.bench(|| remux::segments(black_box(&kf), seconds as f64, 6.0));
}

#[divan::bench]
fn playlist_segments_for_start(bencher: Bencher) {
    let kf = keyframes(7200.0);
    bencher.bench(|| remux::segments_for_start(black_box(&kf), 7200.0, 6.0, 3600.0));
}

#[divan::bench(args = [45 * 60, 120 * 60, 240 * 60])]
fn playlist_media(bencher: Bencher, seconds: usize) {
    let segs = remux::segments(&keyframes(seconds as f64), seconds as f64, 6.0);
    bencher.bench(|| remux::media(black_box(&segs), 1800.0, true));
}

#[divan::bench]
fn playlist_subtitle_segments(bencher: Bencher) {
    let segs = remux::segments(&keyframes(7200.0), 7200.0, 6.0);
    bencher.bench(|| remux::subtitle_segments(black_box(&segs), 0));
}

#[divan::bench]
fn playlist_master(bencher: Bencher) {
    let subs: Vec<(String, String)> = ["en", "sv", "es", "fr", "de", "fi"]
        .iter()
        .map(|l| ((*l).to_owned(), format!("Subtitles {l}")))
        .collect();
    let (bw, avg) = remux::bandwidth(Some(12 << 30), 7200.0);
    bencher.bench(|| {
        remux::master(
            "hvc1.2.4.L153.B0",
            None,
            &Audio::Copy { codec: "ec-3", channels: 6, language: Some("en") },
            bw,
            avg,
            Some((3840, 2160)),
            Some(23.976),
            black_box(&subs),
            true,
        )
    });
}

/// A fragment of WebVTT as ffmpeg's muxer writes it while a session is running.
fn vtt(cues: usize) -> String {
    let mut out = String::from("WEBVTT\n\n");
    for i in 0..cues {
        let s = 10 + i * 4;
        out.push_str(&format!(
            "{:02}:{:02}:{:02}.000 --> {:02}:{:02}:{:02}.500\n<i>Cue {i}: the line a person reads</i>\nsecond line\n\n",
            s / 3600,
            s / 60 % 60,
            s % 60,
            (s + 3) / 3600,
            (s + 3) / 60 % 60,
            (s + 3) % 60
        ));
    }
    out
}

#[divan::bench(args = [200, 1500])]
fn subs_parse_cues(bencher: Bencher, cues: usize) {
    let text = vtt(cues);
    assert_eq!(remux::parse_cues(&text).len(), cues);
    bencher.counter(BytesCount::new(text.len())).bench(|| remux::parse_cues(black_box(&text)));
}

#[divan::bench(args = [200, 1500])]
fn subs_for_hls(bencher: Bencher, cues: usize) {
    let text = vtt(cues).into_bytes();
    bencher.counter(BytesCount::new(text.len())).bench(|| remux::for_hls(black_box(&text)));
}

/// Two fingerprints a scan window long (the intro window caps at 360 s, about 2900 points) sharing a 25 s run.
#[divan::bench]
fn segments_longest_shared(bencher: Bencher) {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        (state >> 16) as u32
    };
    let a: Vec<u32> = (0..2900).map(|_| next()).collect();
    let mut b: Vec<u32> = (0..2900).map(|_| next()).collect();
    b[600..800].copy_from_slice(&a[300..500]);
    assert!(remux::longest_shared(&a, &b, 15.0).is_some());
    bencher.bench(|| remux::longest_shared(black_box(&a), black_box(&b), 15.0));
}

#[divan::bench]
fn auth_url_sig(bencher: Bencher) {
    let key = [7u8; 32];
    bencher.bench(|| remux::url_sig(black_box(&key), "0123456789abcdef0123456789abcdef", 1_800_000_000));
}

#[divan::bench]
fn auth_url_sig_ok(bencher: Bencher) {
    let key = [7u8; 32];
    let sig = remux::url_sig(&key, "0123456789abcdef0123456789abcdef", 1_800_000_000);
    bencher.bench(|| {
        remux::url_sig_ok(black_box(&key), "0123456789abcdef0123456789abcdef", 1_800_000_000, black_box(&sig))
    });
}
