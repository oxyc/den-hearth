//! den-reel hot paths, with allocation counts. Run through `bench/micro.sh`:
//!
//!     cargo bench -p den-reel --features bench

use den_reel::bench_api::{self as reel, Binding, Signer};
use divan::counter::BytesCount;
use divan::{black_box, Bencher};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

#[divan::bench]
fn manifest(bencher: Bencher) {
    bencher.bench(|| reel::manifest(black_box(Some("AAECAwQFBgcICQoLDA0ODw"))));
}

fn youtube_ids(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("dQw4w9{i:05}")).collect()
}

/// The `/meta` answer, unsigned (no PLAY_SECRET) and signed for an install: what every trailer request builds.
#[divan::bench(args = [1, 4, 8])]
fn build_meta_unsigned(bencher: Bencher, ids: usize) {
    let ids = youtube_ids(ids);
    bencher.bench(|| {
        reel::build_meta(
            "movie",
            "tt0137523",
            "https://reel.example",
            black_box(&ids),
            None,
            Binding::Unbound,
        )
        .to_string()
    });
}

#[divan::bench(args = [1, 4, 8])]
fn build_meta_signed(bencher: Bencher, ids: usize) {
    let ids = youtube_ids(ids);
    let binding = Binding::Install { iid: Some("AAECAwQFBgcICQoLDA0ODw"), ep: 3 };
    bencher.bench(|| {
        reel::build_meta(
            "movie",
            "tt0137523",
            "https://reel.example",
            black_box(&ids),
            Some("play-secret-0123456789"),
            binding,
        )
        .to_string()
    });
}

#[divan::bench]
fn sign_tag_deriving_the_key(bencher: Bencher) {
    let msg = reel::message("dQw4w9WgXcQ", Binding::Unbound);
    bencher.bench(|| reel::tag(black_box("play-secret-0123456789"), black_box(&msg)));
}

#[divan::bench]
fn sign_tag_with_signer(bencher: Bencher) {
    let signer = Signer::new("play-secret-0123456789");
    let msg = reel::message("dQw4w9WgXcQ", Binding::Unbound);
    bencher.bench(|| signer.tag(black_box(&msg)));
}

#[divan::bench]
fn sign_verify(bencher: Bencher) {
    let msg = reel::message("dQw4w9WgXcQ", Binding::Unbound);
    let tag = reel::tag("play-secret-0123456789", &msg);
    assert!(reel::verify("play-secret-0123456789", &msg, Some(&tag)));
    bencher
        .bench(|| reel::verify(black_box("play-secret-0123456789"), black_box(&msg), Some(black_box(&tag))));
}

/// ffmpeg's cropdetect stderr over a trailer: one line per analysed keyframe, among the rest of ffmpeg's chatter.
fn cropdetect_stderr(lines: usize) -> String {
    let mut out =
        String::from("Input #0, mov,mp4, from 'x.mp4':\n  Stream #0:0: Video: h264, yuv420p, 1920x1080\n");
    for i in 0..lines {
        out.push_str(&format!(
            "[Parsed_cropdetect_0 @ 0x6000] x1:0 x2:1919 y1:138 y2:941 w:1920 h:800 x:0 y:140 pts:{} t:{}.000 limit:0.094118 crop=1920:800:0:140\n",
            i * 1000,
            i
        ));
    }
    out
}

#[divan::bench(args = [60, 600])]
fn crop_parse_all(bencher: Bencher, lines: usize) {
    let text = cropdetect_stderr(lines);
    assert_eq!(reel::parse_all_crops(&text).len(), lines);
    bencher.counter(BytesCount::new(text.len())).bench(|| reel::parse_all_crops(black_box(&text)));
}

#[divan::bench(args = [60, 600])]
fn crop_typical(bencher: Bencher, lines: usize) {
    let boxes = reel::parse_all_crops(&cropdetect_stderr(lines));
    bencher.bench(|| reel::typical_crop(black_box(&boxes)));
}
