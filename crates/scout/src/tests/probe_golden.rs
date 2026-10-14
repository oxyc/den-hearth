//! Golden outputs of the container parsers over every fixture of both crates (scout's and remux's `testdata`),
//! whole and cut short, so a change to the parsing code underneath them shows up as a diff in
//! `testdata/probe-golden.txt`. Regenerate with `UPDATE_GOLDEN=1 cargo test -p den-scout probe_golden`.
//!
//! The mutation pass runs the same parsers over corrupted copies of the fixtures: it must never panic (overflow
//! checks are on in the test profile), and `FUZZ_ITERS=50000 cargo test -p den-scout -- --ignored --nocapture
//! mutated_heads_digest` prints a digest of every output, which two builds of the parser agree on exactly when they
//! parse the same.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Path;

use crate::probe::{parse_head, PROBE_BYTES};

fn fixtures() -> Vec<(String, Vec<u8>)> {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut out = Vec::new();
    for (label, dir) in [("scout", "testdata"), ("remux", "../remux/testdata")] {
        let mut names: Vec<String> = std::fs::read_dir(manifest.join(dir))
            .expect("testdata")
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| [".mkv", ".mp4", ".avi"].iter().any(|ext| n.ends_with(ext)))
            .collect();
        names.sort();
        for name in names {
            let bytes = std::fs::read(manifest.join(dir).join(&name)).expect("fixture");
            out.push((format!("{label}/{name}"), bytes));
        }
    }
    out
}

fn describe(head: &[u8]) -> String {
    match parse_head(head) {
        Ok(p) => serde_json::to_string(&p).expect("a probe is JSON"),
        Err(e) => format!("err: {e}"),
    }
}

/// What every codec-record reader answers for 40,000 records of every length up to 40 bytes, starting with the
/// bytes a real record starts with, as one number.
fn records_digest() -> u64 {
    use crate::probe::{bit_depth_from_config, dovi_profile, video_shape};

    let mut hasher = DefaultHasher::new();
    let mut rng = Rng(0x2545_F491_4F6C_DD1D);
    for n in 0..40_000usize {
        let mut record: Vec<u8> = (0..n % 41).map(|_| rng.next() as u8).collect();
        if let Some(first) = record.first_mut() {
            *first = [1, 0x81, *first][rng.below(3)];
        }
        if let Some(profile) = record.get_mut(1) {
            *profile = [66, 77, 88, 100, 110, *profile][rng.below(6)];
        }
        for codec in ["h264", "hevc", "av1", "vp9", ""] {
            (codec, &record, bit_depth_from_config(codec, &record), video_shape(codec, &record)).hash(&mut hasher);
        }
        (&record, dovi_profile(&record)).hash(&mut hasher);
    }
    hasher.finish()
}

#[test]
fn every_fixture_probes_as_it_always_did() {
    let mut got = String::new();
    for (name, bytes) in fixtures() {
        for cut in [PROBE_BYTES, 4096, 1000] {
            let head = &bytes[..bytes.len().min(cut)];
            got.push_str(&format!("{name} @{cut}: {}\n", describe(head)));
        }
    }
    got.push_str(&format!("codec records: {:016x}\n", records_digest()));
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/probe-golden.txt");
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, &got).expect("write the golden");
        return;
    }
    let want = std::fs::read_to_string(&path).expect("testdata/probe-golden.txt (UPDATE_GOLDEN=1 writes it)");
    for (g, w) in got.lines().zip(want.lines()) {
        assert_eq!(g, w);
    }
    assert_eq!(got.lines().count(), want.lines().count());
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n.max(1) as u64) as usize
    }
}

/// A copy of `src` with a few corrupted bytes, most of them in the first kilobytes where the headers are, and now
/// and then cut short.
fn mutate(rng: &mut Rng, src: &[u8]) -> Vec<u8> {
    let mut b = src[..src.len().min(PROBE_BYTES)].to_vec();
    for _ in 0..1 + rng.below(4) {
        let window = match rng.below(5) {
            0..=2 => 4096,
            3 => 128 * 1024,
            _ => b.len(),
        };
        let at = rng.below(window.min(b.len()));
        match rng.below(4) {
            0 => b[at] = rng.next() as u8,
            1 => b[at] = [0x00, 0x01, 0x7F, 0x80, 0xFF][rng.below(5)],
            2 => b[at] ^= 1 << rng.below(8),
            _ => {
                // A run of 0xFF: an EBML unknown size, or a box size that overflows.
                let end = (at + 1 + rng.below(8)).min(b.len());
                b[at..end].fill(0xFF);
            }
        }
    }
    if rng.below(8) == 0 {
        let cut = rng.below(b.len());
        b.truncate(cut);
    }
    b
}

fn mutated_digest(iters: usize) -> (u64, usize) {
    let mut hasher = DefaultHasher::new();
    let mut parsed = 0;
    for (i, (name, bytes)) in fixtures().iter().enumerate() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (i as u64 + 1).wrapping_mul(0xD1B5_4A32_D192_ED03));
        for _ in 0..iters {
            let head = mutate(&mut rng, bytes);
            let out = describe(&head);
            parsed += usize::from(!out.starts_with("err"));
            (name, &out).hash(&mut hasher);
        }
    }
    (hasher.finish(), parsed)
}

#[test]
fn mutated_heads_never_panic() {
    let (_, parsed) = mutated_digest(150);
    assert!(parsed > 0, "no mutated head parsed at all, so the pass is not reaching the parsers");
}

#[test]
#[ignore = "a long pass, for comparing two builds"]
fn mutated_heads_digest() {
    let iters = std::env::var("FUZZ_ITERS").ok().and_then(|v| v.parse().ok()).unwrap_or(20_000);
    let (digest, parsed) = mutated_digest(iters);
    eprintln!("scout mutation digest: iters={iters} parsed={parsed} digest={digest:016x}");
}
