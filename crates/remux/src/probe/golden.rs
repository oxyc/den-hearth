//! Golden outputs of the probe over every Matroska and MP4 fixture of both crates (remux's and scout's `testdata`),
//! with the whole head and with a short one (which sends the probe to the SeekHead and the `moov` through
//! `Source`), so a change to the parsing code underneath shows up as a diff in `testdata/probe-golden.txt`.
//! Regenerate with `UPDATE_GOLDEN=1 cargo test -p den-remux probe::golden`.
//!
//! The mutation pass runs the probe over corrupted copies of the fixtures: it must never panic (overflow checks are
//! on in the test profile), and `FUZZ_ITERS=50000 cargo test -p den-remux -- --ignored --nocapture
//! mutated_files_digest` prints a digest of every output, which two builds agree on exactly when they probe the
//! same.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Path;

use super::{probe, MediaInfo, ProbeError, Source};

fn fixtures() -> Vec<(String, Vec<u8>)> {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut out = Vec::new();
    for (label, dir) in [("remux", "testdata"), ("scout", "../scout/testdata")] {
        let mut names: Vec<String> = std::fs::read_dir(manifest.join(dir))
            .expect("testdata")
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".mkv") || n.ends_with(".mp4"))
            .collect();
        names.sort();
        for name in names {
            let bytes = std::fs::read(manifest.join(dir).join(&name)).expect("fixture");
            out.push((format!("{label}/{name}"), bytes));
        }
    }
    out
}

fn bits_hash(values: impl Iterator<Item = u64>) -> u64 {
    let mut h = DefaultHasher::new();
    values.for_each(|v| v.hash(&mut h));
    h.finish()
}

/// Everything in the result, the two long lists (keyframes, byte index) as their length and a hash.
fn describe(r: &Result<MediaInfo, ProbeError>) -> String {
    match r {
        Ok(info) => {
            let short = MediaInfo { keyframes: Vec::new(), byte_index: Vec::new(), ..info.clone() };
            let keys = bits_hash(info.keyframes.iter().map(|k| k.to_bits()));
            let index = bits_hash(info.byte_index.iter().flat_map(|(t, b)| [t.to_bits(), *b]));
            format!(
                "{short:?} keyframes={} {keys:016x} byte_index={} {index:016x}",
                info.keyframes.len(),
                info.byte_index.len()
            )
        }
        Err(e) => format!("err {e:?}"),
    }
}

async fn probe_head(bytes: &[u8], head: usize) -> String {
    describe(&probe(&Source::Mem(bytes), &bytes[..head.min(bytes.len())]).await)
}

/// What every codec-record reader answers for 40,000 records of every length up to 40 bytes, starting with the
/// bytes a real record starts with, as one number.
fn records_digest() -> u64 {
    use super::{av1_codecs, av1_track, avc_codecs, dovi_config, hevc_codecs, hevc_track, Colour};

    let mut hasher = DefaultHasher::new();
    let mut rng = Rng(0x2545_F491_4F6C_DD1D);
    let pq = Colour { primaries: 9, transfer: 16, matrix: 9, full_range: Some(false) };
    for n in 0..40_000usize {
        let mut record: Vec<u8> = (0..n % 41).map(|_| rng.next() as u8).collect();
        if let Some(first) = record.first_mut() {
            *first = [1, 0x81, *first][rng.below(3)];
        }
        (
            &record,
            avc_codecs(&record),
            hevc_codecs(&record),
            av1_codecs(&record, Colour::default()),
            av1_codecs(&record, pq),
            av1_track(&record, Colour::default()),
            hevc_track(&record, pq),
        )
            .hash(&mut hasher);
        (&record, dovi_config(&record).map(|d| (d.profile, d.compat, d.level))).hash(&mut hasher);
    }
    hasher.finish()
}

#[tokio::test]
async fn every_fixture_probes_as_it_always_did() {
    let mut got = String::new();
    for (name, bytes) in fixtures() {
        for head in [crate::scout::HEAD_BYTES as usize, 4096] {
            got.push_str(&format!("{name} @{head}: {}\n", probe_head(&bytes, head).await));
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
    let mut b = src.to_vec();
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

/// `FUZZ_DUMP=path` writes one line per mutated file (fixture, number, and its output), so two builds' passes can be
/// diffed to the files they disagree on.
async fn mutated_digest(iters: usize) -> (u64, usize) {
    let mut hasher = DefaultHasher::new();
    let mut probed = 0;
    let mut dump = String::new();
    for (i, (name, bytes)) in fixtures().iter().enumerate() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (i as u64 + 1).wrapping_mul(0xD1B5_4A32_D192_ED03));
        for n in 0..iters {
            let file = mutate(&mut rng, bytes);
            // Alternate the whole head with a short one, which sends the probe back to the source.
            let head = if n % 2 == 0 { crate::scout::HEAD_BYTES as usize } else { 4096 };
            let out = probe_head(&file, head).await;
            probed += usize::from(!out.starts_with("err"));
            (name, &out).hash(&mut hasher);
            if std::env::var_os("FUZZ_DUMP").is_some() {
                dump.push_str(&format!("{name} #{n} {out}\n"));
            }
            if std::env::var("FUZZ_SHOW").is_ok_and(|show| show == format!("{name}#{n}")) {
                let changed: Vec<String> = (0..file.len())
                    .filter(|&at| bytes.get(at) != Some(&file[at]))
                    .map(|at| format!("{at}:{:02x}->{:02x}", bytes.get(at).copied().unwrap_or(0), file[at]))
                    .collect();
                eprintln!("{name}#{n}: len {} of {}, changed {changed:?}\n  {out}", file.len(), bytes.len());
            }
        }
    }
    if let Some(path) = std::env::var_os("FUZZ_DUMP") {
        std::fs::write(path, dump).expect("write the dump");
    }
    (hasher.finish(), probed)
}

#[tokio::test]
async fn mutated_files_never_panic() {
    let (_, probed) = mutated_digest(60).await;
    assert!(probed > 0, "no mutated file probed at all, so the pass is not reaching the parsers");
}

#[tokio::test]
#[ignore = "a long pass, for comparing two builds"]
async fn mutated_files_digest() {
    let iters = std::env::var("FUZZ_ITERS").ok().and_then(|v| v.parse().ok()).unwrap_or(5_000);
    let (digest, probed) = mutated_digest(iters).await;
    eprintln!("remux mutation digest: iters={iters} probed={probed} digest={digest:016x}");
}
