//! Sessions that outlive a restart of the process.
//!
//! A session is written down once, when it is created, as one sealed record in its scratch directory
//! (`s-<sid>/session.rec`): everything its playlists, its segments and its ffmpeg runs were made from — the release's
//! probe, the chosen audio, subtitles and transcode, the playlists themselves, and where the release is fetched again
//! (scout's install and play URL). The install and the play URL are credentials, so the record is sealed with
//! XChaCha20-Poly1305 under a key derived from `REMUX_URL_KEY`, its session id the associated data: it can't be read
//! without the key, nor moved to another session's directory. The session's own signed URL is unchanged — the record
//! holds its expiry, which the signature covers.
//!
//! A request for a session this process doesn't know, whose record opens and whose signature verifies, rebuilds the
//! session from the record and the scratch beside it, with no call to scout: the playlists are the record's, the
//! `init.mp4` and every finished GOP still on disk are served as they are. Each run notes the keyframe it started on
//! (`j<N>/start`), so its GOPs are placed on the timeline exactly as the run that wrote them placed them. The release is
//! fetched again through scout only when a segment that isn't on disk comes near (`Session::serve_segment`), so the
//! wait for it falls inside what the player has buffered.
//!
//! The record goes with the session: ending a session deletes its directory, record first, so an ended session is
//! never rebuilt. A shutdown ends nothing — it stops each session's ffmpeg and keeps its scratch (`AppState::suspend_all`).

use std::io::Read;
use std::path::{Path, PathBuf};

use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};

use crate::session::Gop;

/// The record in a session's scratch directory.
pub const RECORD_FILE: &str = "session.rec";
/// Where a run notes the keyframe it started on, in its own directory.
const JOB_START_FILE: &str = "start";
/// A parser change must never read a record written under other rules as if it were its own.
pub(crate) const RECORD_SCHEMA: u32 = 1;
/// Far above a long film's probe (`state::PROBE_ENTRY_MAX_BYTES` bounds that at 1 MiB), far below anything a corrupt
/// file could make a restart allocate.
const RECORD_MAX_BYTES: usize = 4 * 1024 * 1024;
const NONCE_LEN: usize = 24;

/// Everything a session is rebuilt from.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct Record {
    pub schema: u32,
    pub sid: String,
    pub exp: u64,
    pub owner: String,
    pub public: bool,
    /// Unix seconds it was created: a rebuilt session keeps its age, for its owner's oldest-first eviction and a
    /// guest's usage report.
    pub started_at: u64,
    pub rid: Option<String>,
    pub imdb: String,
    pub label: String,
    pub filename: String,
    pub size: Option<u64>,
    pub pack_size: Option<u64>,
    /// Scout's install (sealed config included) and the release's play URL: credentials, hence the seal.
    pub scout_base: String,
    pub play_url: String,
    /// The install was opened with `REMUX_SCOUT_KEY`, which is taken from the configuration again, never kept here.
    pub service_key: bool,
    pub opened_key: String,
    /// The release's file (`session::file_key`): a link fetched again must lead to this very file.
    pub file_key: Option<String>,
    pub info: crate::state::StoredMediaInfo,
    pub start_at: f64,
    pub audio: usize,
    pub audio_copy: Option<String>,
    pub audio_out: crate::job::AudioOut,
    pub audio_channels: u32,
    /// den-subtitles' install, a credential too.
    pub subtitles: Option<String>,
    pub head_sum: Option<u64>,
    pub renditions: Vec<crate::subs::Rendition>,
    pub dovi: crate::job::Dovi,
    pub transcoded: bool,
    pub transcode_reason: Option<String>,
    pub preset: crate::job::Preset,
    pub max_bitrate: Option<u64>,
    pub need: Option<crate::session::Need>,
    /// The playlists the player holds, served again byte for byte.
    pub master: String,
    pub media: String,
}

fn cipher(url_key: &[u8]) -> XChaCha20Poly1305 {
    XChaCha20Poly1305::new(&crate::auth::record_key(url_key).into())
}

/// `nonce || XChaCha20-Poly1305(record)`, the session id authenticated beside it.
pub(crate) fn seal(url_key: &[u8], rec: &Record) -> Vec<u8> {
    let json = serde_json::to_vec(rec).expect("a record serialises");
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let sealed = cipher(url_key)
        .encrypt(&nonce, Payload { msg: &json, aad: rec.sid.as_bytes() })
        .expect("XChaCha20-Poly1305 seals any payload");
    let mut out = nonce.to_vec();
    out.extend(sealed);
    out
}

/// The record `bytes` seal for `sid`, if they open under this key and describe a release that passes the probe
/// cache's own bounds.
pub(crate) fn open(url_key: &[u8], sid: &str, bytes: &[u8]) -> Option<Record> {
    if bytes.len() <= NONCE_LEN || bytes.len() > RECORD_MAX_BYTES {
        return None;
    }
    let (nonce, sealed) = bytes.split_at(NONCE_LEN);
    let json = cipher(url_key)
        .decrypt(XNonce::from_slice(nonce), Payload { msg: sealed, aad: sid.as_bytes() })
        .ok()?;
    let rec: Record = serde_json::from_slice(&json).ok()?;
    (rec.schema == RECORD_SCHEMA && rec.sid == sid && rec.info.valid()).then_some(rec)
}

/// Write the record into the session's directory, whole, through a rename.
pub(crate) fn write(dir: &Path, url_key: &[u8], rec: &Record) -> std::io::Result<()> {
    let path = dir.join(RECORD_FILE);
    let tmp = dir.join(format!("{RECORD_FILE}.tmp"));
    std::fs::write(&tmp, seal(url_key, rec))?;
    std::fs::rename(&tmp, &path)
}

/// The record in `dir`, if there is one that opens for `sid`.
pub(crate) fn read(dir: &Path, url_key: &[u8], sid: &str) -> Option<Record> {
    let file = std::fs::File::open(dir.join(RECORD_FILE)).ok()?;
    let mut bytes = Vec::new();
    file.take(RECORD_MAX_BYTES as u64 + 1).read_to_end(&mut bytes).ok()?;
    open(url_key, sid, &bytes)
}

/// Delete a session's scratch, its record first: whatever else is left behind, the session can't come back.
pub(crate) fn remove(dir: &Path) {
    forget(dir);
    let _ = std::fs::remove_dir_all(dir);
}

/// Delete the record alone: the session can't come back, whatever of its scratch is still to be deleted.
pub(crate) fn forget(dir: &Path) {
    let _ = std::fs::remove_file(dir.join(RECORD_FILE));
}

/// Mark the record as written now, at a suspension, so startup tells a session suspended moments ago from one a crash
/// left long ago (`sweep`).
pub(crate) fn touch(dir: &Path) {
    let record = std::fs::OpenOptions::new().write(true).open(dir.join(RECORD_FILE));
    if let Err(e) = record.and_then(|f| f.set_modified(std::time::SystemTime::now())) {
        crate::log_limited("record_touch", || format!("scratch: a suspended session's record: {e}"));
    }
}

/// Note the keyframe run `dir` starts on, for `adopt` after a restart.
pub(crate) fn note_job_start(dir: &Path, start: f64) {
    // `f64`'s Display reads back to the same bits.
    if let Err(e) = std::fs::write(dir.join(JOB_START_FILE), start.to_string()) {
        crate::log_limited("job_start_note", || format!("scratch: a run's start could not be noted: {e}"));
    }
}

/// One earlier run whose files are still in scratch.
pub(crate) struct AdoptedRun {
    pub id: u32,
    pub dir: PathBuf,
    pub start: f64,
    /// How far it read the film: its release's own subtitle files cover `start..upto`.
    pub upto: f64,
}

/// What the scratch of a session from an earlier process still holds.
#[derive(Default)]
pub(crate) struct Adopted {
    pub gops: Vec<Gop>,
    pub finished: Vec<u32>,
    pub runs: Vec<AdoptedRun>,
    pub next_job: u32,
    pub init: Option<PathBuf>,
    pub bytes: u64,
}

/// Take up the finished GOPs every earlier run left in `dir`, placed as the run itself placed them: from the keyframe it
/// noted it started on, each GOP snapped to the release's keyframes. A run with no start noted is deleted.
pub(crate) fn adopt(dir: &Path, keyframes: &[f64], duration: f64) -> Adopted {
    let mut a = Adopted::default();
    let init = dir.join("init.mp4");
    if init.is_file() {
        a.init = Some(init);
    }
    let Ok(rd) = std::fs::read_dir(dir) else { return a };
    for e in rd.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        let Some(id) = name.strip_prefix('j').and_then(|n| n.parse::<u32>().ok()) else { continue };
        let run = e.path();
        a.next_job = a.next_job.max(id + 1);
        let start = std::fs::read_to_string(run.join(JOB_START_FILE))
            .ok()
            .and_then(|s| s.trim().parse::<f64>().ok())
            .filter(|s| s.is_finite() && *s >= 0.0 && *s <= duration);
        let Some(start) = start else {
            let _ = std::fs::remove_dir_all(&run);
            continue;
        };
        let text = std::fs::read_to_string(run.join("gops.m3u8")).unwrap_or_default();
        let finished = text.contains("#EXT-X-ENDLIST");
        let mut next = start;
        for (idx, (file, dur)) in crate::job::parse_gops(&text).into_iter().enumerate() {
            let at = crate::session::snap(keyframes, next);
            next = at + dur;
            let path = run.join(&file);
            // A GOP pruned behind the player is listed but gone; the run's later ones keep their places.
            let Ok(meta) = std::fs::metadata(&path) else { continue };
            a.bytes += meta.len();
            a.gops.push(Gop { job: id, idx: idx as u32, start: at, dur, path, size: meta.len() });
        }
        if finished {
            a.finished.push(id);
        }
        let upto = if finished { duration } else { next - crate::subs::TRAIL };
        a.runs.push(AdoptedRun { id, dir: run, start, upto });
    }
    a.gops.sort_by(|x, y| x.start.total_cmp(&y.start));
    a
}

/// A session an earlier process suspended, as startup found it (`sweep`).
#[derive(Clone, Debug)]
pub(crate) struct Kept {
    /// Whom it counts against, so a revocation reaches it before it is resumed (`AppState::end_owner`).
    pub owner: String,
    /// What its URLs were signed with, for its tombstone.
    pub exp: u64,
    /// What its directory holds, counted against `SCRATCH_MAX_BYTES` until it is resumed or deleted.
    pub bytes: u64,
}

/// Every file's bytes in a session's directory and its runs' directories, and the newest modification among them.
fn measure(dir: &Path) -> (u64, Option<std::time::SystemTime>) {
    let (mut bytes, mut newest) = (0, None);
    let mut walk = |d: &Path, deeper: &mut Vec<PathBuf>| {
        for e in std::fs::read_dir(d).into_iter().flatten().flatten() {
            let Ok(meta) = e.metadata() else { continue };
            if meta.is_dir() {
                deeper.push(e.path());
            } else {
                bytes += meta.len();
                newest = newest.max(meta.modified().ok());
            }
        }
    };
    let mut runs = Vec::new();
    walk(dir, &mut runs);
    for run in runs {
        walk(&run, &mut Vec::new());
    }
    (bytes, newest)
}

/// At startup: keep the scratch of every session a record still vouches for — one that opens under this key, has
/// not expired, and was written to (a run's GOP, or the record touched at suspension) within `idle` — and delete the
/// rest. A process that crashed has suspended nothing, so a crash loop doesn't keep a session's scratch until its
/// expiry. The sessions kept, by sid.
pub(crate) fn sweep(
    scratch: &Path,
    url_key: &[u8],
    idle: std::time::Duration,
) -> std::collections::HashMap<String, Kept> {
    let mut kept = std::collections::HashMap::new();
    let Ok(rd) = std::fs::read_dir(scratch) else { return kept };
    let (now, mut removed) = (crate::state::unix_now(), 0);
    for e in rd.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        let Some(sid) = name.strip_prefix("s-") else { continue };
        let rec =
            crate::auth::is_id(sid).then(|| read(&e.path(), url_key, sid)).flatten().filter(|r| r.exp > now);
        let (bytes, newest) = measure(&e.path());
        let recent = newest.and_then(|t| t.elapsed().ok()).is_some_and(|age| age < idle);
        match rec {
            Some(r) if recent => {
                kept.insert(sid.to_string(), Kept { owner: r.owner, exp: r.exp, bytes });
            }
            _ => {
                remove(&e.path());
                removed += 1;
            }
        }
    }
    if removed > 0 {
        eprintln!("scratch: removed {removed} session dir(s) left by the previous process");
    }
    if !kept.is_empty() {
        eprintln!("scratch: kept {} session(s) of the previous process to resume", kept.len());
    }
    kept
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(sid: &str) -> Record {
        let info = crate::probe::MediaInfo {
            container: "matroska",
            duration: 30.0,
            video: crate::probe::VideoCodec::H264,
            codecs: Some("avc1.640028".into()),
            width: 320,
            height: 180,
            hdr: false,
            hlg: false,
            frame_rate: None,
            dolby_vision: None,
            dolby_vision_record_mismatch: false,
            dolby_vision_recordless: false,
            audio: Vec::new(),
            subtitles: Vec::new(),
            keyframes: vec![0.0, 2.5, 5.0, 8.0],
            closed_gops: true,
            byte_index: Vec::new(),
            video_bytes: None,
        };
        Record {
            schema: RECORD_SCHEMA,
            sid: sid.into(),
            exp: 2_000_000_000,
            owner: "install:0011223344556677".into(),
            public: false,
            started_at: 1_000,
            rid: None,
            imdb: "tt0000001".into(),
            label: "x".into(),
            filename: "x.mkv".into(),
            size: Some(1),
            pack_size: None,
            scout_base: "http://scout.lan/SECRETCONFIG".into(),
            play_url: "http://scout.lan/p/SECRETTICKET".into(),
            service_key: false,
            opened_key: "k".into(),
            file_key: None,
            info: crate::state::StoredMediaInfo::from(&info),
            start_at: 0.0,
            audio: 0,
            audio_copy: None,
            audio_out: crate::job::AudioOut::Stereo,
            audio_channels: 2,
            subtitles: None,
            head_sum: None,
            renditions: Vec::new(),
            dovi: crate::job::Dovi::Absent,
            transcoded: false,
            transcode_reason: None,
            preset: crate::job::HD1080,
            max_bitrate: None,
            need: None,
            master: "#EXTM3U".into(),
            media: "#EXTM3U".into(),
        }
    }

    #[test]
    fn a_record_is_sealed_for_its_session_and_key_alone() {
        let sid = crate::auth::random_id();
        let sealed = seal(b"key", &record(&sid));
        let text = String::from_utf8_lossy(&sealed);
        assert!(!text.contains("SECRET"), "the install and the ticket are not readable on disk");
        assert_eq!(open(b"key", &sid, &sealed).map(|r| r.exp), Some(2_000_000_000));
        assert!(open(b"another key", &sid, &sealed).is_none(), "the key is covered");
        assert!(open(b"key", &crate::auth::random_id(), &sealed).is_none(), "moved to another session");
        let mut tampered = sealed.clone();
        *tampered.last_mut().unwrap() ^= 1;
        assert!(open(b"key", &sid, &tampered).is_none(), "a changed byte");
        assert!(open(b"key", &sid, &sealed[..NONCE_LEN]).is_none(), "truncated");
    }

    #[test]
    fn startup_keeps_only_live_records_and_runs_are_placed_where_they_started() {
        let scratch = std::env::temp_dir().join(format!("den-remux-resume-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&scratch);
        let (live, expired, stale) =
            (crate::auth::random_id(), crate::auth::random_id(), crate::auth::random_id());
        for (sid, exp) in [(&live, 2_000_000_000), (&expired, 10), (&stale, 2_000_000_000)] {
            let dir = scratch.join(format!("s-{sid}"));
            std::fs::create_dir_all(&dir).unwrap();
            write(&dir, b"key", &Record { exp, ..record(sid) }).unwrap();
        }
        // Unexpired, but nothing written for longer than a session sits idle: what a crash loop leaves.
        let old = std::time::SystemTime::now() - std::time::Duration::from_secs(3600);
        std::fs::File::options()
            .write(true)
            .open(scratch.join(format!("s-{stale}")).join(RECORD_FILE))
            .unwrap()
            .set_modified(old)
            .unwrap();
        std::fs::create_dir_all(scratch.join("s-norecord")).unwrap();
        // A run from 2.5 s with three GOPs, the first pruned; and one that never noted its start.
        let dir = scratch.join(format!("s-{live}"));
        let run = dir.join("j3");
        std::fs::create_dir_all(&run).unwrap();
        note_job_start(&run, 2.5);
        std::fs::write(
            run.join("gops.m3u8"),
            "#EXTM3U\n#EXTINF:2.5,\ng0.m4s\n#EXTINF:3.0,\ng1.m4s\n#EXTINF:2,\ng2.m4s\n",
        )
        .unwrap();
        std::fs::write(run.join("g1.m4s"), b"one").unwrap();
        std::fs::write(run.join("g2.m4s"), b"two!").unwrap();
        std::fs::create_dir_all(dir.join("j7")).unwrap();

        let kept = sweep(&scratch, b"key", std::time::Duration::from_secs(600));
        assert_eq!(kept.keys().collect::<Vec<_>>(), vec![&live]);
        for gone in [format!("s-{expired}"), format!("s-{stale}"), "s-norecord".into()] {
            assert!(!scratch.join(&gone).exists(), "{gone} is deleted");
        }
        let k = &kept[&live];
        assert_eq!(k.owner, "install:0011223344556677");
        let on_disk = |p: PathBuf| std::fs::metadata(p).unwrap().len();
        let files = [
            dir.join(RECORD_FILE),
            run.join("start"),
            run.join("gops.m3u8"),
            run.join("g1.m4s"),
            run.join("g2.m4s"),
        ];
        assert_eq!(k.bytes, files.into_iter().map(on_disk).sum::<u64>(), "its scratch is counted");

        let a = adopt(&dir, &[0.0, 2.5, 5.0, 8.0], 30.0);
        assert_eq!(a.next_job, 8);
        assert!(!dir.join("j7").exists(), "a run with no start noted can't be placed");
        let placed: Vec<_> = a.gops.iter().map(|g| (g.job, g.idx, g.start, g.size)).collect();
        assert_eq!(placed, vec![(3, 1, 5.0, 3), (3, 2, 8.0, 4)]);
        assert_eq!(a.bytes, 7);
        assert!(a.finished.is_empty() && a.init.is_none());
        let _ = std::fs::remove_dir_all(&scratch);
    }
}
