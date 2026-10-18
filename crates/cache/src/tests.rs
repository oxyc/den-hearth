use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tempfile::TempDir;

use super::*;

type C = Cache<Arc<str>>;

#[derive(Clone)]
struct BlockingValue(Arc<str>);

static READBACK_HOOK: Mutex<Option<(SyncSender<()>, Receiver<()>)>> = Mutex::new(None);
static BLOCKING_VALUE_TEST: Mutex<()> = Mutex::new(());

impl Value for BlockingValue {
    fn from_str(text: &str) -> Self {
        let hook = READBACK_HOOK.lock().unwrap().take();
        if let Some((started, resume)) = hook {
            started.send(()).unwrap();
            resume.recv().unwrap();
        }
        BlockingValue(Arc::from(text))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

const DAY: Duration = Duration::from_secs(24 * 3600);

/// One Tokio worker is enough to prove the handoff: if `filesystem` occupied it, the second task could not run and
/// release the first. The operation stays on the original task/thread instead of entering a queue of owned closures.
#[test]
fn filesystem_work_lends_tokio_its_core_worker() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
        let (resume_tx, resume_rx) = std::sync::mpsc::sync_channel(0);
        let blocked = tokio::spawn(async move {
            filesystem(|| {
                started_tx.send(()).unwrap();
                resume_rx.recv().unwrap();
            });
        });

        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let (progress_tx, progress_rx) = std::sync::mpsc::sync_channel(0);
        // This task runs on the replacement core worker. Without the handoff, the one worker is parked in recv and it
        // cannot report progress. Keep a rescue sender so a failed assertion does not leave the runtime hung.
        let rescue_tx = resume_tx.clone();
        tokio::spawn(async move {
            progress_tx.send(()).unwrap();
            resume_tx.send(()).unwrap();
        });
        if progress_rx.recv_timeout(Duration::from_secs(1)).is_err() {
            let _ = rescue_tx.send(());
            panic!("filesystem work occupied the only Tokio core worker");
        }
        blocked.await.unwrap();
    });
}

/// A second ordered operation must enter the blocking handoff before it waits for the first operation's stripe. With
/// one core worker, acquiring the commit lock first would park the replacement core and prevent `progress` from ever
/// running. The final assertions also cover the read-before-put ordering protected by that same lock.
#[test]
fn a_waiting_same_key_put_does_not_occupy_the_tokio_core() {
    let _serial = BLOCKING_VALUE_TEST.lock().unwrap_or_else(PoisonError::into_inner);
    let dir = TempDir::new().unwrap();
    let key = "tokio-waiting-same-key";
    put(&open(dir.path()), key, "old", DAY);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(4)
        .build()
        .unwrap();
    runtime.block_on(async {
        let cache = Arc::new(Cache::<BlockingValue>::new(options(1 << 20, dir.path())));
        let (read_started_tx, read_started_rx) = sync_channel(0);
        let (read_resume_tx, read_resume_rx) = sync_channel(0);
        *READBACK_HOOK.lock().unwrap() = Some((read_started_tx, read_resume_rx));

        let reader_cache = Arc::clone(&cache);
        let reader = tokio::spawn(async move { reader_cache.get(key).unwrap().as_str().to_owned() });
        read_started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let (wait_started_tx, wait_started_rx) = sync_channel(0);
        *COMMIT_WAIT_HOOK.lock().unwrap() = Some((key.to_owned(), wait_started_tx));
        let writer_cache = Arc::clone(&cache);
        let writer = tokio::spawn(async move {
            writer_cache.put(key, BlockingValue(Arc::from("new")), 2 * DAY, Class::Durable);
        });
        wait_started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let (progress_tx, progress_rx) = sync_channel(0);
        tokio::spawn(async move {
            progress_tx.send(()).unwrap();
        });
        let progressed = progress_rx.recv_timeout(Duration::from_secs(1)).is_ok();
        read_resume_tx.send(()).unwrap();
        let read = reader.await.unwrap();
        writer.await.unwrap();

        assert!(progressed, "a request waiting for the commit lock occupied the only Tokio core worker");
        assert_eq!(read, "old");
        assert_eq!(cache.get(key).unwrap().as_str(), "new");
        assert_eq!(get(&open(dir.path()), key).as_deref(), Some("new"));
    });
}

#[test]
fn hot_memory_hits_never_enter_the_filesystem_path() {
    let dir = TempDir::new().unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(1).build().unwrap();
    runtime.block_on(async {
        let cache = open(dir.path());
        put(&cache, "hot", "value", DAY);
        let after_put = FILESYSTEM_CALLS.get();

        assert_eq!(get(&cache, "hot").as_deref(), Some("value"));
        assert_eq!(FILESYSTEM_CALLS.get(), after_put, "a hot hit entered the disk handoff");

        cache.remove("hot");
        assert_eq!(FILESYSTEM_CALLS.get(), after_put + 1, "a disk removal bypassed the handoff");
    });
}

#[test]
fn current_thread_runtime_keeps_the_direct_disk_path() {
    let dir = TempDir::new().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
    runtime.block_on(async {
        let cache = open(dir.path());
        put(&cache, "k", "value", DAY);
        assert_eq!(get(&cache, "k").as_deref(), Some("value"));
        cache.remove("k");
        assert_eq!(get(&cache, "k"), None);
    });
}

fn quiet(_: &str) {}

fn options(mem: usize, dir: &Path) -> Options {
    Options { dir: Some(dir.to_path_buf()), log: quiet, ..Options::new(mem) }
}

fn open(dir: &Path) -> C {
    C::new(options(1 << 20, dir))
}

fn put(c: &C, key: &str, value: &str, ttl: Duration) {
    c.put(key, Arc::from(value), ttl, Class::Durable);
}

fn get(c: &C, key: &str) -> Option<String> {
    c.get(key).map(|v| v.to_string())
}

fn files(dir: &Path) -> usize {
    fs::read_dir(dir).unwrap().count()
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

/// A clock the test moves.
fn clock(start: SystemTime) -> (Clock, Arc<AtomicU64>) {
    let offset = Arc::new(AtomicU64::new(0));
    let o = Arc::clone(&offset);
    (Arc::new(move || start + Duration::from_secs(o.load(Ordering::Relaxed))), offset)
}

// ---- the files --------------------------------------------------------------------------------------------------

#[test]
fn a_written_entry_carries_its_length_and_survives_a_restart() {
    let dir = TempDir::new().unwrap();
    let c = open(dir.path());
    put(&c, "k", "value", DAY);
    let raw = fs::read_to_string(c.path_for("k").unwrap()).unwrap();
    let (head, body) = raw.split_once('\n').unwrap();
    let (expiry, len) = head.split_once(' ').expect("a framed header");
    assert!(expiry.parse::<u64>().unwrap() > now_secs());
    assert_eq!((len, body), ("5", "value"));
    assert_eq!(get(&open(dir.path()), "k").as_deref(), Some("value"));
}

#[test]
fn names_are_the_ones_the_services_already_wrote() {
    let dir = TempDir::new().unwrap();
    let hashed = open(dir.path());
    assert_eq!(
        hashed.path_for("probe:v1:abc").unwrap().file_name().unwrap(),
        "IrrFwxQaIC1ssrurL97CDVMz31-BnvStmiJBy7o7rJQ.ent"
    );
    let readable = C::new(Options { naming: Naming::Readable, ..options(1 << 20, dir.path()) });
    let name = |key: &str| readable.path_for(key).unwrap().file_name().unwrap().to_str().unwrap().to_owned();
    assert_eq!(name("search:tt0111161:0:0:"), "c2VhcmNoOnR0MDExMTE2MTowOjA6");
    // Past 180 bytes: the base64 of the first 32 characters, `~`, and the FNV-1a-64 of the whole key.
    let long = format!("translate:{}", "x".repeat(200));
    assert_eq!(name(&long), "dHJhbnNsYXRlOnh4eHh4eHh4eHh4eHh4eHh4eHh4eHg~e507b7a985852a7d");
}

#[test]
fn a_file_in_the_older_unframed_format_still_loads() {
    let dir = TempDir::new().unwrap();
    let path = open(dir.path()).path_for("probe:v1:abc").unwrap();
    fs::write(&path, format!("{}\n{{\"audio\":[\"eng\"]}}", now_secs() + 3600)).unwrap();

    let c = open(dir.path());
    assert_eq!(get(&c, "probe:v1:abc").as_deref(), Some(r#"{"audio":["eng"]}"#), "an old entry was stranded");
    // Rewritten like any other: a day asked for against an hour left on the file is a write, after which the same
    // put is not. The rewrite is framed.
    put(&c, "probe:v1:abc", r#"{"audio":["eng"]}"#, DAY);
    put(&c, "probe:v1:abc", r#"{"audio":["eng"]}"#, DAY);
    assert_eq!((c.disk_counts().writes, c.disk_counts().skipped), (1, 1));
    let want = format!(" {}\n", r#"{"audio":["eng"]}"#.len());
    assert!(fs::read_to_string(&path).unwrap().contains(&want), "the rewrite is not framed");
}

#[test]
fn a_cache_that_never_wrote_the_unframed_format_drops_it() {
    let dir = TempDir::new().unwrap();
    let c = C::new(Options { accept_unframed: false, ..options(1 << 20, dir.path()) });
    let path = c.path_for("k").unwrap();
    fs::write(&path, format!("{}\nno length", now_secs() + 3600)).unwrap();

    let c = C::new(Options { accept_unframed: false, ..options(1 << 20, dir.path()) });
    assert_eq!(get(&c, "k"), None);
    assert!(!path.exists(), "an unreadable entry was left on disk");
}

#[test]
fn a_torn_file_is_never_served() {
    let dir = TempDir::new().unwrap();
    let c = open(dir.path());
    put(&c, "k", "the whole subtitle", DAY);
    let path = c.path_for("k").unwrap();
    let raw = fs::read_to_string(&path).unwrap();
    // The way ENOSPC or a kill mid-write truncates a file: it still parses as an expiry and a value, which is the
    // hazard, so the recorded length is what has to reject it.
    fs::write(&path, &raw[..raw.len() - 6]).unwrap();

    // Found at open: the file's size disagrees with its header, so the opening walk takes it.
    let reopened = open(dir.path());
    assert!(!path.exists());
    assert_eq!(get(&reopened, "k"), None, "served a truncated entry");

    // Torn after the index had seen it whole: caught on read, and dropped.
    let c = C::new(options(1, dir.path())); // no memory tier to answer from
    put(&c, "k", "the whole subtitle", DAY);
    fs::write(&path, &raw[..raw.len() - 6]).unwrap();
    assert_eq!(get(&c, "k"), None, "served a truncated entry");
    assert!(!path.exists(), "a torn entry should be dropped, not left to be re-read");
}

#[test]
fn a_completed_put_leaves_exactly_one_file() {
    let dir = TempDir::new().unwrap();
    let c = open(dir.path());
    put(&c, "k", "v", DAY);
    put(&c, "k", "v2", DAY);
    assert_eq!(files(dir.path()), 1, "a put left scratch behind");
}

#[test]
fn a_stale_sweep_verdict_cannot_unlink_a_new_generation() {
    let dir = TempDir::new().unwrap();
    let c = open(dir.path());
    put(&c, "k", "old", DAY);
    let old = c.disk.as_ref().unwrap().generation_for("k").unwrap();

    // This is the exact ordering of a sweep that copied old metadata, then lost the namespace lock to a put.
    put(&c, "k", "new", 2 * DAY);
    c.disk.as_ref().unwrap().retire_generation_for_test("k", old);

    assert_eq!(get(&open(dir.path()), "k").as_deref(), Some("new"));
}

#[test]
fn write_bursts_coalesce_to_one_active_sweep_and_one_follow_up() {
    let dir = TempDir::new().unwrap();
    let c = open(dir.path());
    let disk = c.disk.as_ref().unwrap();
    let before = c.disk_counts().readdirs;
    assert!(disk.request_burst_sweep(), "the idle tier did not assign a sweep owner");
    assert!(!disk.request_burst_sweep(), "a second write burst tried to spawn another sweep");
    assert!(!disk.request_burst_sweep(), "more writes should stay folded into the follow-up");
    disk.sweep_after_burst(SystemTime::now());
    assert_eq!(c.disk_counts().readdirs - before, 2, "the coalesced follow-up was lost or repeated");
    assert!(disk.request_burst_sweep(), "the tier did not return to idle");
    disk.sweep_after_burst(SystemTime::now());
}

#[test]
fn a_readback_cannot_overwrite_a_put_that_began_while_it_was_decoding() {
    let _serial = BLOCKING_VALUE_TEST.lock().unwrap_or_else(PoisonError::into_inner);
    let dir = TempDir::new().unwrap();
    put(&open(dir.path()), "k", "old", DAY);
    let cache = Arc::new(Cache::<BlockingValue>::new(options(1 << 20, dir.path())));
    let (started_tx, started_rx) = sync_channel(0);
    let (resume_tx, resume_rx) = sync_channel(0);
    *READBACK_HOOK.lock().unwrap() = Some((started_tx, resume_rx));

    let reader_cache = Arc::clone(&cache);
    let reader = std::thread::spawn(move || reader_cache.get("k").unwrap().as_str().to_owned());
    started_rx.recv().unwrap();

    let writer_cache = Arc::clone(&cache);
    let writer = std::thread::spawn(move || {
        writer_cache.put("k", BlockingValue(Arc::from("new")), 2 * DAY, Class::Durable);
    });
    resume_tx.send(()).unwrap();
    assert_eq!(reader.join().unwrap(), "old");
    writer.join().unwrap();
    assert_eq!(cache.get("k").unwrap().as_str(), "new");
    assert_eq!(get(&open(dir.path()), "k").as_deref(), Some("new"));
}

// ---- what is written --------------------------------------------------------------------------------------------

#[test]
fn short_lived_entries_stay_in_memory_and_retire_the_file() {
    let dir = TempDir::new().unwrap();
    let c = open(dir.path());
    put(&c, "answer", "five minutes", Duration::from_secs(300));
    put(&c, "tombstone", "", Duration::from_nanos(1));
    assert_eq!(get(&c, "answer").as_deref(), Some("five minutes"));
    assert_eq!((files(dir.path()), c.disk_counts().writes), (0, 0));

    put(&c, "k", "old", DAY);
    assert_eq!(files(dir.path()), 1);
    put(&c, "k", "", Duration::from_nanos(1)); // a tombstone retires the durable value
    assert_eq!(files(dir.path()), 0);
    assert_eq!(get(&open(dir.path()), "k"), None, "the retired value came back");
}

#[test]
fn the_threshold_is_the_caller_s_to_set() {
    let dir = TempDir::new().unwrap();
    let c = C::new(Options { min_persist_ttl: Duration::ZERO, ..options(1 << 20, dir.path()) });
    put(&c, "marker", "1", Duration::from_secs(60));
    assert_eq!(files(dir.path()), 1);
}

#[test]
fn an_unchanged_value_with_most_of_its_life_left_writes_nothing() {
    let dir = TempDir::new().unwrap();
    let c = open(dir.path());
    for _ in 0..5 {
        put(&c, "id", "12345", DAY);
    }
    let n = c.disk_counts();
    assert_eq!((n.writes, n.skipped), (1, 4), "{n:?}");
    put(&c, "id", "67890", DAY);
    assert_eq!(c.disk_counts().writes, 2, "a changed value was not written");
    put(&c, "id", "67890", 10 * DAY);
    assert_eq!(c.disk_counts().writes, 3, "the extended expiry was not written");
}

#[test]
fn volatile_entries_never_reach_disk_and_supersede_a_durable_one() {
    let dir = TempDir::new().unwrap();
    let c = C::new(options(64, dir.path()));
    put(&c, "transition", "old-durable", DAY);
    c.put("transition", Arc::from("new-volatile"), Duration::from_millis(1), Class::Volatile);
    for i in 0..20 {
        c.put(&format!("filler{i}"), Arc::from("x".repeat(30).as_str()), DAY, Class::Volatile);
    }
    assert_eq!(files(dir.path()), 0, "a volatile burst wrote files");
    assert_eq!(get(&C::new(options(64, dir.path())), "transition"), None, "the superseded value resurfaced");
}

// ---- the directory ----------------------------------------------------------------------------------------------

#[test]
fn the_directory_is_read_once_and_the_index_answers_the_rest() {
    let dir = TempDir::new().unwrap();
    {
        let first = open(dir.path());
        for i in 0..10 {
            put(&first, &format!("fact-{i}"), "v", DAY);
        }
    }
    let c = C::new(options(1, dir.path()));
    let opened = c.disk_counts();
    assert_eq!(opened.reads, 10, "the scan should read each file's head once: {opened:?}");
    assert_eq!(opened.readdirs, 1, "opening should list the directory once: {opened:?}");
    for i in 0..500 {
        assert!(c.get(&format!("never-written-{i}")).is_none());
    }
    c.sweep();
    let after = c.disk_counts();
    assert_eq!(after.reads, opened.reads, "lookups and a sweep opened files the index already described");
    assert_eq!(after.readdirs, opened.readdirs + 1);
    assert_eq!(get(&c, "fact-3").as_deref(), Some("v"));
}

#[test]
fn a_file_that_has_gone_is_a_miss_and_the_index_forgets_it() {
    let dir = TempDir::new().unwrap();
    put(&open(dir.path()), "k", "v", DAY);
    let c = open(dir.path());
    fs::remove_file(c.path_for("k").unwrap()).unwrap();
    let before = c.disk_counts().reads;
    assert_eq!(get(&c, "k"), None);
    assert_eq!(get(&c, "k"), None);
    assert_eq!(c.disk_counts().reads, before + 1, "the index still listed the missing file");
}

#[test]
fn files_it_does_not_recognise_are_left_alone() {
    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("README"), "not an entry").unwrap();
    fs::write(dir.path().join("notes.txt"), "nor this").unwrap();
    let c = open(dir.path());
    assert_eq!(c.sweep(), 0);
    assert_eq!(files(dir.path()), 2);
}

// ---- the sweep --------------------------------------------------------------------------------------------------

fn age(path: &Path, by: Duration) {
    let when = SystemTime::now() - by;
    let f = fs::File::options().write(true).open(path).unwrap();
    f.set_times(fs::FileTimes::new().set_modified(when)).unwrap();
}

#[test]
fn the_sweep_reclaims_what_nobody_reads_back_and_keeps_the_rest() {
    let dir = TempDir::new().unwrap();
    let c = C::new(options(1, dir.path()));
    for i in 0..20 {
        fs::write(c.path_for(&format!("dead-{i}")).unwrap(), "1\nv").unwrap();
    }
    // The opening walk reclaims what expired while the process was down...
    let c = C::new(options(1, dir.path()));
    assert_eq!(files(dir.path()), 0);
    // ...and a later sweep what expires while it runs.
    for i in 0..20 {
        fs::write(c.path_for(&format!("dead-{i}")).unwrap(), "1\nv").unwrap();
    }
    put(&c, "alive", "v", DAY);
    assert_eq!(c.sweep(), 20);
    assert_eq!(files(dir.path()), 1);
    assert_eq!(get(&open(dir.path()), "alive").as_deref(), Some("v"));
}

#[test]
fn the_sweep_clears_abandoned_temp_files_only() {
    let dir = TempDir::new().unwrap();
    let c = open(dir.path());
    let stale = dir.path().join(".tmp-crashed");
    let fresh = dir.path().join(".tmp-inflight");
    fs::write(&stale, "half").unwrap();
    fs::write(&fresh, "writing").unwrap();
    age(&stale, TEMP_FILE_GRACE * 2);
    c.sweep();
    assert!(!stale.exists(), "an abandoned temp file survived");
    assert!(fresh.exists(), "the sweep raced a write that was still in flight");
}

#[test]
fn a_temp_born_just_after_the_sweep_read_the_clock_is_in_flight_even_over_budget() {
    let dir = TempDir::new().unwrap();
    let c = open(dir.path());
    let inflight = dir.path().join(".tmp-other-thread");
    fs::write(&inflight, "x".repeat(1000)).unwrap();
    // Another thread's write, created after this sweep's clock was read. A budget pass that took it would fail that
    // thread's rename and switch persistence off.
    let f = fs::File::options().write(true).open(&inflight).unwrap();
    f.set_times(fs::FileTimes::new().set_modified(SystemTime::now() + Duration::from_millis(100))).unwrap();
    c.set_disk_bytes(10);
    c.sweep();
    assert!(inflight.exists(), "the sweep deleted a write that was in flight");

    // A clock stepped well back is another matter: that temp has no age at all, and the budget bounds it.
    let undatable = dir.path().join(".tmp-undatable");
    fs::write(&undatable, "x".repeat(1000)).unwrap();
    let f = fs::File::options().write(true).open(&undatable).unwrap();
    f.set_times(fs::FileTimes::new().set_modified(SystemTime::now() + Duration::from_secs(60))).unwrap();
    c.sweep();
    assert!(!undatable.exists());
}

#[test]
fn a_temp_left_by_the_readable_scheme_s_older_naming_is_reclaimed_too() {
    let dir = TempDir::new().unwrap();
    let c = C::new(Options { naming: Naming::Readable, ..options(1 << 20, dir.path()) });
    let stray = c.path_for("k").unwrap().with_extension("t99");
    fs::write(&stray, "half").unwrap();
    age(&stray, TEMP_FILE_GRACE * 2);
    c.sweep();
    assert!(!stray.exists());
}

#[test]
fn over_budget_the_sweep_spends_the_cheap_entries_first() {
    let dir = TempDir::new().unwrap();
    let cost = |key: &str| u8::from(key.starts_with("os:")) + 2 * u8::from(key.starts_with("translate:"));
    let o = || Options { naming: Naming::Readable, cost, ..options(1 << 20, dir.path()) };
    let c = C::new(Options { disk_bytes: 0, ..o() });
    // The expensive entry is written FIRST, so age alone would take it.
    put(&c, "translate:film", &"x".repeat(40), DAY);
    std::thread::sleep(Duration::from_millis(5));
    put(&c, "os:1", &"x".repeat(40), DAY);
    std::thread::sleep(Duration::from_millis(5));
    put(&c, "search:film", &"x".repeat(40), DAY);

    // Found by a scan too, which reads the cost back from the Readable name.
    let reopened = C::new(Options { disk_bytes: 130, ..o() });
    reopened.sweep();
    assert!(!reopened.path_for("search:film").unwrap().exists(), "the free-to-rebuild entry survived");
    assert!(reopened.path_for("translate:film").unwrap().exists(), "the costliest entry was spent first");
    assert!(reopened.path_for("os:1").unwrap().exists());
}

#[test]
fn within_a_cost_the_oldest_goes_first() {
    let dir = TempDir::new().unwrap();
    let c = open(dir.path());
    c.set_disk_bytes(0);
    for i in 0..40 {
        put(&c, &format!("k{i}"), &"x".repeat(8 << 10), DAY);
        std::thread::sleep(Duration::from_millis(1));
    }
    c.set_disk_bytes(64 << 10);
    c.sweep();
    let left: u64 = fs::read_dir(dir.path()).unwrap().flatten().map(|e| e.metadata().unwrap().len()).sum();
    assert!(left > 0 && left <= 64 << 10, "{left} bytes left");
    assert!(get(&c, "k39").is_some(), "the newest entry was evicted before older ones");
}

#[test]
fn a_failed_write_turns_persistence_off_until_the_next_sweep() {
    let dir = TempDir::new().unwrap();
    let c = open(dir.path());
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o500)).unwrap();
    put(&c, "k", "v", DAY);
    assert_eq!((c.write_failures(), c.disk_enabled()), (1, false));
    assert_eq!(get(&c, "k").as_deref(), Some("v"), "memory stopped serving");

    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    c.sweep(); // reaping is what frees the space a full volume was short of
    assert!(c.disk_enabled());
    put(&c, "k2", "v2", DAY);
    assert_eq!(c.disk_counts().writes, 1);
}

#[test]
fn files_already_on_disk_are_still_served_after_a_failed_write() {
    let dir = TempDir::new().unwrap();
    put(&open(dir.path()), "old", "paid for", DAY);
    // No memory tier, so every answer below comes off disk.
    let c = C::new(options(1, dir.path()));
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o500)).unwrap();
    put(&c, "new", "v", DAY);
    assert!(!c.disk_enabled(), "the failed write did not turn writes off");
    let served = get(&c, "old");
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(served.as_deref(), Some("paid for"), "a failed write stopped existing files being served");
}

#[test]
fn a_shorter_ttl_than_the_file_holds_is_written() {
    let dir = TempDir::new().unwrap();
    let c = open(dir.path());
    put(&c, "k", "v", 10 * DAY);
    put(&c, "k", "v", Duration::from_secs(15 * 60));
    assert_eq!(c.disk_counts().writes, 2, "the old long expiry was kept");
    let raw = fs::read_to_string(c.path_for("k").unwrap()).unwrap();
    let expiry: u64 = raw.split_once(' ').unwrap().0.parse().unwrap();
    assert!(expiry <= now_secs() + 15 * 60 + 1, "the file still expires in {}s", expiry - now_secs());
}

#[test]
fn a_sweep_started_with_a_stale_clock_leaves_a_fresh_temp_alone() {
    let dir = TempDir::new().unwrap();
    let behind: Clock = Arc::new(|| SystemTime::now() - Duration::from_secs(100));
    let c = open(dir.path()).with_clock(behind);
    let inflight = dir.path().join(".tmp-written-while-the-sweep-ran");
    fs::write(&inflight, "x".repeat(1000)).unwrap();
    c.set_disk_bytes(10);
    c.sweep();
    assert!(inflight.exists(), "the sweep took a write that began after it started");
}

#[test]
fn a_directory_that_cannot_be_made_stays_off() {
    let base = TempDir::new().unwrap();
    let file = base.path().join("not-a-dir");
    fs::write(&file, "x").unwrap();
    let c = C::new(options(1 << 20, &file.join("cache")));
    put(&c, "k", "v", DAY);
    assert_eq!(get(&c, "k").as_deref(), Some("v"), "memory stopped serving");
    c.sweep();
    assert!(!c.disk_enabled(), "a sweep cannot make an uncreatable directory writable");
}

// ---- memory -----------------------------------------------------------------------------------------------------

#[test]
fn memory_expires_on_the_cache_s_clock() {
    let (clk, offset) = clock(UNIX_EPOCH + Duration::from_secs(1000));
    let c = C::new(Options::new(1 << 20)).with_clock(clk);
    c.put("k", Arc::from("v"), Duration::from_secs(10), Class::Durable);
    assert_eq!(c.ttl("k"), Some(Duration::from_secs(10)));
    offset.store(11, Ordering::Relaxed);
    assert_eq!(get(&c, "k"), None);
}

#[test]
fn the_sweep_drops_expired_memory_nobody_reads() {
    let (clk, offset) = clock(UNIX_EPOCH + Duration::from_secs(1000));
    let c = C::new(Options::new(1 << 20)).with_clock(clk);
    c.put("a", Arc::from("0123456789"), Duration::from_secs(10), Class::Durable);
    c.put("b", Arc::from("0123456789"), DAY, Class::Durable);
    offset.store(11, Ordering::Relaxed);
    c.sweep();
    assert_eq!(c.mem_stats(), MemStats { entries: 1, bytes: 1 + 10 });
}

#[test]
fn memory_is_bounded_by_bytes_and_keeps_what_is_read() {
    let c = C::new(Options::new(1000));
    c.put("hot", Arc::from("0123456789"), DAY, Class::Durable);
    for i in 0..200 {
        c.put(&format!("scan{i:03}"), Arc::from("x".repeat(40).as_str()), DAY, Class::Durable);
        assert!(c.get("hot").is_some(), "the entry read on every write was evicted at scan{i:03}");
    }
    assert!(c.mem_stats().bytes <= 1000, "{:?}", c.mem_stats());
}

#[test]
fn a_reader_outlives_the_entry_it_read() {
    let c = C::new(Options::new(1 << 20));
    c.put("k", Arc::from("0123456789"), DAY, Class::Durable);
    let held = c.get("k").unwrap();
    let again = c.get("k").unwrap();
    assert!(Arc::ptr_eq(&held, &again), "a hit copied the value");
    assert_eq!(c.mem_stats().bytes, 1 + 10, "readers must not add to the budget");
    c.remove("k");
    assert_eq!(&*held, "0123456789");
    assert_eq!(c.mem_stats().bytes, 0);
}

#[test]
fn get_mem_does_not_probe_the_disk() {
    let dir = TempDir::new().unwrap();
    put(&open(dir.path()), "k", "v", DAY);
    let c = C::new(options(1 << 20, dir.path()));
    assert!(c.get_mem("k").is_none());
    assert_eq!(c.disk_counts().reads, 1, "only the scan's head read");
    assert!(c.get("k").is_some());
    assert!(c.get_mem("k").is_some(), "the disk hit was not hoisted into memory");
}

#[test]
fn an_absurd_ttl_does_not_panic() {
    let c = C::new(Options::new(1 << 20));
    c.put("k", Arc::from("v"), Duration::MAX, Class::Durable);
    assert!(c.get("k").is_some());
}
