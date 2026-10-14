//! Go: `internal/scout/probe_test.go` (201 lines). Work package 5.
//!
//! The Go tests serve responses from `httptest.NewServer`. Here `serve_once` does the same for one request
//! on a `std::net` listener and thread, so the crate's dev-dependencies stay as they are.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use super::probe_containers::testdata;

// Serves one HTTP request on a loopback port: reads the request head, then hands the socket to `respond`,
// which writes the response. Returns the URL.
fn serve_once(respond: impl FnOnce(&mut TcpStream) + Send + 'static) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    std::thread::spawn(move || {
        let Ok((mut conn, _)) = listener.accept() else { return };
        let mut seen = Vec::new();
        let mut buf = [0u8; 1024];
        while !seen.windows(4).any(|w| w == b"\r\n\r\n") {
            match conn.read(&mut buf) {
                Ok(0) | Err(_) => return,
                Ok(n) => seen.extend_from_slice(&buf[..n]),
            }
        }
        respond(&mut conn);
    });
    format!("http://{addr}/file.mkv")
}

// Minimal RIFF/AVI builders: id + little-endian size + payload, padded to an even boundary.
fn avi_chunk(id: &str, payload: &[u8]) -> Vec<u8> {
    let mut out = id.as_bytes().to_vec();
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
    if payload.len() % 2 == 1 {
        out.push(0);
    }
    out
}

fn avi_file(chunks: &[Vec<u8>]) -> Vec<u8> {
    let body: Vec<u8> = chunks.concat();
    let mut out = b"RIFF".to_vec();
    out.extend_from_slice(&((4 + body.len()) as u32).to_le_bytes());
    out.extend_from_slice(b"AVI ");
    out.extend_from_slice(&body);
    out
}

// The parser is what makes the picker honest, so it is tested against a REAL head — 5 KB cut from a
// release TorBox actually served, not a synthetic file. That release is the reason this exists: offered
// for a Swedish series, it carries Italian audio and no subtitles, and nothing in its title said so.
#[test]
fn parse_matroska_tracks_real_head() {
    let head = testdata("tracks.mkv");
    let got = crate::probe::parse_matroska_tracks(&head).expect("parse");
    assert!(got.audio.len() == 1 && got.audio[0] == "it", "audio = {:?}, want [it]", got.audio);
    assert!(got.subtitles.is_empty(), "subtitles = {:?}, want none", got.subtitles);
}

// A head with no Tracks element (an MP4 with its moov at the end, a truncated read) must say so rather
// than report an empty-but-successful result — the caller keeps the title's own badges instead.
#[test]
fn parse_matroska_tracks_no_tracks() {
    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("empty", b"".to_vec()),
        ("garbage", b"not a matroska file at all, just bytes".to_vec()),
        ("truncated", vec![0x1A, 0x45, 0xDF, 0xA3, 0x01]),
    ];
    for (name, buf) in cases {
        assert_eq!(
            crate::probe::parse_matroska_tracks(&buf).err(),
            Some(crate::probe::ERR_NO_TRACKS),
            "{name}: want ErrNoTracks"
        );
    }
}

// Matroska says an absent Language means "eng". Honouring that announced ENGLISH audio for an untagged
// AC-3 track in a Swedish series — the tag was missing, not English. An honest unknown beats a confident
// wrong answer, so untagged tracks are counted, never named.
#[test]
fn parse_track_entry_untagged_is_not_english() {
    let entry = [crate::probe::ID_TRACK_TYPE, 0x81, crate::probe::TRACK_TYPE_AUDIO];
    let (kind, lang) = crate::probe::parse_track_entry(&entry);
    assert!(
        kind == crate::probe::TRACK_TYPE_AUDIO && lang == "und",
        "kind={kind} lang={lang:?}, want audio/und"
    );
}

// The same file carries both ISO 639-2 and BCP-47 for a track, and across a 30-release sweep the output
// mixed "eng" with "en" — which no consumer can group on.
#[test]
fn clean_lang_normalises_to_two_letter() {
    let cases = [
        ("swe", "sv"),
        ("eng", "en"),
        ("ger", "de"),
        ("deu", "de"),
        ("cze", "cs"),
        ("ces", "cs"),
        ("en", "en"),
        ("sv", "sv"),
        ("klingon", "klingon"),
    ];
    for (input, want) in cases {
        let got = crate::probe::clean_lang(input.as_bytes());
        assert_eq!(got, want, "cleanLang({input:?}) = {got:?}, want {want:?}");
    }
}

// A regional tag and its base language are the same shelf to someone picking a track.
#[test]
fn clean_lang() {
    let cases = [("swe\x00", "sv"), ("PT-BR", "pt"), ("en_US", "en"), ("und", ""), ("  ", ""), ("ita", "it")];
    for (input, want) in cases {
        let got = crate::probe::clean_lang(input.as_bytes());
        assert_eq!(got, want, "cleanLang({input:?}) = {got:?}, want {want:?}");
    }
}

// Only audio and subtitle tracks are reported; video and the cover-art stream the sample carries are not
// languages anyone chooses between.
#[test]
fn parse_track_entry_ignores_video() {
    let entry = [crate::probe::ID_TRACK_TYPE, 0x81, 0x01];
    let (_, lang) = crate::probe::parse_track_entry(&entry);
    assert!(lang.is_empty(), "video track reported language {lang:?}");
}

// A server that ignores Range answers 200 and starts sending the whole file. The probe must still read
// only its megabyte: an unbounded drain would download a 20 GB remux to discard it, which is precisely
// the download this exists to avoid, and on a metered debrid account it is the expensive kind of bug.
#[tokio::test]
async fn probe_tracks_reads_only_the_head_when_range_is_ignored() {
    const FILE_SIZE: usize = 40 << 20;
    // Atomic: the handler goes on writing (that is the point — it ignores Range) while the assertion
    // below reads the tally, so a plain int is a data race the -race build fails on.
    let served = Arc::new(AtomicI64::new(0));
    let served_srv = Arc::clone(&served);
    let head = testdata("tracks.mkv");
    let url = serve_once(move |conn| {
        // deliberately ignores the Range header
        if write!(conn, "HTTP/1.1 200 OK\r\nContent-Length: {FILE_SIZE}\r\nConnection: close\r\n\r\n")
            .is_err()
        {
            return;
        }
        if conn.write_all(&head).is_err() {
            return;
        }
        served_srv.fetch_add(head.len() as i64, Ordering::SeqCst);
        let buf = vec![0u8; 1 << 16];
        while served_srv.load(Ordering::SeqCst) < FILE_SIZE as i64 {
            match conn.write(&buf) {
                Ok(m) => served_srv.fetch_add(m as i64, Ordering::SeqCst),
                Err(_) => return,
            };
        }
    });

    let client = crate::deps::ReqwestDoer(reqwest::Client::new());
    let p = crate::probe::probe_tracks(&client, &url).await.expect("probe");
    let got = served.load(Ordering::SeqCst);
    assert!(got <= 8 << 20, "pulled {got} bytes for a 1 MiB probe — the drain is unbounded");
    // The server ignored Range and is sending the whole file: Content-Length IS the file's total size,
    // unlike a 206's (which names only the chunk asked for).
    assert_eq!(
        p.size_bytes, FILE_SIZE as i64,
        "SizeBytes = {} (the Content-Length of the ignored-Range response)",
        p.size_bytes
    );
}

// ProbeTracks resolves the EXACT file this stream plays — for an episode picked out of a season pack,
// that is the episode alone — so the total it reads off the response is the episode's own size, never
// the pack's. Mirrors den-remux's own log for a real Fauda S1E3 release: the indexer's title claimed a
// 68 GB season pack, and the file actually served was 1,136,580,921 bytes.
#[tokio::test]
async fn probe_tracks_size_bytes_is_the_episode_not_the_pack() {
    let head = testdata("tracks.mkv");
    const EPISODE_SIZE: i64 = 1_136_580_921;
    let url = serve_once(move |conn| {
        let _ = write!(
            conn,
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-{}/{EPISODE_SIZE}\r\nConnection: close\r\n\r\n",
            head.len() - 1
        );
        let _ = conn.write_all(&head);
    });

    let client = crate::deps::ReqwestDoer(reqwest::Client::new());
    let p = crate::probe::probe_tracks(&client, &url).await.expect("probe");
    assert_eq!(p.size_bytes, EPISODE_SIZE, "SizeBytes = {} (the episode's own size)", p.size_bytes);
}

// AVI's richest-audio rule, which had no test while its MP4 twin did.
#[test]
fn parse_avi_richest_audio_track_wins() {
    // strf carries a WAVEFORMATEX whose second field is the channel count; two audio streams, poorer first.
    let strf = |channels: u16| {
        let mut b = vec![0u8; 16];
        b[2..4].copy_from_slice(&channels.to_le_bytes());
        b
    };
    let mut strh_audio = b"auds".to_vec();
    strh_audio.extend_from_slice(&[0u8; 8]);
    let file = avi_file(&[
        avi_chunk("strh", &strh_audio),
        avi_chunk("strf", &strf(6)),
        avi_chunk("strh", &strh_audio),
        avi_chunk("strf", &strf(8)),
    ]);
    let (p, ok) = crate::probe_avi::parse_avi(&file);
    assert!(
        ok && p.audio_channels == "7.1",
        "5.1 listed first won over 7.1: {:?} (ok={ok})",
        p.audio_channels
    );
}
