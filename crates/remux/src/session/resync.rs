//! Signed session controls for opt-in subtitle alignment. Source capabilities stay in this module;
//! callers can only name a rendition and the audio ordinal already fixed by their session.
use super::*;
use bytes::Bytes;
use hyper::HeaderMap;
use serde::Deserialize;
use sha2::{Digest, Sha256};

const BUDGET: Duration = Duration::from_secs(120);
const FAILURE_RETRY: Duration = Duration::from_secs(30);

pub(super) struct State {
    pub cancel: tokio::sync::watch::Sender<bool>,
    gate: tokio::sync::Mutex<()>,
    results: std::sync::Mutex<Vec<Option<Cached>>>,
}
impl Default for State {
    fn default() -> Self {
        Self {
            cancel: tokio::sync::watch::channel(false).0,
            gate: Default::default(),
            results: Default::default(),
        }
    }
}
#[derive(Clone)]
struct Aligned {
    doc: Arc<str>,
    offset_ms: i64,
    via_sparse: bool,
}
struct Cached {
    at: Instant,
    result: Result<Aligned, &'static str>,
}

// Created only by the gate owner once it starts actual work. Dropping a timed-out/canceled
// operation records the same cooldown as a returned failure; a queued caller never owns this guard.
struct Attempt<'a> {
    state: &'a State,
    rendition: usize,
    finished: bool,
}
impl Drop for Attempt<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut results = self.state.results.lock().unwrap_or_else(|e| e.into_inner());
        if results.len() <= self.rendition {
            results.resize_with(self.rendition + 1, || None);
        }
        results[self.rendition] = Some(Cached { at: Instant::now(), result: Err("alignment_failed") });
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Selection {
    rendition: usize,
    audio_track: usize,
    #[serde(default)]
    check_only: bool,
}

pub(crate) fn aligned_file(file: &str) -> Option<usize> {
    let n = file.strip_prefix("sub")?.strip_suffix(".aligned.vtt")?;
    if n.len() != 1 {
        return None;
    }
    n.parse::<usize>().ok().filter(|n| *n < crate::subs::MAX_LANGUAGES)
}

pub(crate) fn error(code: &'static str) -> Response<Body> {
    let status = match code {
        "invalid_selection" => StatusCode::BAD_REQUEST,
        "unsupported" => StatusCode::CONFLICT,
        "session_ended" => StatusCode::GONE,
        _ => StatusCode::BAD_GATEWAY,
    };
    httputil::json(status, &serde_json::json!({"error":code}), &[("cache-control", "no-store")])
}

impl Session {
    pub(crate) async fn resync_subtitle(&self, st: &AppState, choice: Selection) -> Response<Body> {
        let mut cancel = self.resync.cancel.subscribe();
        let canceled = *cancel.borrow();
        if canceled || self.lock().ended {
            return error("session_ended");
        }
        if choice.audio_track != self.audio
            || choice.audio_track > 4095
            || choice.rendition >= crate::subs::MAX_LANGUAGES
        {
            return error("invalid_selection");
        }
        if !self.renditions.get(choice.rendition).is_some_and(|r| r.den && r.own.is_none())
            || self.subs.is_none()
        {
            return error("unsupported");
        }
        self.touch();
        tokio::select! {
            biased;
            _ = cancel.changed() => error("session_ended"),
            result = tokio::time::timeout(BUDGET, self.align_selected(st, choice)) => {
                result.unwrap_or_else(|_| error("alignment_failed"))
            }
        }
    }

    async fn align_selected(&self, st: &AppState, choice: Selection) -> Response<Body> {
        // Holding one gate through the operation coalesces repeated clicks and bounds this session
        // to one alignment. Failed answers have a short cooldown; successes are immutable until end.
        let _gate = self.resync.gate.lock().await;
        let n = choice.rendition;
        if self.subtitle(st, n).await.is_none() {
            return error("alignment_failed");
        }
        let sb = self.subs.as_ref().expect("checked subtitle capability");
        let Some(entry) = sb.cache.lock().await.selected.get(n).and_then(Clone::clone) else {
            return error("unsupported");
        };
        if choice.check_only {
            return httputil::json(
                StatusCode::OK,
                &serde_json::json!({"supported":!entry.hash_match,"hashMatch":entry.hash_match,"source":"opensubtitles"}),
                &[("cache-control", "no-store")],
            );
        }
        if entry.hash_match {
            return error("unsupported");
        }
        let cached = self
            .resync
            .results
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(n)
            .and_then(|c| c.as_ref())
            .filter(|c| c.result.is_ok() || c.at.elapsed() < FAILURE_RETRY)
            .map(|c| c.result.clone());
        let result = match cached {
            Some(result) => result,
            None => {
                let mut attempt = Attempt { state: &self.resync, rendition: n, finished: false };
                let result = self.align_entry(st, &entry).await;
                // End may race completion; never stage an answer after the session ended.
                if self.lock().ended {
                    return error("session_ended");
                }
                let mut results = self.resync.results.lock().unwrap_or_else(|e| e.into_inner());
                if results.len() <= n {
                    results.resize_with(n + 1, || None);
                }
                results[n] = Some(Cached { at: Instant::now(), result: result.clone() });
                attempt.finished = true;
                result
            }
        };
        match result {
            Ok(aligned) => httputil::json(
                StatusCode::OK,
                &serde_json::json!({
                    "url":format!("/remux/s/{}/{}/sub{n}.aligned.vtt", self.sid, self.sig),
                    "offsetMs":aligned.offset_ms, "viaSparse":aligned.via_sparse,
                }),
                &[("cache-control", "no-store")],
            ),
            Err(code) => error(code),
        }
    }

    async fn align_entry(&self, st: &AppState, entry: &crate::subs::Entry) -> Result<Aligned, &'static str> {
        let source = self.lock().play_url.clone();
        let mut url =
            reqwest::Url::parse(&crate::subs::vtt_url(&entry.url)).map_err(|_| "alignment_failed")?;
        let kept: Vec<(String, String)> = url
            .query_pairs()
            .filter(|(k, _)| !matches!(k.as_ref(), "resync" | "resync_key" | "resync_audio" | "sparse"))
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        url.set_query(None);
        url.query_pairs_mut()
            .extend_pairs(kept)
            .append_pair("resync", &source)
            .append_pair("resync_audio", &format!("a:{}", self.audio));
        if let Some(key) = &self.file_key {
            url.query_pairs_mut().append_pair(
                "resync_key",
                &Sha256::digest(key.as_bytes()).iter().map(|b| format!("{b:02x}")).collect::<String>(),
            );
        }
        let first = fetch(st, url.as_str()).await?;
        let retry = !first.status().is_success()
            && first.headers().get("x-den-degraded").is_some_and(|v| v == "sparse");
        let response = if retry {
            url.query_pairs_mut().append_pair("sparse", "1");
            fetch(st, url.as_str()).await?
        } else {
            first
        };
        decode(response, retry)
    }

    pub(crate) async fn aligned_subtitle(&self, n: usize) -> Response<Body> {
        if self.lock().ended {
            return error("session_ended");
        }
        let results = self.resync.results.lock().unwrap_or_else(|e| e.into_inner());
        let Some(Ok(aligned)) = results.get(n).and_then(|c| c.as_ref()).map(|c| &c.result) else {
            return httputil::not_found();
        };
        let mut response = httputil::text("text/vtt; charset=utf-8", "no-store", aligned.doc.to_string());
        response
            .headers_mut()
            .insert("x-den-subtitle-offset-ms", aligned.offset_ms.to_string().parse().unwrap());
        response
    }
}

fn decode(response: Response<Bytes>, via_sparse: bool) -> Result<Aligned, &'static str> {
    if !response.status().is_success() || response.headers().contains_key("x-den-degraded") {
        return Err("alignment_failed");
    }
    // A plain unaligned fallback is never success, even when the upstream responds 200.
    let offset_ms = response
        .headers()
        .get("x-den-subtitle-offset-ms")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<i64>().ok())
        .ok_or("alignment_failed")?;
    let doc = crate::subs::for_hls(response.body()).ok_or("alignment_failed")?;
    if doc.len() > crate::subs::MAX_SUBTITLE as usize || crate::subs::parse_cues(&doc).is_empty() {
        return Err("alignment_failed");
    }
    Ok(Aligned { doc: doc.into(), offset_ms, via_sparse })
}

async fn fetch(st: &AppState, url: &str) -> Result<Response<Bytes>, &'static str> {
    if !crate::subs::on_origin(url, &st.cfg.subtitle_origins) {
        return Err("alignment_failed");
    }
    let local = crate::config::local(url, &st.cfg.origin_aliases);
    if let Some(call) =
        st.local_scout.get().and_then(|l| l.get(&local, HeaderMap::new(), crate::subs::MAX_SUBTITLE as usize))
    {
        return call.await.map_err(|_| "alignment_failed");
    }
    let response = st.scout_http.get(&local).timeout(BUDGET).send().await.map_err(|_| "alignment_failed")?;
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = crate::probe::read_capped(response, crate::subs::MAX_SUBTITLE)
        .await
        .map_err(|_| "alignment_failed")?;
    let mut response = Response::new(Bytes::from(bytes));
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn staged_vtt_bound_includes_the_added_timestamp_map() {
        let prefix = "WEBVTT\n\n00:00:01.000 --> 00:00:02.000\n";
        for (length, accepted) in
            [(crate::subs::MAX_SUBTITLE as usize, false), (crate::subs::MAX_SUBTITLE as usize - 128, true)]
        {
            let body = format!("{prefix}{}\n", "x".repeat(length - prefix.len() - 1));
            assert_eq!(body.len(), length);
            let response =
                Response::builder().header("x-den-subtitle-offset-ms", "0").body(Bytes::from(body)).unwrap();
            assert_eq!(decode(response, false).is_ok(), accepted);
        }
    }
}
