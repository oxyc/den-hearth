//! Session lifecycle supervision, idle expiry, and producer parking.

use super::*;

/// Tick while ffmpeg runs, sleep otherwise, and end the session when it goes idle or expires.
pub(super) async fn supervise(st: Arc<AppState>, s: Arc<Session>) {
    loop {
        let (running, idle_at, park_in) = {
            let mut i = s.lock();
            if i.ended {
                return;
            }
            s.refresh(&mut i, &st);
            s.gate(&mut i, &st);
            (
                i.job.as_ref().is_some_and(|j| j.exit.is_none() && !j.stopped),
                i.idle_deadline(st.cfg.session_idle),
                // Measured from the SESSION's own last request, not the job's own pause: a job paused for
                // filling its ahead window is not idle, only unneeded for a few seconds yet, and a job's pause
                // duration says nothing about whether its viewer is still right there. This is also the
                // threshold `start_producer`'s own contention reap (`reap_for_contention`) honours, so a
                // producer is never pulled out from under a session still being watched either way.
                i.job
                    .as_ref()
                    .and_then(Job::parked_for)
                    .map(|_| (i.last_seen + st.cfg.producer_idle).saturating_duration_since(Instant::now())),
            )
        };
        if park_in.is_some_and(|remaining| remaining.is_zero()) {
            let parked = {
                let mut i = s.lock();
                i.job
                    .as_ref()
                    .and_then(Job::parked_for)
                    .filter(|_| i.last_seen.elapsed() >= st.cfg.producer_idle)
                    .and_then(|_| i.job.take())
            };
            if let Some(job) = parked {
                eprintln!(
                    "session {}: {}{}",
                    s.short(),
                    job.pull_line("parked; producer slot released"),
                    rid_tail(s.rid.as_deref())
                );
                job.stop().await;
                continue;
            }
        }
        let now = unix_now();
        if Instant::now() >= idle_at {
            st.end_session(&s.sid, "idle").await;
            return;
        }
        if now >= s.exp {
            st.end_session(&s.sid, "expired").await;
            return;
        }
        let exp_at = Instant::now() + Duration::from_secs(s.exp - now);
        let until_end = idle_at.min(exp_at).saturating_duration_since(Instant::now());
        // Producer waiters belong to other sessions and cannot notify this one directly, so parked jobs poll cheaply.
        let wait = if running || park_in.is_some() {
            TICK.min(park_in.unwrap_or(TICK)).min(until_end)
        } else {
            until_end
        };
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = s.wake.notified() => {}
        }
    }
}
