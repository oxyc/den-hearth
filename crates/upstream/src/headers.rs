//! What a response says about waiting: `Retry-After`, a spent rate-limit window, and whether a status is a refusal
//! worth pausing for.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::header::{HeaderMap, RETRY_AFTER};
use reqwest::StatusCode;

/// The longest wait honoured from an upstream's own headers. Debrid limits are hourly, and a day named by a broken
/// proxy or a hostile host must not switch an upstream off for a day.
pub const MAX_RETRY_AFTER: Duration = Duration::from_secs(60 * 60);

/// `Retry-After` as a wait: delta-seconds, or an HTTP-date, capped at [`MAX_RETRY_AFTER`]. A date already past is
/// `Some(ZERO)`. `None` when the header is absent or unreadable, which leaves the caller on its own backoff.
///
/// All three date forms RFC 9110 §5.6.7 requires a recipient to accept are read: the IMF-fixdate
/// (`Sun, 06 Nov 1994 08:49:37 GMT`), the obsolete RFC 850 form and asctime.
pub fn retry_after(headers: &HeaderMap, now: SystemTime) -> Option<Duration> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
    let wait = match value.parse::<u64>() {
        Ok(secs) => Duration::from_secs(secs),
        Err(_) => httpdate::parse_http_date(value).ok()?.duration_since(now).unwrap_or(Duration::ZERO),
    };
    Some(wait.min(MAX_RETRY_AFTER))
}

/// How long until an upstream's allowance comes back, when its answer says none is left. Read from the IETF draft
/// `RateLimit: "policy";r=0;t=30` (or the early `remaining=0, reset=30`), its older `RateLimit-Remaining` /
/// `RateLimit-Reset`, or the common `X-RateLimit-Remaining` / `X-RateLimit-Reset`, in that order: the first that
/// states a remaining count decides. A reset over a billion is a Unix time, not seconds to wait. Capped at
/// [`MAX_RETRY_AFTER`]. `None` when something is left, nothing is said, or there is no wait to honour.
pub fn exhausted_for(headers: &HeaderMap, now: SystemTime) -> Option<Duration> {
    let field = |name: &str| headers.get(name).and_then(|v| v.to_str().ok()).map(str::trim);
    let number = |name: &str| field(name).and_then(|v| v.parse::<u64>().ok());

    let mut draft = (None, None);
    for (key, value) in
        field("ratelimit").unwrap_or_default().split([',', ';']).filter_map(|p| p.split_once('='))
    {
        let value = value.trim().parse::<u64>().ok();
        match key.trim() {
            "remaining" | "r" => draft.0 = value,
            "reset" | "t" => draft.1 = value,
            _ => {}
        }
    }
    let (remaining, reset) = [
        draft,
        (number("ratelimit-remaining"), number("ratelimit-reset")),
        (number("x-ratelimit-remaining"), number("x-ratelimit-reset")),
    ]
    .into_iter()
    .find(|(remaining, _)| remaining.is_some())?;
    if remaining? != 0 {
        return None;
    }
    let reset = reset?;
    let wait = if reset > 1_000_000_000 {
        (UNIX_EPOCH + Duration::from_secs(reset)).duration_since(now).unwrap_or(Duration::ZERO)
    } else {
        Duration::from_secs(reset)
    };
    let wait = wait.min(MAX_RETRY_AFTER);
    (!wait.is_zero()).then_some(wait)
}

/// An upstream telling a credential to stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Throttle {
    /// It said how long.
    Stated(Duration),
    /// A 429 that did not: the caller's own exponential window applies.
    Unstated,
}

/// Is this response the upstream telling its caller to wait? A 429 always; a 503 only when it names a wait, since
/// a bare 503 is the service being unwell rather than the credential being told to stop, and any other 5xx is one
/// request failing. A `Retry-After` of zero or a date already past is not a wait: "retry now" is not advice worth
/// taking, so on a 429 it falls to the caller's window and a 503 carrying it is no refusal.
pub fn throttle(status: StatusCode, headers: &HeaderMap, now: SystemTime) -> Option<Throttle> {
    let stated = retry_after(headers, now).filter(|wait| !wait.is_zero());
    match status {
        StatusCode::TOO_MANY_REQUESTS => Some(stated.map_or(Throttle::Unstated, Throttle::Stated)),
        StatusCode::SERVICE_UNAVAILABLE => stated.map(Throttle::Stated),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.append(*k, v.parse().expect("a header value"));
        }
        h
    }

    fn at(unix: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(unix)
    }

    /// RFC 9110's own example date.
    const DATE: &str = "Sun, 06 Nov 1994 08:49:37 GMT";
    const DATE_UNIX: u64 = 784_111_777;

    #[test]
    fn retry_after_reads_seconds_dates_and_refuses_garbage() {
        let ra = |v: &str, now: u64| retry_after(&headers(&[("retry-after", v)]), at(now));
        assert_eq!(ra("120", 0), Some(Duration::from_secs(120)));
        assert_eq!(ra(" 7 ", 0), Some(Duration::from_secs(7)));
        assert_eq!(ra(DATE, DATE_UNIX - 90), Some(Duration::from_secs(90)));
        // A date already past means "now", not a negative wait or a parse failure.
        assert_eq!(ra(DATE, DATE_UNIX + 5), Some(Duration::ZERO));
        // Both forms are capped: a stated day is an hour.
        assert_eq!(ra("86400", 0), Some(MAX_RETRY_AFTER));
        assert_eq!(ra(DATE, 0), Some(MAX_RETRY_AFTER));
        for junk in [
            "",
            "soon",
            "-5",
            "1.5",
            "Sun, 06 Nov 1994 08:49:37",
            "Sun, 06 Foo 1994 08:49:37 GMT",
            "Sun, 32 Nov 1994 08:49:37 GMT",
            "Sunday, 06-Foo-94 08:49:37 GMT",
            "Sun Nov  6 25:49:37 1994",
        ] {
            assert_eq!(ra(junk, DATE_UNIX), None, "{junk:?}");
        }
        assert_eq!(retry_after(&HeaderMap::new(), at(0)), None);
    }

    /// RFC 9110 §5.6.7: a recipient must accept the IMF-fixdate and both obsolete forms.
    #[test]
    fn retry_after_accepts_all_three_http_date_formats() {
        let ra = |v: &str, now: u64| retry_after(&headers(&[("retry-after", v)]), at(now));
        for date in
            ["Sun, 06 Nov 1994 08:49:37 GMT", "Sunday, 06-Nov-94 08:49:37 GMT", "Sun Nov  6 08:49:37 1994"]
        {
            assert_eq!(ra(date, DATE_UNIX - 90), Some(Duration::from_secs(90)), "{date}");
            assert_eq!(ra(date, DATE_UNIX + 5), Some(Duration::ZERO), "{date}");
            assert_eq!(ra(date, 0), Some(MAX_RETRY_AFTER), "{date}");
        }
    }

    #[test]
    fn an_exhausted_window_is_read_from_every_spelling() {
        let now = at(1_789_639_200);
        let epoch = (1_789_639_200u64 + 45).to_string();
        let spent = |pairs: &[(&'static str, &str)]| exhausted_for(&headers(pairs), now);
        let secs = |n: u64| Some(Duration::from_secs(n));

        assert_eq!(spent(&[("x-ratelimit-remaining", "0"), ("x-ratelimit-reset", "30")]), secs(30));
        // A unix-time reset reads as one.
        assert_eq!(spent(&[("x-ratelimit-remaining", "0"), ("x-ratelimit-reset", &epoch)]), secs(45));
        assert_eq!(spent(&[("ratelimit-remaining", "0"), ("ratelimit-reset", "4")]), secs(4));
        assert_eq!(spent(&[("ratelimit", "limit=100, remaining=0, reset=50")]), secs(50));
        assert_eq!(spent(&[("ratelimit", "\"default\";r=0;t=20")]), secs(20));
        // The draft field wins over the headers beside it.
        assert_eq!(spent(&[("ratelimit", "\"h\";r=0;t=9"), ("x-ratelimit-remaining", "5")]), secs(9));
        // A day is an hour.
        assert_eq!(
            spent(&[("x-ratelimit-remaining", "0"), ("x-ratelimit-reset", "86400")]),
            Some(MAX_RETRY_AFTER)
        );

        // Something left, nothing said, no reset to wait for, or a reset already past: no pause.
        assert_eq!(spent(&[("x-ratelimit-remaining", "3"), ("x-ratelimit-reset", "30")]), None);
        assert_eq!(spent(&[("ratelimit", "\"h\";r=4;t=90"), ("x-ratelimit-remaining", "0")]), None);
        assert_eq!(spent(&[]), None);
        assert_eq!(spent(&[("x-ratelimit-remaining", "0")]), None);
        assert_eq!(spent(&[("x-ratelimit-remaining", "0"), ("x-ratelimit-reset", "0")]), None);
        assert_eq!(spent(&[("x-ratelimit-remaining", "0"), ("x-ratelimit-reset", "1700000000")]), None);
    }

    #[test]
    fn only_a_429_or_a_503_that_names_a_wait_throttles() {
        let now = at(DATE_UNIX);
        let ra = |v: &'static str| headers(&[("retry-after", v)]);
        let wait = |n: u64| Some(Throttle::Stated(Duration::from_secs(n)));

        assert_eq!(throttle(StatusCode::TOO_MANY_REQUESTS, &HeaderMap::new(), now), Some(Throttle::Unstated));
        assert_eq!(throttle(StatusCode::TOO_MANY_REQUESTS, &ra("120"), now), wait(120));
        assert_eq!(throttle(StatusCode::SERVICE_UNAVAILABLE, &ra("120"), now), wait(120));
        // A bare 503 and every other 5xx is one request failing.
        for status in [500, 502, 503, 504] {
            let status = StatusCode::from_u16(status).unwrap();
            assert_eq!(throttle(status, &HeaderMap::new(), now), None, "{status}");
        }
        assert_eq!(throttle(StatusCode::INTERNAL_SERVER_ERROR, &ra("120"), now), None);
        // "Retry now" is not a wait: a 429 falls to the caller's window, a 503 is no refusal at all.
        for zero in ["0", "Sun, 06 Nov 1994 08:00:00 GMT"] {
            assert_eq!(
                throttle(StatusCode::TOO_MANY_REQUESTS, &ra(zero), now),
                Some(Throttle::Unstated),
                "{zero}"
            );
            assert_eq!(throttle(StatusCode::SERVICE_UNAVAILABLE, &ra(zero), now), None, "{zero}");
        }
        assert_eq!(throttle(StatusCode::OK, &ra("120"), now), None);
    }
}
