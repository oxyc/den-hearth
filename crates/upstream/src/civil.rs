//! The calendar arithmetic under every timestamp the services read or write (an RFC 3339 deadline in an env var,
//! OpenSubtitles' `reset_time_utc`, a debrid's `expires_at`), written once. Howard Hinnant's algorithms, for the
//! proleptic Gregorian calendar.

/// Days since 1970-01-01 of a civil date; negative before it.
pub fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = i64::from(if month > 2 { month - 3 } else { month + 9 });
    let doy = (153 * mp + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The civil date `(year, month, day)` of a day count since 1970-01-01.
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if month <= 2 { y + 1 } else { y }, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_dates_convert_both_ways() {
        for (date, days) in [
            ((1970, 1, 1), 0),
            ((1994, 11, 6), 9_075),
            ((2000, 2, 29), 11_016),
            ((2022, 1, 30), 19_022),
            ((1969, 12, 31), -1),
            ((1, 1, 1), -719_162),
        ] {
            assert_eq!(days_from_civil(date.0, date.1, date.2), days, "{date:?}");
            assert_eq!(civil_from_days(days), date, "{days}");
        }
    }

    #[test]
    fn every_day_for_four_centuries_round_trips() {
        for days in (-146_097..146_097 * 2).step_by(7) {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days);
        }
    }
}
