//! The 6-hour window grid: where a window starts for any instant.

use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use patina::scheduler::window_floor;

fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, mo, d, h, mi, s).unwrap()
}

#[test]
fn a_window_starts_on_the_six_hour_grid_in_utc() {
    let cases = [
        (utc(2026, 11, 1, 0, 0, 0), utc(2026, 11, 1, 0, 0, 0)), // on the grid
        (utc(2026, 11, 1, 0, 0, 1), utc(2026, 11, 1, 0, 0, 0)),
        (utc(2026, 11, 1, 5, 59, 59), utc(2026, 11, 1, 0, 0, 0)), // last second of a window
        (utc(2026, 11, 1, 6, 0, 0), utc(2026, 11, 1, 6, 0, 0)),   // first second of the next
        (utc(2026, 11, 1, 7, 0, 0), utc(2026, 11, 1, 6, 0, 0)),
        (utc(2026, 11, 1, 23, 59, 59), utc(2026, 11, 1, 18, 0, 0)),
    ];
    for (t, expected) in cases {
        assert_eq!(window_floor(t), expected, "for {t}");
    }
}

#[test]
fn a_fraction_of_a_second_does_not_move_the_window() {
    let t = utc(2026, 11, 1, 5, 59, 59) + TimeDelta::milliseconds(999);
    assert_eq!(window_floor(t), utc(2026, 11, 1, 0, 0, 0));
}

#[test]
fn times_before_1970_round_down_not_towards_zero() {
    assert_eq!(
        window_floor(utc(1969, 12, 31, 23, 59, 59)),
        utc(1969, 12, 31, 18, 0, 0)
    );
    assert_eq!(
        window_floor(utc(1969, 12, 31, 18, 0, 0)),
        utc(1969, 12, 31, 18, 0, 0)
    );
}
