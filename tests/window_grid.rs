//! The 6-hour window grid: where a window starts for any instant.

use jiff::{SignedDuration, Timestamp};
use patina::scheduler::window_floor;

fn utc(y: i16, mo: i8, d: i8, h: i8, mi: i8, s: i8) -> Timestamp {
    jiff::civil::datetime(y, mo, d, h, mi, s, 0)
        .in_tz("UTC")
        .unwrap()
        .timestamp()
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
    let t = utc(2026, 11, 1, 5, 59, 59) + SignedDuration::from_millis(999);
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

#[test]
fn a_time_just_before_1970_belongs_to_the_window_before_it() {
    // -0.5 s: the window is [-6 h, 0), not [0, 6 h).
    let t = Timestamp::new(0, -500_000_000).unwrap();
    assert_eq!(window_floor(t), Timestamp::new(-6 * 3600, 0).unwrap());
}
