//! One clock grid for periodic timers.
//!
//! A timer on the grid of `every` ticks when the wall clock reads a whole
//! multiple of `every` since the Unix epoch. Timers whose periods divide each
//! other fire together, in one wakeup, and so do the timers of processes on
//! machines whose clocks agree, such as the heartbeats a publisher and its
//! subscribers send. On a battery-powered machine a few wakeups cost far less
//! than as many spread out.
//!
//! The wall clock is read once, when the grid is first used; it moves with the
//! monotonic clock after that, so a step of the wall clock doesn't move it.

use std::{
    sync::LazyLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::{self, Instant, Interval, MissedTickBehavior};

/// An instant, and the wall clock at it, in ns since the Unix epoch.
static REFERENCE: LazyLock<(Instant, u128)> = LazyLock::new(|| {
    let wall = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    (Instant::now(), wall.as_nanos())
});

/// The first tick of the grid of `every` after `after`.
pub fn next(every: Duration, after: Instant) -> Instant {
    let (at, wall) = *REFERENCE;
    let wall = match after.checked_duration_since(at) {
        Some(d) => wall + d.as_nanos(),
        None => wall.saturating_sub(at.duration_since(after).as_nanos()),
    };
    let every_ns = every.as_nanos().max(1);
    after + Duration::from_nanos((every_ns - wall % every_ns) as u64)
}

/// An interval on the grid of `every`, first ticking at the first tick
/// after `start`. Ticks missed, while it wasn't polled or was slow, are
/// skipped, so it stays on the grid.
pub fn interval_at(start: Instant, every: Duration) -> Interval {
    let mut i = time::interval_at(next(every, start), every);
    i.set_missed_tick_behavior(MissedTickBehavior::Skip);
    i
}

/// An interval on the grid of `every`, first ticking at its next tick.
pub fn interval(every: Duration) -> Interval {
    interval_at(Instant::now(), every)
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn ticks_fall_on_whole_multiples_of_the_period_in_wall_time() {
        let now = Instant::now();
        let second = Duration::from_secs(1);
        let t = next(second, now);
        assert!(t > now && t <= now + second);
        assert_eq!(next(second, t), t + second, "a tick is not after itself");
        // The wall clock at the tick reads a whole second.
        let (at, wall) = *REFERENCE;
        let wall_at_t = wall + t.duration_since(at).as_nanos();
        assert_eq!(wall_at_t % second.as_nanos(), 0);
        // A grid whose period divides another's ticks with it.
        let minute = next(Duration::from_secs(60), now);
        assert_eq!(next(second, minute - Duration::from_millis(1)), minute);
        let mut quarter = next(Duration::from_millis(250), now);
        while quarter < t {
            quarter = next(Duration::from_millis(250), quarter);
        }
        assert_eq!(quarter, t);
        // An instant before the grid was first used.
        let before = at - Duration::from_millis(300);
        let wall_before = wall - 300_000_000;
        let t = next(second, before);
        assert_eq!((wall_before + t.duration_since(before).as_nanos()) % second.as_nanos(), 0);
    }

    #[tokio::test]
    async fn an_interval_first_ticks_after_its_start() {
        let start = Instant::now() + Duration::from_millis(50);
        let mut i = interval_at(start, Duration::from_millis(20));
        let first = i.tick().await;
        assert!(first > start, "{first:?} is not after {start:?}");
        assert_eq!(first, next(Duration::from_millis(20), start));
    }
}
