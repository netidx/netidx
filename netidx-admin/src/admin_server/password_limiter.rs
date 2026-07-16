use anyhow::{Result, bail};
use std::{
    collections::{HashMap, VecDeque},
    net::{IpAddr, Ipv6Addr},
    time::{Duration, Instant},
};

const FAILURE_WINDOW: Duration = Duration::from_secs(10 * 60);
const BACKOFF_STEP: Duration = Duration::from_secs(1);
const MAX_SOURCES: usize = 16_384;

#[derive(Debug)]
struct Entry {
    failures: VecDeque<Instant>,
    in_flight: bool,
    last_seen: Instant,
}

#[derive(Debug, Default)]
pub(super) struct PasswordLimiter {
    entries: HashMap<IpAddr, Entry>,
}

impl PasswordLimiter {
    pub(super) fn reserve(&mut self, source: IpAddr) -> Result<Duration> {
        self.reserve_at(source, Instant::now())
    }

    pub(super) fn complete(&mut self, source: IpAddr, success: Option<bool>) {
        self.complete_at(source, success, Instant::now())
    }

    fn reserve_at(&mut self, source: IpAddr, now: Instant) -> Result<Duration> {
        let source = source_key(source);
        let entries = &mut self.entries;
        if entries.len() >= MAX_SOURCES && !entries.contains_key(&source) {
            entries.retain(|_, entry| {
                entry.in_flight
                    || now.saturating_duration_since(entry.last_seen) < FAILURE_WINDOW
            });
            if entries.len() >= MAX_SOURCES
                && let Some(oldest) = entries
                    .iter()
                    .filter(|(_, entry)| !entry.in_flight)
                    .min_by_key(|(_, entry)| entry.last_seen)
                    .map(|(source, _)| *source)
            {
                entries.remove(&oldest);
            }
        }
        let entry = entries.entry(source).or_insert_with(|| Entry {
            failures: VecDeque::new(),
            in_flight: false,
            last_seen: now,
        });
        prune(entry, now);
        if entry.in_flight {
            bail!("another password attempt from this source is already in progress");
        }
        let delay = entry
            .failures
            .back()
            .map(|last| {
                let penalty = BACKOFF_STEP
                    .saturating_mul(entry.failures.len().min(u32::MAX as usize) as u32)
                    .min(FAILURE_WINDOW);
                last.checked_add(penalty)
                    .map(|eligible| eligible.saturating_duration_since(now))
                    .unwrap_or(FAILURE_WINDOW)
            })
            .unwrap_or(Duration::ZERO);
        entry.in_flight = true;
        entry.last_seen = now;
        Ok(delay)
    }

    fn complete_at(&mut self, source: IpAddr, success: Option<bool>, now: Instant) {
        let Some(entry) = self.entries.get_mut(&source_key(source)) else { return };
        entry.in_flight = false;
        entry.last_seen = now;
        prune(entry, now);
        if success == Some(false) {
            entry.failures.push_back(now);
        }
    }
}

fn source_key(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(ip) => IpAddr::V4(ip),
        IpAddr::V6(ip) => {
            let mut octets = ip.octets();
            octets[8..].fill(0);
            IpAddr::V6(Ipv6Addr::from(octets))
        }
    }
}

fn prune(entry: &mut Entry, now: Instant) {
    while entry
        .failures
        .front()
        .is_some_and(|failure| now.saturating_duration_since(*failure) >= FAILURE_WINDOW)
    {
        entry.failures.pop_front();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failures_apply_linear_backoff_and_expire_on_the_sliding_window() {
        let mut limiter = PasswordLimiter::default();
        let source: IpAddr = "192.0.2.10".parse().unwrap();
        let start = Instant::now();

        assert_eq!(limiter.reserve_at(source, start).unwrap(), Duration::ZERO);
        assert!(limiter.reserve_at(source, start).is_err(), "only one KDF per IP");
        limiter.complete_at(source, Some(false), start);

        assert_eq!(limiter.reserve_at(source, start).unwrap(), Duration::from_secs(1));
        limiter.complete_at(source, Some(false), start + Duration::from_secs(1));
        assert_eq!(
            limiter.reserve_at(source, start + Duration::from_secs(1)).unwrap(),
            Duration::from_secs(2)
        );
        limiter.complete_at(source, Some(true), start + Duration::from_secs(3));
        assert_eq!(
            limiter.reserve_at(source, start + Duration::from_secs(3)).unwrap(),
            Duration::ZERO
        );
        limiter.complete_at(source, Some(false), start + Duration::from_secs(3));
        assert_eq!(
            limiter.reserve_at(source, start + Duration::from_secs(3)).unwrap(),
            Duration::from_secs(3)
        );
        limiter.complete_at(source, Some(true), start + Duration::from_secs(6));

        let expired = start + FAILURE_WINDOW + Duration::from_secs(4);
        assert_eq!(limiter.reserve_at(source, expired).unwrap(), Duration::ZERO);
        limiter.complete_at(source, Some(true), expired);
    }

    #[test]
    fn ipv6_privacy_addresses_share_a_64_bit_source_key() {
        let mut limiter = PasswordLimiter::default();
        let start = Instant::now();
        let first: IpAddr = "2001:db8:1234:5678::1".parse().unwrap();
        let same_64: IpAddr = "2001:db8:1234:5678:ffff::2".parse().unwrap();
        let other_64: IpAddr = "2001:db8:1234:5679::1".parse().unwrap();

        limiter.reserve_at(first, start).unwrap();
        assert!(limiter.reserve_at(same_64, start).is_err());
        assert_eq!(limiter.reserve_at(other_64, start).unwrap(), Duration::ZERO);
        limiter.complete_at(first, Some(true), start);
        limiter.complete_at(other_64, Some(true), start);
    }

    #[test]
    fn completion_without_a_result_releases_the_source_without_a_failure() {
        let mut limiter = PasswordLimiter::default();
        let source: IpAddr = "198.51.100.7".parse().unwrap();
        let now = Instant::now();
        limiter.reserve_at(source, now).unwrap();
        limiter.complete_at(source, None, now);
        assert_eq!(limiter.reserve_at(source, now).unwrap(), Duration::ZERO);
        limiter.complete_at(source, Some(true), now);
    }
}
