//! Follow the resolver addresses in a config file.
//!
//! A resolver cluster gains and loses members, and the address list in
//! `client.json` is where that shows up. Re-reading it means a publisher or
//! subscriber picks the change up on its own instead of waiting to be
//! restarted.
//!
//! Only the addresses are followed. Everything else in the file describes what
//! the process was started with — where it is mounted in the namespace, how it
//! authenticates, which identities it loaded — and changing any of that under
//! a running process would be a different cluster or a different trust
//! decision, not a new address list. Those changes are refused, with a warning,
//! and the process keeps what it has until it is restarted.

use super::{Config, Origin, Tls};
use crate::{
    protocol::resolver::{Auth, Referral},
    tls,
};
use anyhow::{Context, Result, bail};
use log::{info, warn};
use poolshark::{global::GPooled, local::LPooled};
use std::{
    mem::{self, Discriminant},
    path::Path as FsPath,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{sync::watch, task, time};

/// How often the origin file is checked. The list it holds changes at most a
/// couple of times an hour even in an actively maintained admin domain, so
/// this is about how quickly an operator sees their edit take effect, not
/// about keeping up.
#[cfg(not(test))]
pub(crate) const POLL_INTERVAL: Duration = Duration::from_secs(30);
/// The in-crate tests drive real config edits through this loop, so they run
/// it at a pace a test can wait for.
#[cfg(test)]
pub(crate) const POLL_INTERVAL: Duration = Duration::from_millis(100);

fn auth_kind(auth: &Auth) -> Discriminant<Auth> {
    mem::discriminant(auth)
}

/// Can `next` replace the addresses `current` is running with?
///
/// Everything here is a case where the file has been changed into something
/// this process cannot act on without restarting, so the honest answer is to
/// keep running with what we have and say so.
fn acceptable(current: &Config, next: &Config) -> Result<()> {
    if next.base != current.base {
        bail!(
            "base changed from {} to {}; that is a different cluster",
            current.base,
            next.base
        )
    }
    if mem::discriminant(&next.default_auth) != mem::discriminant(&current.default_auth) {
        bail!(
            "default_auth changed from {:?} to {:?}",
            current.default_auth,
            next.default_auth
        )
    }
    let allowed = current
        .addrs
        .iter()
        .map(|(_, auth)| auth_kind(auth))
        .collect::<LPooled<Vec<_>>>();
    for (addr, auth) in next.addrs.iter() {
        if !allowed.contains(&auth_kind(auth)) {
            bail!("{addr} would use {auth:?}, which this process did not start with")
        }
        // A tls identity is loaded once, at startup. A new member covered by
        // an identity we already hold is fine — that is the ordinary case,
        // since identities are keyed by domain — but one that isn't could
        // only fail at connect time.
        if let Auth::Tls { name } = auth {
            let Some(loaded) = current.tls.as_ref() else {
                bail!("{addr} wants tls, but no tls identities are loaded")
            };
            let mut reversed: LPooled<String> = LPooled::take();
            reversed.push_str(name);
            Tls::reverse_domain_name(&mut reversed);
            if tls::get_match(&loaded.identities, &reversed).is_none() {
                bail!("no loaded tls identity covers {name}; restart to use {addr}")
            }
        }
    }
    Ok(())
}

/// The file's modification time, or `None` if it isn't there — so a config
/// that disappears and comes back is noticed. One `stat` per interval.
async fn stamp(path: &FsPath) -> Option<SystemTime> {
    tokio::fs::metadata(path).await.ok()?.modified().ok()
}

/// What we ran with, or `None` for a config that came from somewhere else.
/// Compared against [`stamp`] to decide whether to look again.
fn running(cfg: &Config) -> Option<SystemTime> {
    cfg.origin.mtime()
}

/// Re-read `path` and return the new config if its addresses are both usable
/// and different from what we are running with.
fn reload(path: &FsPath, current: &Config) -> Result<Option<Config>> {
    let next = Config::load(path).context("re-reading")?;
    acceptable(current, &next)?;
    Ok(if next.addrs == current.addrs { None } else { Some(next) })
}

async fn poll(mut current: Config, path: Arc<FsPath>, tx: watch::Sender<Arc<Referral>>) {
    // What we started from comes from the load, not from a fresh look at the
    // path — see `crate::config_file`.
    let mut last = running(&current);
    // Look again on the next tick after a failure even if nothing else moved.
    // A hand-edited file can be caught mid-write, and the whole point of
    // following it is that an edit takes effect — one retry covers that
    // without turning a genuinely broken config into a warning every tick
    // forever.
    let mut retry = false;
    loop {
        tokio::select! {
            _ = tx.closed() => break,
            _ = time::sleep(POLL_INTERVAL) => (),
        }
        let now = stamp(&path).await;
        if now == last && !retry {
            continue;
        }
        retry = false;
        let reloaded = task::spawn_blocking({
            let path = path.clone();
            let current = current.clone();
            move || reload(&path, &current)
        })
        .await;
        match reloaded {
            Err(e) => {
                last = now;
                warn!("{}: reload task failed: {e}", path.display())
            }
            Ok(Err(e)) => {
                last = now;
                retry = true;
                warn!("{}: keeping the current resolvers: {e:#}", path.display())
            }
            // Nothing we can act on changed, but record what we looked at so
            // we don't re-read it every interval from here on.
            Ok(Ok(None)) => last = now,
            Ok(Ok(Some(next))) => {
                info!("{}: resolvers are now {:?}", path.display(), next.addrs);
                // From the descriptor the new config was read from, not from
                // `now` — a write between the stat and the read would
                // otherwise be remembered as already applied.
                last = running(&next);
                let addrs = next.addrs.clone();
                current = next;
                let referral = Referral {
                    path: current.base.clone(),
                    ttl: None,
                    addrs: GPooled::orphan(addrs),
                };
                let _ = tx.send(Arc::new(referral));
            }
        }
    }
}

/// Follow `cfg`'s origin file, publishing a new default referral on `tx`
/// whenever its resolver addresses change.
///
/// For a config that has no origin — built in memory, or deliberately
/// detached — `tx` is simply dropped. Its receivers keep working and just
/// never see a change, so callers need no second code path.
pub(crate) fn follow(cfg: &Config, tx: watch::Sender<Arc<Referral>>) {
    let Origin::File { path, .. } = cfg.origin.clone() else { return };
    let current = cfg.clone();
    task::spawn(poll(current, path, tx));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DefaultAuthMech, file};
    use arcstr::ArcStr;

    /// The stamp a follower starts from has to come from the load, so that a
    /// write landing between reading the file and describing it is not
    /// mistaken for something already applied.
    #[test]
    fn a_loaded_config_remembers_when_it_was_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("client.json");
        std::fs::write(
            &path,
            br#"{"base":"/","addrs":[["127.0.0.1:4564","Anonymous"]],"default_auth":"Anonymous"}"#,
        )
        .unwrap();
        let cfg = Config::load(&path).unwrap();
        let loaded = running(&cfg).expect("a file origin records its mtime");
        assert_eq!(
            Some(loaded),
            std::fs::metadata(&path).unwrap().modified().ok(),
            "the recorded time must be the file we read",
        );
        // Detaching drops the whole relationship, stamp included.
        let mut detached = cfg.clone();
        detached.detach();
        assert_eq!(running(&detached), None);
    }

    fn cfg(base: &str, addrs: &[(&str, file::Auth)], auth: DefaultAuthMech) -> Config {
        let f = file::ConfigBuilder::default()
            .base(base)
            .addrs(
                addrs
                    .iter()
                    .map(|(a, auth)| (a.parse().unwrap(), auth.clone()))
                    .collect::<Vec<_>>(),
            )
            .default_auth(auth)
            .build()
            .unwrap();
        Config::from_file(f).unwrap()
    }

    fn anon(base: &str, addrs: &[&str]) -> Config {
        let addrs = addrs.iter().map(|a| (*a, file::Auth::Anonymous)).collect::<Vec<_>>();
        cfg(base, &addrs, DefaultAuthMech::Anonymous)
    }

    #[test]
    fn adding_and_removing_members_is_accepted() {
        let one = anon("/", &["192.0.2.1:4564"]);
        let two = anon("/", &["192.0.2.1:4564", "192.0.2.2:4564"]);
        let other = anon("/", &["192.0.2.2:4564"]);
        acceptable(&one, &two).unwrap();
        acceptable(&two, &one).unwrap();
        acceptable(&one, &other).unwrap();
    }

    #[test]
    fn a_different_base_is_a_different_cluster() {
        let here = anon("/eu", &["192.0.2.1:4564"]);
        let there = anon("/ap", &["192.0.2.1:4564"]);
        assert!(acceptable(&here, &there).is_err());
    }

    #[test]
    fn the_auth_mechanism_may_not_change() {
        let anonymous = anon("/", &["192.0.2.1:4564"]);
        let krb5 = cfg(
            "/",
            &[("192.0.2.1:4564", file::Auth::Krb5(ArcStr::from("netidx/host")))],
            DefaultAuthMech::Anonymous,
        );
        // Per address...
        assert!(acceptable(&anonymous, &krb5).is_err());
        assert!(acceptable(&krb5, &anonymous).is_err());
        // ...and the process default.
        let default_changed = anon("/", &["192.0.2.1:4564"]);
        let mut default_changed = default_changed;
        default_changed.default_auth = DefaultAuthMech::Krb5;
        assert!(acceptable(&anonymous, &default_changed).is_err());
    }

    #[test]
    fn a_new_member_may_not_bring_an_unloaded_tls_identity() {
        // No identities are loaded at all, which is the reachable case: an
        // anonymous or krb5 client whose config sprouts a tls address.
        let anonymous = anon("/", &["192.0.2.1:4564"]);
        let mut with_tls = anonymous.clone();
        with_tls.addrs.push((
            "192.0.2.2:4564".parse().unwrap(),
            Auth::Tls { name: ArcStr::from("resolver.example.com") },
        ));
        assert!(acceptable(&anonymous, &with_tls).is_err());
    }
}
