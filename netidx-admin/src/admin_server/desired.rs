//! What the CA says a server's configuration should be, and how a server gets
//! there.
//!
//! The admin plane has one direction: the CA is authoritative, and a member
//! converges on it by polling. That poll already exists — every admin server
//! registers with the CA every 30s, reporting the versions of the state it
//! holds — so this module is only the two halves that hang off it.
//! [`updates_for`] runs at the CA and answers "what is this server missing";
//! [`apply`] runs at the member and installs it.
//!
//! Nothing is pushed. A member that was down for an edit is afterwards just a
//! member behind a version, and the next register repairs it without an
//! operator doing anything and without the CA needing to reach inward.

use super::Server;
use crate::admin_proto::{
    AdminServerId, DesiredUpdate, RegisterRequest, Role, VersionedIdMap, VersionedPerms,
    VersionedResolverConfig,
};
use anyhow::{Context, Result};
use std::sync::Arc;

/// What a server turned out to be missing, and what the CA wants it at.
pub(crate) struct Missing {
    pub(crate) updates: DesiredUpdate,
    /// The config version the CA has rendered for this server, whether or not
    /// it needed sending. Recorded in the map beside what the server reports,
    /// so who is behind is answerable from the map alone.
    pub(crate) config_version: Option<u64>,
}

/// CA-side: what `server` does not yet have, given what it reported.
///
/// Cheap in the steady state — a server whose reported versions match the
/// models costs two integer comparisons and no file reads. That matters
/// because this runs on every register from every server.
pub(crate) async fn updates_for(
    state: &Arc<Server>,
    server: AdminServerId,
    req: &RegisterRequest,
) -> Result<Missing> {
    let (cluster, holds_id_map) = state
        .read(move |state| {
            state
                .map
                .admin_servers
                .iter()
                .find(|s| s.id == server)
                .map(|s| (s.cluster, s.roles.contains(Role::IdMap)))
        })
        .await
        .with_context(|| format!("server {server} has no approved enrollment grant"))?;
    let mut updates = DesiredUpdate::default();
    if let Some(cluster) = cluster {
        let reported = req.perms_version;
        updates.perms = state
            .read_async(async move |state| match state.ca.as_ref() {
                Some(ca) => ca.store.perms_model().await.ok(),
                None => None,
            })
            .await
            .and_then(|model| {
                model.behind(cluster, reported).then(|| {
                    let p = model.get(cluster).expect("behind implies established");
                    VersionedPerms { version: p.version, perms: p.perms.clone() }
                })
            });
    }
    // Re-render this server's resolver config from its stored block and the
    // map's current topology, every time. Rendering is a pure function of the
    // two, so doing it here rather than fanning out over affected servers when
    // topology moves means there is nothing to keep in sync — and the version
    // advances only when the document actually differs, so a member chasing it
    // can arrive.
    let rendered = match render_config(state, server).await {
        Ok(rendered) => rendered,
        Err(e) => {
            log::warn!("admin-server: rendering the config for {server}: {e:#}");
            None
        }
    };
    let config_version = rendered.as_ref().map(|(version, _)| *version);
    updates.config = rendered
        .filter(|(version, _)| req.config_version.is_none_or(|have| have < *version))
        .map(|(version, config)| VersionedResolverConfig { version, config });
    if holds_id_map {
        let reported = req.id_map_version;
        updates.id_map = state
            .read_async(async move |state| match state.ca.as_ref() {
                Some(ca) => ca.store.id_map_model().await.ok(),
                None => None,
            })
            .await
            .and_then(|model| {
                let behind = model.established()
                    && reported.is_none_or(|have| have < model.version);
                behind.then(|| VersionedIdMap {
                    version: model.version,
                    id_map: model.shape().clone(),
                })
            });
    }
    Ok(Missing { updates, config_version })
}

/// Render `server`'s resolver config and record it, returning the version it
/// is now at. `None` when there is nothing stored to render from — a server
/// with no resolver, or one that enrolled before the CA kept these.
async fn render_config(
    state: &Arc<Server>,
    server: AdminServerId,
) -> Result<Option<(u64, netidx::resolver_server::config::file::Config)>> {
    state
        .write_async(async move |state| {
            let Some(ca) = state.ca.as_ref() else { return Ok(None) };
            let mut stored = ca.store.desired_configs().await?;
            let Some(config) =
                crate::desired_config::render(&state.map, server, &stored, |cluster| {
                    super::topology::referrals_for(&state.map, cluster)
                })
            else {
                return Ok(None);
            };
            let (version, changed) = stored.set(server, config.clone());
            if changed {
                ca.store.save_desired_configs(&stored).await?;
            }
            Ok(Some((version, config)))
        })
        .await
}

/// Take ownership of the CA host's own resolver config, once.
///
/// Every other host hands its config to the CA at enrollment. The CA never
/// enrolls, so without this there is nothing stored for it, [`render_config`]
/// returns `None` on its first line, and the CA is the single host whose
/// resolver config the CA does not own — the one host for which the topology
/// push really is the mechanism rather than an optimization over the poll.
///
/// Seeded only when absent. It adopts the installed document once and is
/// authoritative from then on, so this cannot become a way for an edit made on
/// the CA's disk to overwrite what the CA decided.
async fn adopt_own_config(state: &Arc<Server>) -> Result<()> {
    let (server, path) = state
        .read(|state| {
            (
                state.cfg.server_id,
                state.cfg.roles.resolver.as_ref().map(|r| r.config.clone()),
            )
        })
        .await;
    let Some(path) = path else { return Ok(()) };
    let already = state
        .read_async(async move |state| match state.ca.as_ref() {
            Some(ca) => {
                ca.store.desired_configs().await.ok().map(|c| c.get(server).is_some())
            }
            None => None,
        })
        .await;
    if already != Some(false) {
        return Ok(());
    }
    let config = crate::resolver::ResolverConfig::load_async(&path)
        .await
        .with_context(|| format!("reading our own resolver config {}", path.display()))?
        .into_file();
    state
        .write_async(async move |state| {
            let Some(ca) = state.ca.as_ref() else { return Ok(()) };
            let mut stored = ca.store.desired_configs().await?;
            if stored.get(server).is_some() {
                return Ok(());
            }
            stored.set(server, config);
            ca.store.save_desired_configs(&stored).await
        })
        .await
        .context("recording the CA host's own resolver config")
}

/// The CA bringing itself up to its own models.
///
/// Same two halves as everyone else — work out what is missing, install it —
/// with the round trip removed. Going through [`updates_for`] and [`apply`]
/// rather than a shortcut is the point: a second path here is how the host
/// holding the models would come to disagree with them.
pub(crate) async fn converge_self(state: &Arc<Server>) -> Result<()> {
    adopt_own_config(state).await?;
    let (server, addr) =
        state.read(|state| (state.cfg.server_id, state.cfg.listen)).await;
    let req = RegisterRequest {
        addr,
        resolver: None,
        id_map_version: state.applied_id_map_version().await,
        perms_version: state.applied_perms_version().await,
        config_version: state.applied_config_version().await,
    };
    let missing = updates_for(state, server, &req).await?;
    if missing.updates.is_empty() { Ok(()) } else { apply(state, &missing.updates).await }
}

/// Member-side: install what the CA handed back.
///
/// Each part is applied and stamped on its own. A part that fails leaves its
/// version unstamped, so the server reports itself still behind on that part
/// and only that part — the next register brings it again. Reporting the
/// failure rather than swallowing it is what gets an operator an error instead
/// of a server that quietly never converges.
pub(crate) async fn apply(state: &Arc<Server>, updates: &DesiredUpdate) -> Result<()> {
    let DesiredUpdate { perms, id_map, config } = updates;
    let mut failures = Vec::new();
    // The resolver config first: perms are written to a path this document
    // names, so installing them against a stale one would put them where
    // nothing reads.
    if let Some(c) = config
        && let Err(e) = install_config(state, &c.config, c.version).await
    {
        failures.push(format!("resolver config (version {}): {e:#}", c.version));
    }
    if let Some(p) = perms
        && let Err(e) =
            super::permissions::install_perms(state, &p.perms, Some(p.version)).await
    {
        failures.push(format!("permissions (version {}): {e:#}", p.version));
    }
    if let Some(m) = id_map
        && let Err(e) = super::id_map::install_id_map(state, &m.id_map, m.version).await
    {
        failures.push(format!("id-map (version {}): {e:#}", m.version));
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("applying the configuration the CA handed back: {}", failures.join("; "))
    }
}

/// Write the CA's rendered config into this host's resolver, stamping
/// `version` on success.
///
/// Validated before it is written — `ResolverConfig::validate` runs the
/// resolver's own `Config::from_file`, which opens the certificate and key the
/// document names. A config this host cannot actually load is refused, and
/// refusing leaves the version unstamped so the next register brings it again
/// rather than the host quietly running on a file it could not parse.
async fn install_config(
    state: &Arc<Server>,
    config: &netidx::resolver_server::config::file::Config,
    version: u64,
) -> Result<()> {
    let path = state
        .read(move |state| state.cfg.roles.resolver.as_ref().map(|r| r.config.clone()))
        .await
        .context("this host has no resolver role — nothing to install into")?;
    let path = state.config_lock.require_contained(path)?;
    let rc = crate::resolver::ResolverConfig::from_file(config.clone());
    rc.validate_for_path(&path)
        .context("the CA sent a resolver config this host cannot load")?;
    rc.save_async(&path).await?;
    crate::version_stamp::record(&path, version).await
}
