#[cfg(test)]
#[path = "revocation_tests.rs"]
mod tests;

use super::{
    MutableState, PUSH_TIMEOUT, Server, audit,
    auth::{
        PreparedAdminAuthentication, PreparedServerUnlock, admin_authority_over,
        authenticate, broad_admin, safe_auth_failure, scope_covers, server_unlock,
    },
    ca_dir,
};
use crate::{
    admin_proto::{
        self, ApplyCrlRequest, ApplyCrlResponse, GetCrlResponse, PeerResult, RevokeOk,
        RevokeRequest, RevokeResponse, SERVING_SAN,
    },
    admin_server_config::AdminServerConfig,
    ca_store,
    config_lock::ConfigDirLock,
    transport,
};
use anyhow::{Context, Result, anyhow, bail};
use futures::{StreamExt, stream};
use log::{info, warn};
use std::{
    collections::BTreeSet,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

/// Revoke every still-live serving certificate carrying `server_id`.
pub(super) async fn revoke_server_certificates(
    ca: &mut ca_store::CaDir,
    server_id: admin_proto::AdminServerId,
    admin: &str,
    prepared_server_unlock: &PreparedServerUnlock,
) -> Result<usize> {
    let signing = server_unlock(ca, prepared_server_unlock)
        .await
        .map_err(|reason| anyhow!(reason))?;
    let now = ca_store::now_unix();
    let store = &mut ca.store;
    let serials: Vec<_> = store
        .list_signed()
        .await
        .context("reading the issuance index")?
        .into_iter()
        .filter(|record| {
            record.live(now)
                && record.name.eq_ignore_ascii_case(SERVING_SAN)
                && crate::tls::admin_cert_identity_from_pem(record.cert_pem.as_bytes())
                    .is_ok_and(|identity| identity.server_id == server_id)
        })
        .map(|record| record.serial)
        .collect();
    let mut revoked = 0;
    for serial in serials {
        let revocation = ca_store::Revocation {
            serial,
            revoked_unix: now,
            reason: format!("admin server {server_id} removed by {admin}"),
        };
        if store.revoke(serial, revocation).await? {
            revoked += 1;
        }
    }
    if revoked > 0 {
        store
            .write_crl(&signing.ca_key_pem)
            .await
            .context("publishing the updated CRL")?;
    }
    Ok(revoked)
}

pub(super) async fn handle_get_crl(state: &Arc<Server>) -> GetCrlResponse {
    if ca_dir(state).await.is_none() {
        return GetCrlResponse { crl_pem: None };
    }
    let path = state
        .read(move |state| state.ca.as_ref().expect("CA role held").store.crl_path())
        .await;
    match tokio::fs::read_to_string(path).await {
        Ok(pem) => GetCrlResponse { crl_pem: Some(pem) },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            GetCrlResponse { crl_pem: None }
        }
        Err(e) => {
            warn!("admin-server: reading the CRL failed: {e:#}");
            GetCrlResponse { crl_pem: None }
        }
    }
}

struct PreparedRevoke {
    admin: String,
    warnings: Vec<String>,
    crl_pem: Option<String>,
}

/// Authenticate, apply the requested serial revocations, and re-sign the CRL.
/// The async wrapper below performs the trust domain fanout after this
/// Argon2/signing-bound phase releases the signing semaphore.
async fn prepare_revoke(
    state: &Server,
    req: &RevokeRequest,
    authentication: &PreparedAdminAuthentication,
    prepared_server_unlock: &PreparedServerUnlock,
    operation_id: admin_proto::OperationId,
) -> std::result::Result<PreparedRevoke, String> {
    let req = req.clone();
    state
        .write_async(async move |state| {
            prepare_revoke_inner(
                state,
                &req,
                authentication,
                prepared_server_unlock,
                operation_id,
            )
            .await
        })
        .await
}

/// Whether `authd` may revoke the certificate with this `serial`, given its
/// `record` from the issuance index (`None` ⇒ this CA has no such serial).
///
/// Deny is the default. Revocation is destructive — revoking serving certs or
/// another region's leaves is a denial of service — so it is scope-bound
/// exactly like issuance: an admin may revoke only what it could have signed
/// (see [`admin_authority_over`]). A serial we cannot resolve to a record is a
/// serial whose scope we cannot evaluate, so there is nothing to authorize
/// against and it is refused rather than passed through.
///
/// `Err` carries the operator-facing reason it was skipped.
fn revoke_authority(
    authd: &crate::ca_vault::Authenticated,
    map: &admin_proto::TrustDomainMap,
    serial: u64,
    record: Option<&ca_store::IssuedRecord>,
) -> std::result::Result<(), String> {
    if broad_admin(authd) {
        return Ok(());
    }
    let Some(record) = record else {
        return Err(format!(
            "serial {serial} is not in this CA's issuance index; skipped"
        ));
    };
    let authorized = if record.name.eq_ignore_ascii_case(SERVING_SAN) {
        // A serving cert's scope is its admin server's resolver cluster, which only the
        // certificate's embedded identity can tell us.
        crate::tls::admin_cert_identity_from_pem(record.cert_pem.as_bytes())
            .ok()
            .and_then(|identity| {
                let cluster = map
                    .admin_servers
                    .iter()
                    .find(|server| server.id == identity.server_id)?
                    .cluster?;
                map.resolver_clusters.iter().find(|entry| entry.id == cluster).map(
                    |entry| scope_covers(&authd.policy.server_enroll_scopes, &entry.base),
                )
            })
            .unwrap_or(false)
    } else {
        admin_authority_over(authd, &record.name)
            .map_err(|e| format!("evaluating authority for serial {serial}: {e:#}"))?
    };
    if authorized {
        Ok(())
    } else {
        Err(format!(
            "serial {serial} ({:?}) is outside admin {}'s authority; skipped",
            record.name, authd.admin
        ))
    }
}

async fn prepare_revoke_inner(
    state: &mut MutableState,
    req: &RevokeRequest,
    authentication: &PreparedAdminAuthentication,
    prepared_server_unlock: &PreparedServerUnlock,
    operation_id: admin_proto::OperationId,
) -> std::result::Result<PreparedRevoke, String> {
    let MutableState { map, ca, .. } = state;
    let ca = ca.as_mut().expect("CA role held");
    let authd = match authenticate(ca, &req.credential, authentication) {
        Ok(a) => a,
        Err(reason) => {
            return Err(safe_auth_failure(&req.credential, reason));
        }
    };
    // Revocation is privileged — revoking serving certs or another region's
    // leaves is a denial of service — and it must be **scope-bound** exactly
    // like issuance: an admin may revoke only what it could have signed.
    // Reject up front any admin with no issuance/management authority at all;
    // the per-serial check below confines the rest to their own scope.
    if !broad_admin(&authd)
        && authd.policy.allowed_san.is_empty()
        && authd.policy.server_enroll_scopes.is_empty()
    {
        return Err(format!(
            "admin {} is not authorized to revoke certificates",
            authd.admin
        ));
    }
    if req.serials.is_empty() {
        return Err("no serials to revoke".to_string());
    }
    // Resolve each serial to the name it was issued for, so we can confine a
    // scoped admin to revoking only certs within its `allowed_san`. Read once
    // (names are immutable for a serial); the state write lock serializes the
    // actual revoke loop with every other CA operation.
    let records: std::collections::HashMap<u64, ca_store::IssuedRecord> =
        match ca.store.list_signed().await {
            Ok(records) => records.into_iter().map(|r| (r.serial, r)).collect(),
            Err(e) => {
                return Err(format!("reading the issuance index: {e:#}"));
            }
        };
    let now = ca_store::now_unix();
    let mut warnings = Vec::new();
    // Scope-check before mutating any records.
    let mut serials = Vec::new();
    for serial in &req.serials {
        match revoke_authority(&authd, map, *serial, records.get(serial)) {
            Ok(()) => serials.push(*serial),
            Err(reason) => warnings.push(reason),
        }
    }
    // Each revocation is a read-modify-write of an `issued/<id>` record; the
    // batch shares one scan of the index. The state write lock keeps the whole
    // thing serialized with `set_push_done`.
    {
        let ca_dir = ca.dir().to_path_buf();
        let store = &mut ca.store;
        let revocations: Vec<_> = serials
            .into_iter()
            .map(|serial| {
                (
                    serial,
                    ca_store::Revocation {
                        serial,
                        revoked_unix: now,
                        reason: req.reason.clone(),
                    },
                )
            })
            .collect();
        match store.revoke_many(&revocations).await {
            Ok(outcomes) => {
                for (serial, revoked) in outcomes {
                    if revoked {
                        audit(
                            &ca_dir,
                            &authd.admin,
                            "revoke",
                            &format!("operation {operation_id}: serial {serial}"),
                            Duration::ZERO,
                        )
                        .await
                    } else {
                        warnings.push(format!(
                            "serial {serial} was not live (unknown or already revoked)"
                        ))
                    }
                }
            }
            Err(e) => warnings.push(format!("revoking certificates: {e:#}")),
        }
    }
    // Re-sign the CRL with the server's own key (the autorenew credential).
    let crl_pem = match server_unlock(ca, prepared_server_unlock).await {
        Ok(signing) => {
            let path = {
                let store = &mut ca.store;
                match store.write_crl(&signing.ca_key_pem).await {
                    Ok(()) => store.crl_path(),
                    Err(e) => {
                        warnings.push(format!(
                            "re-signing the CRL failed; immediate enforcement is unavailable: {e:#}"
                        ));
                        return Ok(PreparedRevoke {
                            admin: authd.admin,
                            warnings,
                            crl_pem: None,
                        });
                    }
                }
            };
            match tokio::fs::read_to_string(&path).await {
                Ok(pem) => Some(pem),
                Err(e) => {
                    warnings.push(format!(
                        "reading the signed CRL for immediate distribution: {e:#}"
                    ));
                    None
                }
            }
        }
        Err(reason) => {
            warnings.push(format!(
                "re-signing the CRL failed; immediate enforcement is unavailable: {reason}"
            ));
            None
        }
    };
    Ok(PreparedRevoke { admin: authd.admin, warnings, crl_pem })
}

/// Validate that `crl_pem` is exactly one CRL signed by this node's immutable
/// home CA. Controller-only transport is the authorization boundary, while
/// this signature check prevents a corrupted payload from replacing working
/// revocation state.
pub(super) fn validate_home_crl(crl_pem: &str, home_ca_der: &[u8]) -> Result<()> {
    transport::validate_crl_signed_by_any(crl_pem, [home_ca_der])
        .context("CRL signature does not verify against the home CA")
}

/// Every local trust bundle whose inbound TLS authentication is administered
/// by this daemon. The admin-plane bundle is always present; a resolver role
/// may name the same bundle more than once, so destinations are deduplicated.
pub(super) async fn local_crl_destinations(
    cfg: &AdminServerConfig,
) -> Result<BTreeSet<PathBuf>> {
    use netidx::resolver_server::config::file::Auth;
    let mut destinations = BTreeSet::new();
    destinations.insert(cfg.trusted.with_file_name("crl.pem"));
    if let Some(path) = cfg.roles.resolver.as_ref().map(|role| &role.config) {
        let cfg = crate::resolver::ResolverConfig::load_async(path)
            .await
            .with_context(|| format!("loading resolver config {}", path.display()))?;
        for member in &cfg.as_file().member_servers {
            if let Auth::Tls { trusted, .. } = &member.auth {
                destinations
                    .insert(Path::new(trusted.as_str()).with_file_name("crl.pem"));
            }
        }
    }
    Ok(destinations)
}

/// Install a verified CRL atomically beside all local trust bundles. Identical
/// content is left untouched so file watchers do not rebuild TLS state twice.
pub(super) async fn apply_crl_to_destinations(
    config_lock: &ConfigDirLock,
    crl_pem: &str,
    home_ca_der: &[u8],
    destinations: BTreeSet<PathBuf>,
) -> Result<()> {
    validate_home_crl(crl_pem, home_ca_der)?;
    let destinations = destinations
        .into_iter()
        .map(|destination| config_lock.require_contained(destination))
        .collect::<Result<Vec<_>>>()?;
    let mut failures = Vec::new();
    for destination in destinations.iter() {
        match tokio::fs::read(&destination).await {
            Ok(current) if current == crl_pem.as_bytes() => continue,
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                failures.push(format!("reading {}: {e}", destination.display()));
                continue;
            }
        }
        match crate::atomic::write_atomic_async(&destination, crl_pem.as_bytes(), 0o644)
            .await
        {
            Ok(()) => info!(
                "admin-server: installed immediate CRL at {}",
                destination.display()
            ),
            Err(e) => failures.push(format!("writing {}: {e:#}", destination.display())),
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("one or more CRL destinations failed: {}", failures.join("; "))
    }
}

async fn apply_crl_local(state: &Server, crl_pem: &str) -> Result<()> {
    let crl_pem = crl_pem.to_string();
    let home_ca_der = state.home_ca_der.clone();
    let config_lock = state.config_lock.clone();
    state
        .write_async(async move |mutable| {
            let destinations = local_crl_destinations(&mutable.cfg).await?;
            apply_crl_to_destinations(
                &config_lock,
                &crl_pem,
                home_ca_der.as_ref(),
                destinations,
            )
            .await
        })
        .await
}

pub(super) async fn handle_apply_crl(
    state: &Server,
    req: &ApplyCrlRequest,
) -> ApplyCrlResponse {
    info!("admin-server: applying CRL operation {}", req.operation_id);
    match apply_crl_local(state, &req.crl_pem).await {
        Ok(()) => ApplyCrlResponse::Ok(()),
        Err(e) => ApplyCrlResponse::Err { reason: format!("{e:#}") },
    }
}

pub(super) async fn registered_crl_targets(
    state: &Server,
) -> Vec<(admin_proto::AdminServerId, SocketAddr)> {
    let mut targets: Vec<_> = state
        .read(move |state| {
            state
                .map
                .admin_servers
                .iter()
                .filter(|server| server.state == admin_proto::ServerState::Registered)
                .map(|server| (server.id, server.addr))
                .collect()
        })
        .await;
    targets.sort_by_key(|(server, _)| *server);
    targets
}

pub(super) async fn collect_peer_results<I, F, Fut>(
    targets: I,
    apply: F,
) -> Vec<PeerResult>
where
    I: IntoIterator<Item = (admin_proto::AdminServerId, SocketAddr)>,
    F: Fn(admin_proto::AdminServerId, SocketAddr) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut results: Vec<_> = stream::iter(targets.into_iter().map(|(server, addr)| {
        let future = apply(server, addr);
        async move {
            PeerResult {
                server,
                addr,
                error: future.await.err().map(|error| format!("{error:#}")),
            }
        }
    }))
    .buffer_unordered(32)
    .collect()
    .await;
    results.sort_by_key(|result| result.server);
    results
}

/// Immediately distribute a newly signed CRL to every registered node. The
/// local controller uses the same application core without a loopback TLS
/// connection; remote targets are exact-ID/home-CA pinned and bounded exactly
/// like the other controller mutation fanouts.
pub(super) async fn push_crl_to_peers(
    state: &Arc<Server>,
    crl_pem: &str,
    operation_id: admin_proto::OperationId,
) -> Vec<PeerResult> {
    let targets = registered_crl_targets(state).await;
    let (my_id, controller) =
        state.read(move |state| (state.cfg.server_id, state.map.controller)).await;
    let mut results = Vec::new();
    if let Some((server, addr)) = targets.iter().copied().find(|(id, _)| *id == my_id) {
        let crl_pem = crl_pem.to_string();
        let error =
            apply_crl_local(state, &crl_pem).await.err().map(|e| format!("{e:#}"));
        results.push(PeerResult { server, addr, error });
    }
    let client = match state.outbound_client().await {
        Ok(client) => client,
        Err(e) => {
            results.extend(
                targets.into_iter().filter(|(server, _)| *server != my_id).map(
                    |(server, addr)| PeerResult {
                        server,
                        addr,
                        error: Some(format!("loading outbound identity failed: {e:#}")),
                    },
                ),
            );
            return results;
        }
    };
    let home_ca = state.home_ca_der.clone();
    let mut remote = collect_peer_results(
        targets.into_iter().filter(|(server, _)| *server != my_id),
        |server, addr| {
            let client = client.clone();
            let home_ca = home_ca.clone();
            let crl_pem = crl_pem.to_string();
            async move {
                tokio::time::timeout(
                    PUSH_TIMEOUT,
                    transport::push_crl(
                        &client,
                        addr,
                        server,
                        server == controller,
                        home_ca,
                        operation_id,
                        &crl_pem,
                    ),
                )
                .await
                .map_err(|_| anyhow!("timed out after {}s", PUSH_TIMEOUT.as_secs()))
                .and_then(|result| result)
            }
        },
    )
    .await;
    results.append(&mut remote);
    results.sort_by_key(|result| result.server);
    results
}

pub(super) async fn handle_revoke(
    state: &Arc<Server>,
    req: &RevokeRequest,
    authentication: &PreparedAdminAuthentication,
    prepared_server_unlock: &PreparedServerUnlock,
) -> RevokeResponse {
    let operation_id = admin_proto::OperationId::new();
    let prepared =
        prepare_revoke(state, req, authentication, prepared_server_unlock, operation_id)
            .await;
    let PreparedRevoke { admin, mut warnings, crl_pem } = match prepared {
        Ok(prepared) => prepared,
        Err(reason) => return RevokeResponse::Err { reason },
    };
    audit(
        &state.ca_dir().await.expect("CA role held"),
        &admin,
        "fanout-crl",
        &format!("operation {operation_id}"),
        Duration::ZERO,
    )
    .await;
    let peers = match crl_pem {
        Some(crl_pem) => push_crl_to_peers(state, &crl_pem, operation_id).await,
        None => {
            let reason =
                "fresh CRL unavailable; immediate distribution was not attempted";
            warnings.push(reason.to_string());
            registered_crl_targets(state)
                .await
                .into_iter()
                .map(|(server, addr)| PeerResult {
                    server,
                    addr,
                    error: Some(reason.to_string()),
                })
                .collect()
        }
    };
    RevokeResponse::Ok(RevokeOk { warnings, operation_id: Some(operation_id), peers })
}
