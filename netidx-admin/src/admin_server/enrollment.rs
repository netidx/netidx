#[cfg(test)]
#[path = "enrollment_tests.rs"]
mod tests;

use super::{
    MutableState, Server,
    auth::{
        PreparedAdminAuthentication, PreparedServerUnlock, authenticate, local_superuser,
        reject, safe_auth_failure, scope_covers, server_unlock, signing_slot,
    },
    ca_dir,
    issuance::{Issuance, IssuanceMode, issue_serialized},
    revocation::revoke_server_certificates,
};
use crate::{
    admin_domain,
    admin_proto::{
        self, AdminDomainMap, EnrollRequest, Role, SERVING_SAN, SignOk, SignResponse,
    },
    ca_store, ca_vault,
};
use anyhow::{Context, Result};
use log::warn;
use std::{net::SocketAddr, sync::Arc};

async fn record_peer(state: &Server, peer: SocketAddr) {
    let cfg_path = state.cfg_path.clone();
    let config_lock = state.config_lock.clone();
    state
        .write_async(async move |inner| {
            let cfg = &mut inner.cfg;
            if peer == cfg.listen || cfg.peers.contains(&peer) {
                return;
            }
            cfg.peers.push(peer);
            if let Some(path) = &cfg_path
                && let Err(e) =
                    crate::admin_server_config::save_async(&config_lock, path, cfg).await
            {
                warn!("admin-server: failed to persist enrolled peer {peer}: {e:#}");
            }
        })
        .await;
}

pub(super) async fn finish_enrollment(
    state: &Arc<Server>,
    server_id: admin_proto::AdminServerId,
    enrollment: &admin_proto::EnrollmentRequest,
    prepared_server_unlock: &PreparedServerUnlock,
) -> Result<()> {
    grant_enrollment(state, server_id, enrollment, prepared_server_unlock).await?;
    record_peer(state, enrollment.listen).await;
    Ok(())
}

pub(super) async fn handle_enroll(
    state: &Arc<Server>,
    req: &EnrollRequest,
    authentication: &PreparedAdminAuthentication,
    prepared_server_unlock: &PreparedServerUnlock,
    local: bool,
) -> SignResponse {
    if ca_dir(state).await.is_none() {
        return reject("this host does not hold the CA");
    }
    let enrollment = admin_proto::EnrollmentRequest {
        listen: req.listen,
        roles: req.roles,
        resolver_member: req.resolver_member.clone(),
        resolver_members: req.resolver_members.clone(),
        resolver_config: req.resolver_config.clone(),
        cluster: req.cluster.clone(),
        replaces: req.replaces,
    };
    let resp = state
        .write_async(async move |state| {
            let map = state.map.clone();
            handle_enroll_request(
                state.ca.as_mut().expect("CA role held"),
                req,
                authentication,
                prepared_server_unlock,
                local,
                Some(&map),
            )
            .await
        })
        .await;
    if let SignResponse::Ok(SignOk { signed_cert_pem, .. }) = &resp
        && !local
    {
        let result =
            match crate::tls::admin_cert_identity_from_pem(signed_cert_pem.as_bytes()) {
                Ok(identity) => {
                    finish_enrollment(
                        state,
                        identity.server_id,
                        &enrollment,
                        prepared_server_unlock,
                    )
                    .await
                }
                Err(e) => Err(e),
            };
        if let Err(e) = result {
            return SignResponse::Err {
                reason: format!("recording enrollment grant: {e:#}"),
            };
        }
    }
    resp
}

async fn grant_enrollment(
    state: &Server,
    server_id: admin_proto::AdminServerId,
    enrollment: &admin_proto::EnrollmentRequest,
    prepared_server_unlock: &PreparedServerUnlock,
) -> Result<admin_proto::ResolverClusterId> {
    let cfg_path = state.cfg_path.clone();
    let config_lock = state.config_lock.clone();
    let enrollment = enrollment.clone();
    state
        .write_async(async move |mutable| {
            let MutableState { cfg, map, ca, .. } = mutable;
            let ca = ca.as_mut().context("this host does not hold the CA")?;
            let ca_dir = ca.dir().to_path_buf();
            let replaced_addr = enrollment.replaces.and_then(|old| {
                map.admin_servers
                    .iter()
                    .find(|server| server.id == old)
                    .map(|server| server.addr)
            });
            let mut staged = map.clone();
            let cluster = stage_enrollment(&mut staged, server_id, &enrollment)?;
            if let Some(old) = enrollment.replaces {
                revoke_server_certificates(
                    ca,
                    old,
                    "approved restore",
                    prepared_server_unlock,
                )
                .await
                .context("revoking the replaced server identity")?;
            }
            // Take ownership of this host's resolver config *before* the grant
            // is persisted. From here the CA renders the whole document and the
            // host stops being authoritative for any of it — including the half
            // only it could have told us: its bind address, its certificate and
            // key paths, its pid file, its tuning.
            //
            // Before, so that a failure here fails the enrollment. The other
            // order would leave a granted server whose config the CA does not
            // own — a server that looks managed and is not. An entry for a
            // server whose grant then failed is harmless by comparison: nothing
            // renders for a server that is not in the map.
            if let Some(installed) = enrollment.resolver_config.clone() {
                let mut configs = ca
                    .store
                    .desired_configs()
                    .await
                    .context("reading the desired resolver configs")?;
                configs.set(server_id, installed);
                ca.store
                    .save_desired_configs(&configs)
                    .await
                    .context("recording this server's resolver config")?;
            }
            admin_domain::save_async(&config_lock, &ca_dir, &staged)
                .await
                .context("persisting the enrollment grant")?;
            *map = staged;
            if let Some(old_addr) = replaced_addr {
                cfg.peers.retain(|peer| *peer != old_addr);
                if let Some(path) = &cfg_path {
                    crate::admin_server_config::save_async(&config_lock, path, cfg)
                        .await
                        .context("persisting removal of the replaced peer hint")?;
                }
            }
            Ok(cluster)
        })
        .await
}

/// Stage a new enrollment and, for restore, atomically replace the failed
/// satellite named by the bundle. Enroll first so replacing the sole member of
/// a resolver cluster cannot transiently delete that stable resolver cluster ID.
pub(super) fn stage_enrollment(
    map: &mut AdminDomainMap,
    server_id: admin_proto::AdminServerId,
    enrollment: &admin_proto::EnrollmentRequest,
) -> Result<admin_proto::ResolverClusterId> {
    let replaced_cluster = match enrollment.replaces {
        Some(old) if old == map.ca => {
            bail!("the active CA cannot be replaced by satellite enrollment")
        }
        Some(old) => Some(
            map.admin_servers
                .iter()
                .find(|server| server.id == old)
                .with_context(|| {
                    format!(
                        "replacement server {old} is absent from the authoritative map"
                    )
                })?
                .cluster,
        ),
        None => None,
    };
    if let Some(old) = enrollment.replaces {
        // The replacement normally owns the same resolver endpoint. Release
        // that ownership on the staged copy before enrolling the fresh ID;
        // the old server remains a resolver cluster member until the new grant exists,
        // so the stable resolver cluster itself can never disappear in between.
        if let Some(server) = map.admin_servers.iter_mut().find(|server| server.id == old)
        {
            server.resolver = None;
        }
    }
    let cluster = admin_domain::enroll(map, server_id, enrollment)?;
    if let Some(old) = enrollment.replaces {
        if replaced_cluster.flatten() != Some(cluster) {
            bail!("a restored server must rejoin the same resolver cluster it replaces");
        }
        admin_domain::remove(map, old)?;
    }
    Ok(cluster)
}

pub(super) fn authorize_enrollment(
    authd: &ca_vault::Authenticated,
    enrollment: &admin_proto::EnrollmentRequest,
    map: Option<&AdminDomainMap>,
) -> std::result::Result<(), String> {
    if enrollment.roles.contains(Role::Ca) {
        return Err("an enrollee may never request the CA role".to_string());
    }
    if !enrollment.roles.contains(Role::Resolver) {
        return Err("every non-ca enrollment must include Resolver".to_string());
    }
    if signing_slot(authd) {
        return Ok(());
    }
    if !authd.policy.server_enroll_roles.contains(enrollment.roles) {
        return Err(format!(
            "requested roles {:?} exceed admin {}'s allowed enrollment roles {:?}",
            enrollment.roles, authd.admin, authd.policy.server_enroll_roles
        ));
    }
    let base = match enrollment.cluster {
        admin_proto::ResolverClusterPlacement::Create { ref base } => base.as_str(),
        admin_proto::ResolverClusterPlacement::Join { cluster } => map
            .and_then(|m| m.resolver_clusters.iter().find(|c| c.id == cluster))
            .map(|c| c.base.as_str())
            .ok_or_else(|| {
                "the requested resolver cluster is not in the authoritative map"
                    .to_string()
            })?,
    };
    if !scope_covers(&authd.policy.server_enroll_scopes, base) {
        return Err(format!(
            "admin {} is not authorized to enroll servers at {base:?}",
            authd.admin
        ));
    }
    Ok(())
}

pub(super) async fn handle_enroll_request(
    ca: &mut ca_store::CaDir,
    req: &EnrollRequest,
    authentication: &PreparedAdminAuthentication,
    prepared_server_unlock: &PreparedServerUnlock,
    local: bool,
    map: Option<&AdminDomainMap>,
) -> SignResponse {
    match try_enroll(ca, req, authentication, prepared_server_unlock, local, map).await {
        Ok(resp) => resp,
        Err(e) => SignResponse::Err { reason: format!("internal error: {e:#}") },
    }
}

fn enrollment_cert_identity(
    local: bool,
    renew_identity: Option<admin_proto::AdminServerId>,
    map: Option<&AdminDomainMap>,
) -> std::result::Result<crate::tls::AdminCertIdentity, String> {
    if !local {
        return Ok(crate::tls::AdminCertIdentity {
            server_id: admin_proto::AdminServerId::new(),
            ca: false,
        });
    }
    let server_id = renew_identity.ok_or_else(|| {
        "local CA enrollment is renewal-only and requires its existing identity"
            .to_string()
    })?;
    if map.is_none_or(|map| map.ca != server_id) {
        return Err(
            "the requested local renewal identity is not the active CA".to_string()
        );
    }
    Ok(crate::tls::AdminCertIdentity { server_id, ca: true })
}

async fn try_enroll(
    ca: &mut ca_store::CaDir,
    req: &EnrollRequest,
    authentication: &PreparedAdminAuthentication,
    prepared_server_unlock: &PreparedServerUnlock,
    local: bool,
    map: Option<&AdminDomainMap>,
) -> Result<SignResponse> {
    // A request over the local control socket is already authorized as a
    // signing-tier superuser (`SO_PEERCRED` root / the daemon's own uid),
    // which carries full local authority. This is the path renewd uses to
    // re-mint the admin server's OWN serving cert without TLS — the only way
    // to recover from an already-expired serving cert, since renewing it
    // over TLS-to-self can't connect once it's expired.
    let authd = if local {
        local_superuser()
    } else {
        let authd = match authenticate(ca, &req.credential, authentication) {
            Ok(a) => a,
            Err(reason) => {
                return Ok(reject(&safe_auth_failure(&req.credential, reason)));
            }
        };
        let enrollment = admin_proto::EnrollmentRequest {
            listen: req.listen,
            roles: req.roles,
            resolver_member: req.resolver_member.clone(),
            resolver_members: req.resolver_members.clone(),
            resolver_config: req.resolver_config.clone(),
            cluster: req.cluster.clone(),
            replaces: req.replaces,
        };
        if let Err(reason) = authorize_enrollment(&authd, &enrollment, map) {
            return Ok(reject(&reason));
        }
        authd
    };
    let record_req = ca_store::QueuedReq::new(
        admin_proto::NodeKind::AdminServer,
        req.csr_pem.clone(),
        SERVING_SAN.to_string(),
        ca.lifetimes.leaf_validity,
        "(enroll)".to_string(),
        None,
        Some(admin_proto::EnrollmentRequest {
            listen: req.listen,
            roles: req.roles,
            resolver_member: req.resolver_member.clone(),
            resolver_members: req.resolver_members.clone(),
            resolver_config: req.resolver_config.clone(),
            cluster: req.cluster.clone(),
            replaces: req.replaces,
        }),
    );
    let signing = match server_unlock(ca, prepared_server_unlock).await {
        Ok(u) => u,
        Err(reason) => return Ok(reject(&reason)),
    };
    // Serving certs aren't subject to the one-live check (many admin
    // servers legitimately hold the reserved SAN), and carry no groups.
    let identity = match enrollment_cert_identity(local, req.renew_identity, map) {
        Ok(identity) => identity,
        Err(reason) => return Ok(reject(&reason)),
    };
    if !local {
        let enrollment = admin_proto::EnrollmentRequest {
            listen: req.listen,
            roles: req.roles,
            resolver_member: req.resolver_member.clone(),
            resolver_members: req.resolver_members.clone(),
            resolver_config: req.resolver_config.clone(),
            cluster: req.cluster.clone(),
            replaces: req.replaces,
        };
        let Some(map) = map else {
            return Ok(reject("the CA-owned admin domain map is unavailable"));
        };
        let mut staged = map.clone();
        if let Err(e) = stage_enrollment(&mut staged, identity.server_id, &enrollment) {
            return Ok(reject(&format!("invalid enrollment grant: {e:#}")));
        }
    }
    let signed = issue_serialized(
        ca,
        &signing,
        Issuance {
            audit_admin: &authd.admin,
            audit_op: "enroll",
            record_req: &record_req,
            name: SERVING_SAN,
            validity: ca.lifetimes.leaf_validity,
            mode: IssuanceMode::Enrollment { identity },
            pending_request: None,
        },
    )
    .await?;
    Ok(signed.resp)
}
