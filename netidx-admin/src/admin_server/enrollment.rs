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
    issuance::{Issuance, IssuanceMode, issue_serialized, leaf_serial},
    revocation::revoke_server_certificates,
};
use crate::{
    admin_domain,
    admin_proto::{
        self, AdminDomainMap, EnrollRequest, Role, SERVING_SAN, SignOk, SignResponse,
    },
    ca_store, ca_vault,
    config_lock::ConfigDirLock,
};
use anyhow::{Context, Result, bail};
use log::warn;
use std::{net::SocketAddr, path::Path, sync::Arc};

async fn record_peer(
    cfg: &mut crate::admin_server_config::AdminServerConfig,
    cfg_path: Option<&Path>,
    config_lock: &ConfigDirLock,
    peer: SocketAddr,
) {
    if peer == cfg.listen || cfg.peers.contains(&peer) {
        return;
    }
    cfg.peers.push(peer);
    if let Some(path) = cfg_path
        && let Err(e) =
            crate::admin_server_config::save_async(config_lock, path, cfg).await
    {
        warn!("admin-server: failed to persist enrolled peer {peer}: {e:#}");
    }
}

pub(super) async fn finish_enrollment(
    state: &mut MutableState,
    cfg_path: Option<&Path>,
    config_lock: &ConfigDirLock,
    server_id: admin_proto::AdminServerId,
    enrollment: &admin_proto::EnrollmentRequest,
    prepared_server_unlock: &PreparedServerUnlock,
) -> Result<()> {
    grant_enrollment(
        state,
        cfg_path,
        config_lock,
        server_id,
        enrollment,
        prepared_server_unlock,
    )
    .await?;
    record_peer(&mut state.cfg, cfg_path, config_lock, enrollment.listen).await;
    Ok(())
}

fn leaf_serial_from_pem(pem: &str) -> Option<u64> {
    let der =
        rustls_pemfile::certs(&mut std::io::Cursor::new(pem.as_bytes())).next()?.ok()?;
    leaf_serial(der.as_ref())
}

async fn issued_id_for_serial(
    store: &ca_store::CAStore,
    serial: u64,
) -> Result<Option<String>> {
    let mut entries = store.issued_records().await?;
    while let Some(rec) = entries.next().await? {
        if rec.serial == serial {
            return Ok(Some(rec.req.id.clone()));
        }
    }
    Ok(None)
}

/// Withdraw a serving cert that was committed before its enrollment grant
/// failed, while the state write lock is still held. The cert never left
/// this lock, so it is not revoked — `issued/` is the CRL's source, and
/// deleting the record after a revoke would drop the serial on the next
/// `write_crl`.
pub(super) async fn undo_enrollment_issuance(
    ca: &mut ca_store::CaDir,
    signed_cert_pem: &str,
    queued_id: Option<&str>,
) -> Result<()> {
    let id = match queued_id {
        Some(id) => id.to_string(),
        None => {
            let serial = leaf_serial_from_pem(signed_cert_pem)
                .context("cannot withdraw an enrollment cert with no serial")?;
            issued_id_for_serial(&ca.store, serial)
                .await?
                .context("cannot withdraw an enrollment cert missing from issued/")?
        }
    };
    ca.store.uncommit_signed(&id, queued_id.is_some()).await?;
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
    let cfg_path = state.cfg_path.clone();
    let config_lock = state.config_lock.clone();
    state
        .write_async(async move |state| {
            let map = state.map.clone();
            let resp = handle_enroll_request(
                state.ca.as_mut().expect("CA role held"),
                req,
                authentication,
                prepared_server_unlock,
                local,
                Some(&map),
            )
            .await;
            if let SignResponse::Ok(SignOk { signed_cert_pem, .. }) = &resp
                && !local
            {
                let result = match crate::tls::admin_cert_identity_from_pem(
                    signed_cert_pem.as_bytes(),
                ) {
                    Ok(identity) => {
                        finish_enrollment(
                            state,
                            cfg_path.as_deref(),
                            &config_lock,
                            identity.server_id,
                            &enrollment,
                            prepared_server_unlock,
                        )
                        .await
                    }
                    Err(e) => Err(e),
                };
                if let Err(e) = result {
                    if let Some(ca) = state.ca.as_mut()
                        && let Err(undo) =
                            undo_enrollment_issuance(ca, signed_cert_pem, None).await
                    {
                        return reject(&format!(
                            "recording enrollment grant: {e:#} (also failed to \
                             withdraw the cert: {undo:#})"
                        ));
                    }
                    return reject(&format!("recording enrollment grant: {e:#}"));
                }
            }
            resp
        })
        .await
}

async fn grant_enrollment(
    state: &mut MutableState,
    cfg_path: Option<&Path>,
    config_lock: &ConfigDirLock,
    server_id: admin_proto::AdminServerId,
    enrollment: &admin_proto::EnrollmentRequest,
    prepared_server_unlock: &PreparedServerUnlock,
) -> Result<admin_proto::ResolverClusterId> {
    let MutableState { cfg, map, ca, .. } = state;
    let ca = ca.as_mut().context("this host does not hold the CA")?;
    let ca_dir = ca.dir().to_path_buf();
    let replaced_addr = enrollment.replaces.and_then(|old| {
        map.admin_servers.iter().find(|server| server.id == old).map(|server| server.addr)
    });
    let mut staged = map.clone();
    let cluster = stage_enrollment(&mut staged, server_id, &enrollment)?;
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
    grant_member_self_perms(ca, &mut staged, cluster, &enrollment)
        .await
        .context("granting the enrolling member its own permissions")?;
    admin_domain::save_async(config_lock, &ca_dir, &staged)
        .await
        .context("persisting the enrollment grant")?;
    *map = staged;
    // After the grant is durable. Revoking first meant a later persist
    // failure left the old identity dead and the new one withdrawn.
    if let Some(old) = enrollment.replaces {
        if let Err(e) = revoke_server_certificates(
            ca,
            old,
            "approved restore",
            prepared_server_unlock,
        )
        .await
        {
            warn!(
                "admin-server: enrollment grant persisted but failed to revoke \
                 replaced server {old}: {e:#}"
            );
        }
    }
    if let Some(old_addr) = replaced_addr {
        cfg.peers.retain(|peer| *peer != old_addr);
        if let Some(path) = cfg_path
            && let Err(e) =
                crate::admin_server_config::save_async(config_lock, path, cfg).await
        {
            warn!(
                "admin-server: failed to persist removal of the replaced peer hint: {e:#}"
            );
        }
    }
    Ok(cluster)
}

/// Add the enrolling member's own identity to its resolver cluster's
/// permissions.
///
/// A resolver host uses its own certificate as a client — that is what makes
/// `netidx resolver list` work on the box, and the installer drops a
/// `client.json` for it — so its identity needs rights at the cluster base.
/// The installer used to write that grant into the host's own perms file,
/// which is how two members of one cluster came to hold different documents:
/// each granted itself and not the other, so the same subscription authorized
/// differently depending on which member it reached, and the first CA edit
/// then propagated one host's document over everyone else's, silently revoking
/// one grant and extending another's across the cluster.
///
/// Permissions belong to the resolver cluster, so the CA owns them and a
/// member never does. The grant is *derived* from what the CA is issuing
/// rather than requested by the enrollee: the only entry a host could
/// legitimately ask for is the one for the identity the CA is about to give
/// it, and the CA already knows that — so there is nothing here an enrollee
/// could ask for on someone else's behalf.
async fn grant_member_self_perms(
    ca: &ca_store::CaDir,
    map: &mut AdminDomainMap,
    cluster: admin_proto::ResolverClusterId,
    enrollment: &admin_proto::EnrollmentRequest,
) -> Result<()> {
    let Some(member) = enrollment.resolver_member.as_ref() else { return Ok(()) };
    // Anonymous has no identity, and local auth is peer credentials with no
    // fixed name — neither is an entity permissions can name.
    let entity = match &member.auth {
        admin_proto::InfoAuth::Tls { name } => name.as_str(),
        admin_proto::InfoAuth::Krb5 { spn } => spn.as_str(),
        admin_proto::InfoAuth::Anonymous => return Ok(()),
    };
    let base = map
        .resolver_clusters
        .iter()
        .find(|c| c.id == cluster)
        .context("the enrolled grant references a missing resolver cluster")?
        .base
        .clone();
    let mut model = ca.store.perms_model().await?;
    let mut perms = match model.get(cluster) {
        Some(recorded) => recorded.perms.clone(),
        // The first member of a new cluster: start from the shared seed, so a
        // cluster has a document from the moment it exists rather than from
        // whenever someone first edits it.
        None => crate::perms::default_seed(&base, seed_groups(enrollment)),
    };
    crate::perms::add_entry(&mut perms, &base, entity, "swlpd")
        .with_context(|| format!("granting {entity} swlpd at {base}"))?;
    let version = model.set(cluster, &perms)?;
    ca.store.save_perms_model(&model).await?;
    admin_domain::set_perms_version(map, cluster, version);
    Ok(())
}

/// Forget everything the CA was keeping on behalf of a server it no longer
/// has: its stored resolver config, and its own grant in its cluster's
/// permissions.
///
/// The counterpart to [`grant_member_self_perms`]. Neither is load-bearing for
/// security — a removed server's certificate is revoked, so its identity no
/// longer authenticates and the entry is inert — but leaving them is how a
/// document nobody edited grows entries for hosts nobody remembers, and an
/// operator reading permissions should not have to know which names are ghosts.
///
/// `before` is the map as it was, because that is the only place the removed
/// server's cluster and identity still exist; `after` is consulted for whether
/// the cluster outlived it, since removing the last member deletes the cluster
/// and then the whole document goes rather than one line of it.
pub(super) async fn forget_removed_server(
    ca: &ca_store::CaDir,
    before: &AdminDomainMap,
    after: &mut AdminDomainMap,
    server: admin_proto::AdminServerId,
) -> Result<()> {
    let mut configs = ca.store.desired_configs().await?;
    if configs.forget(server) {
        ca.store.save_desired_configs(&configs).await?;
    }
    let Some(entry) = before.admin_servers.iter().find(|s| s.id == server) else {
        return Ok(());
    };
    let Some(cluster) = entry.cluster else { return Ok(()) };
    let mut model = ca.store.perms_model().await?;
    match after.resolver_clusters.iter().find(|c| c.id == cluster) {
        None => {
            if model.forget(cluster) {
                ca.store.save_perms_model(&model).await?;
            }
        }
        Some(surviving) => {
            let entity = match entry.resolver.as_ref().map(|r| &r.auth) {
                Some(admin_proto::InfoAuth::Tls { name }) => name.clone(),
                Some(admin_proto::InfoAuth::Krb5 { spn }) => spn.clone(),
                Some(admin_proto::InfoAuth::Anonymous) | None => return Ok(()),
            };
            let base = surviving.base.clone();
            let Some(recorded) = model.get(cluster) else { return Ok(()) };
            let mut perms = recorded.perms.clone();
            if !crate::perms::remove_entry(&mut perms, &base, &entity) {
                return Ok(());
            }
            let version = model.set(cluster, &perms)?;
            ca.store.save_perms_model(&model).await?;
            admin_domain::set_perms_version(after, cluster, version);
        }
    }
    Ok(())
}

/// Whether this cluster's seed should carry the shared `users` group grant.
///
/// The seed's shared entry names a group, and with no id mapping there are no
/// groups to be a member of, so it would grant nothing to nobody.
fn seed_groups(enrollment: &admin_proto::EnrollmentRequest) -> crate::perms::Groups {
    use netidx::resolver_server::config::file::IdMapType;
    let mapped = enrollment.resolver_config.as_ref().is_none_or(|config| {
        config
            .member_servers
            .first()
            .is_none_or(|m| !matches!(m.id_map_type, IdMapType::DoNotMap))
    });
    if mapped {
        crate::perms::Groups::Resolve
    } else {
        crate::perms::Groups::DoNotResolve
    }
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
