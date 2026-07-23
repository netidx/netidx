#[cfg(test)]
#[path = "issuance_tests.rs"]
mod tests;

use super::{
    PUSH_TIMEOUT, Server, audit,
    auth::{
        PreparedAdminAuthentication, PreparedServerUnlock, authenticate, name_permitted,
        one_live_refusal, reject, safe_auth_failure, server_unlock,
    },
    ca_dir,
    revocation::push_crl_to_peers,
};
use crate::{
    admin_client,
    admin_proto::{
        self, AddIdentityOk, AddIdentityRequest, AddIdentityResponse, NodeKind, Role,
        SERVING_SAN, SignOk, SignRequest, SignResponse,
    },
    ca::{Ca, SanEntry},
    ca_store, ca_vault,
    config_lock::ConfigDirLock,
    id_map,
};
use anyhow::{Context, Result, anyhow, bail};
use futures::{StreamExt, stream};
use log::{info, warn};
use rustls_pki_types::CertificateDer;
use std::{net::SocketAddr, path::Path, sync::Arc, time::Duration};

pub(super) fn one_live_name(kind: NodeKind) -> bool {
    kind != NodeKind::Resolver
}

pub(super) fn restore_kind_matches(old: NodeKind, new: NodeKind) -> bool {
    old == new
        || matches!(
            (old, new),
            (NodeKind::Client, NodeKind::Resolver)
                | (NodeKind::Resolver, NodeKind::Client)
        )
}

/// List the pending queue for an authenticated admin.

pub(super) async fn propagate_issuance(
    state: &Arc<Server>,
    warnings: &mut Vec<String>,
    push: Option<PushPlan>,
    replacement_crl: Option<String>,
) -> Option<admin_proto::OperationId> {
    let mut operation_id = if let Some(plan) = push {
        let (operation_id, push_warnings) = push_registrations(state, &plan).await;
        warnings.extend(push_warnings);
        Some(operation_id)
    } else {
        None
    };
    if let Some(crl) = replacement_crl {
        let op = operation_id.unwrap_or_else(admin_proto::OperationId::new);
        for result in push_crl_to_peers(state, &crl, op).await {
            if let Some(error) = result.error {
                warnings.push(format!(
                    "CRL push to {} at {}: {error}",
                    result.server, result.addr
                ));
            }
        }
        operation_id = Some(op);
    }
    operation_id
}

pub(super) async fn handle_sign(
    state: &Arc<Server>,
    req: &SignRequest,
    authentication: &PreparedAdminAuthentication,
    prepared_server_unlock: &PreparedServerUnlock,
) -> SignResponse {
    if ca_dir(state).await.is_none() {
        return reject("this host does not hold the CA");
    }
    let Signed { resp, push, replacement_crl } = state
        .write_async(async move |state| {
            handle_sign_request(
                state.ca.as_mut().expect("CA role held"),
                req,
                authentication,
                prepared_server_unlock,
            )
            .await
        })
        .await;
    match resp {
        resp @ SignResponse::Err { .. } => resp,
        SignResponse::Ok(SignOk {
            signed_cert_pem,
            trusted_pem,
            mut warnings,
            operation_id: _,
        }) => {
            let operation_id =
                propagate_issuance(state, &mut warnings, push, replacement_crl).await;
            SignResponse::Ok(SignOk {
                signed_cert_pem,
                trusted_pem,
                warnings,
                operation_id,
            })
        }
    }
}

/// The shared signing tail of every issuance path: build a transient
/// [`Ca`] from the decrypted key, sign the CSR for exactly `name` with
/// the caller-allocated `serial`, and bundle the trust anchors.
pub(super) async fn sign_csr(
    dir: &Path,
    store: &ca_store::CAStore,
    ca_key_pem: &[u8],
    csr_pem: &str,
    san: &[SanEntry],
    validity: Duration,
    serial: u64,
) -> Result<SignResponse> {
    let cert_pem = tokio::fs::read(dir.join("certificate.pem"))
        .await
        .context("reading CA certificate")?;
    let dir = dir.to_path_buf();
    let ca_key_pem = ca_key_pem.to_vec();
    let csr_pem = csr_pem.as_bytes().to_vec();
    let san = san.to_vec();
    let signed = tokio::task::spawn_blocking(move || {
        let ca =
            Ca::from_pem(dir, &ca_key_pem, &cert_pem).context("loading CA from vault")?;
        ca.sign_request(&csr_pem, &san, validity, serial).context("signing CSR")
    })
    .await
    .context("CA signing task panicked")??;
    let trusted_pem = store.read_trusted_bundle().await?;
    Ok(SignResponse::Ok(SignOk {
        signed_cert_pem: String::from_utf8(signed).context("signed cert not utf8")?,
        trusted_pem,
        warnings: Vec::new(),
        operation_id: None,
    }))
}

/// Handle an id-map registration against the map at `map_path`. A
/// missing file starts from the seeded empty map — zero-touch joins
/// must work on a host whose id-map daemon hasn't registered anyone
/// yet.
pub(super) async fn handle_add_identity(
    config_lock: &ConfigDirLock,
    map_path: &Path,
    req: &AddIdentityRequest,
) -> AddIdentityResponse {
    info!(
        "admin-server: applying id-map registration operation {} for {:?}",
        req.operation_id, req.san
    );
    let map_path = match config_lock.require_contained(map_path) {
        Ok(path) => path,
        Err(e) => return AddIdentityResponse::Err { reason: format!("{e:#}") },
    };
    let mut map = if tokio::fs::try_exists(&map_path).await.unwrap_or(false) {
        match id_map::load_async(&map_path).await {
            Ok(m) => m,
            Err(e) => {
                return AddIdentityResponse::Err {
                    reason: format!("loading id-map: {e:#}"),
                };
            }
        }
    } else {
        id_map::empty()
    };
    let groups: Vec<&str> = req.groups.iter().map(|s| s.as_str()).collect();
    match id_map::register_identity(&mut map, &req.san, &req.primary_group, &groups) {
        Ok(uid) => match id_map::save_async(&map_path, &map).await {
            Ok(()) => AddIdentityResponse::Ok(AddIdentityOk { uid }),
            Err(e) => {
                AddIdentityResponse::Err { reason: format!("saving id-map: {e:#}") }
            }
        },
        Err(e) => AddIdentityResponse::Err { reason: format!("{e:#}") },
    }
}

/// Extract an X.509 certificate's serial as u64 (this CA issues from a
/// u64 counter; foreign certs with big serials yield `None`, which
/// simply never matches the index).
pub(super) fn leaf_serial(der: &[u8]) -> Option<u64> {
    use x509_parser::prelude::{FromDer, X509Certificate};
    let (_, cert) = X509Certificate::from_der(der).ok()?;
    cert.tbs_certificate.serial.to_string().parse().ok()
}

/// Queue a signing request for later admin approval. Unauthenticated
/// by design — the requester has no credentials yet; trust is
/// established when the admin matches the request's CSR-key
/// fingerprint before approving. Only cheap structural checks happen
/// here (the policy checks run at approval, under the approving
/// admin's slot).
///
/// The exception is a **verified renewal**: the connection presented a
/// valid client cert whose SAN is exactly the requested name and whose
/// serial is live in our own index. That's cryptographic continuation
/// of an identity the admin already approved once — it bypasses the
/// reserved-name and one-live-cert rules (a renewal's name *does* have
/// a live cert; that's the point) and is flagged for glyph-free,
/// batchable (or automatic) approval.

pub(super) struct Signed {
    pub(super) resp: SignResponse,
    /// `Some` only when the sign succeeded *and* the request asked for
    /// id-map registration: the issued name and the admin's chosen
    /// (policy-validated) groups.
    pub(super) push: Option<PushPlan>,
    /// A restore replacement revoked its old machine certificate and produced
    /// this fresh CRL for immediate fanout.
    pub(super) replacement_crl: Option<String>,
}

/// What [`push_registrations`] needs after a successful sign.
pub(super) struct PushPlan {
    /// The issued record's id, so a confirmed push can mark its record
    /// `push_done` (closing the issue→push recovery loop).
    pub(super) id: String,
    pub(super) name: String,
    pub(super) groups: Vec<String>,
}

/// Authenticate the requesting admin — role-capable, and crucially **no CA
/// key**. Every admin-authenticated request starts here: the non-signing
/// ops (list, deny, remove-server, delegation review) end here, and the
/// signing ops authenticate this way too, then obtain the key separately
/// via [`server_unlock`]. The `Err` is a safe wire reason.
pub(super) async fn handle_sign_request(
    ca: &mut ca_store::CaDir,
    req: &SignRequest,
    authentication: &PreparedAdminAuthentication,
    prepared_server_unlock: &PreparedServerUnlock,
) -> Signed {
    // A direct Sign has no queue entry; synthesize a request to carry in
    // the issued record (its fresh id keys the `issued/` file).
    let record_req = ca_store::QueuedReq::new(
        req.kind,
        req.csr_pem.clone(),
        req.requested_name.clone(),
        req.requested_validity,
        "(direct sign)".to_string(),
        None,
        None,
    );
    handle_sign_request_op(
        ca,
        req,
        authentication,
        prepared_server_unlock,
        "sign",
        &record_req,
        None,
    )
    .await
}

/// [`handle_sign_request`] with the audit-log operation name, the
/// record's originating request, and (for the approve path) the id to
/// re-check while the write transaction still owns the CA. The approve path
/// signs through the identical checks but audits as `op=approve` and keys
/// the record by the *queued* request id.
pub(super) async fn handle_sign_request_op(
    ca: &mut ca_store::CaDir,
    req: &SignRequest,
    authentication: &PreparedAdminAuthentication,
    prepared_server_unlock: &PreparedServerUnlock,
    op: &str,
    record_req: &ca_store::QueuedReq,
    recheck_id: Option<&str>,
) -> Signed {
    match try_handle(
        ca,
        req,
        authentication,
        prepared_server_unlock,
        op,
        record_req,
        recheck_id,
    )
    .await
    {
        Ok(signed) => signed,
        Err(e) => Signed {
            resp: SignResponse::Err { reason: format!("internal error: {e:#}") },
            push: None,
            replacement_crl: None,
        },
    }
}

async fn try_handle(
    ca: &mut ca_store::CaDir,
    req: &SignRequest,
    authentication: &PreparedAdminAuthentication,
    prepared_server_unlock: &PreparedServerUnlock,
    op: &str,
    record_req: &ca_store::QueuedReq,
    recheck_id: Option<&str>,
) -> Result<Signed> {
    let failed = |resp: SignResponse| Signed { resp, push: None, replacement_crl: None };
    // 1. Authenticate the REQUESTING admin — no CA key (a role admin is a
    //    first-class issuer here; the server, not the admin, holds the key).
    let authd = match authenticate(ca, &req.credential, authentication) {
        Ok(a) => a,
        Err(reason) => {
            return Ok(failed(reject(&safe_auth_failure(&req.credential, reason))));
        }
    };

    // 2. Authorize against the REQUESTER's policy. Since the server can sign
    //    anything once it unlocks (step 3), this gate is the whole boundary.
    let name = req.requested_name.trim();
    if name.is_empty() {
        return Ok(failed(reject("requested name is empty")));
    }
    // The admin server's own serving name is reserved: the trust model
    // hinges on *only* genuine daemons holding a CA-signed cert with it.
    // Issuing it via Sign — even to an admin whose policy glob (e.g. "*")
    // happens to match — would let that admin stand up an impostor
    // daemon. Refuse it unconditionally; admin servers are minted only by
    // the local setup path or a scoped server-enrollment grant.
    if name.eq_ignore_ascii_case(SERVING_SAN) {
        return Ok(failed(reject(
            "that name is reserved for the admin server and cannot be issued",
        )));
    }
    if !name_permitted(name, &authd.policy.allowed_san)? {
        return Ok(failed(reject(&format!(
            "name {name:?} is not permitted for admin {}",
            authd.admin
        ))));
    }
    let validity = req.requested_validity.min(authd.policy.max_validity);
    if validity.is_zero() {
        return Ok(failed(reject("validity must be > 0 and within policy")));
    }
    // The id-map groups are *chosen* by the admin at enrollment time, but
    // bounded by the policy's allowed set. Refusing the whole sign on a
    // disallowed group is deliberate: silently dropping the registration
    // would produce a node whose cert works but whose perms don't.
    let groups: Vec<String> = {
        let mut gs: Vec<String> = req
            .id_map_groups
            .iter()
            .map(|g| g.trim().to_string())
            .filter(|g| !g.is_empty())
            .collect();
        gs.dedup();
        gs
    };
    for g in &groups {
        if !authd.policy.id_map_groups.iter().any(|a| a == g) {
            return Ok(failed(reject(&format!(
                "id-map group {g:?} is not permitted for admin {}; allowed: {:?}",
                authd.admin, authd.policy.id_map_groups,
            ))));
        }
    }
    // 3. The server signs with its OWN credential; the requester is audited.
    let signing = match server_unlock(ca, prepared_server_unlock).await {
        Ok(u) => u,
        Err(reason) => return Ok(failed(reject(&reason))),
    };
    // The one-live-cert check, serial allocation, sign, and atomic record
    // commit are one write transaction.
    issue_serialized(
        ca,
        &signing,
        &authd.admin,
        record_req,
        name,
        validity,
        groups,
        one_live_name(record_req.kind),
        None,
        req.replaces_serial,
        None,
        op,
        recheck_id,
    )
    .await
}

/// The serialized issuance core, shared by sign / approve / enroll / renewal.
/// The state write lock keeps exclusive access across the one-live scan, serial
/// allocation, sign, and the single atomic `commit_signed` — so the
/// one-live invariant holds and the issuance is committed by one write.
/// `one_live` is false for serving certs / verified renewals, where
/// multiple live certs for a name are legitimate.
#[allow(clippy::too_many_arguments)]
pub(super) async fn issue_serialized(
    ca: &mut ca_store::CaDir,
    // The SERVER's autorenew-unlocked key (does the crypto)…
    signing: &ca_vault::Unlocked,
    // …vs the REQUESTING admin (named in the audit trail). They differ now:
    // the server signs, the human authorized.
    audit_admin: &str,
    record_req: &ca_store::QueuedReq,
    name: &str,
    validity: Duration,
    groups: Vec<String>,
    one_live: bool,
    // For a verified renewal: the serial of the cert being renewed. It was
    // proven live (and key-matched) at enqueue, but a revocation can land
    // between then and now — so we re-check it is still live in this
    // write transaction, and refuse the renewal if it isn't. `None` for any
    // non-renewal issuance.
    renewal_of: Option<u64>,
    // Restore replacement approved for this exact old serial.
    replacement_of: Option<u64>,
    serving_identity: Option<crate::tls::AdminCertIdentity>,
    audit_op: &str,
    // For the approve path: re-check the queue entry is still Pending
    // in the same write transaction, so two approvals (or an approve racing a deny) can't
    // both transition it. `None` for direct (non-queued) issuance.
    recheck_id: Option<&str>,
) -> Result<Signed> {
    let dir = ca.dir().to_path_buf();
    let config_lock = ca.config_lock();
    let store = &mut ca.store;
    if let Some(id) = recheck_id {
        match store.status(id).await {
            Ok(ca_store::Status::Pending(_)) => {}
            Ok(ca_store::Status::Signed(_)) => {
                return Ok(Signed {
                    resp: SignResponse::Err {
                        reason: "that request was already approved".to_string(),
                    },
                    push: None,
                    replacement_crl: None,
                });
            }
            Ok(ca_store::Status::Denied(_)) => {
                return Ok(Signed {
                    resp: SignResponse::Err {
                        reason: "that request was already denied".to_string(),
                    },
                    push: None,
                    replacement_crl: None,
                });
            }
            Ok(ca_store::Status::Unknown) => {
                return Ok(Signed {
                    resp: SignResponse::Err {
                        reason: "no such pending request (expired or never queued)"
                            .to_string(),
                    },
                    push: None,
                    replacement_crl: None,
                });
            }
            Err(e) => return Err(e).context("re-checking the queue before issuance"),
        }
    }
    let replacement_record = if let Some(serial) = replacement_of {
        let records = store.list_signed().await.context("checking replacement serial")?;
        match records.into_iter().find(|record| record.serial == serial) {
            Some(record)
                if record.name.eq_ignore_ascii_case(name)
                    && restore_kind_matches(record.req.kind, record_req.kind)
                    && record.live(ca_store::now_unix()) =>
            {
                Some(record)
            }
            Some(record)
                if record.name.eq_ignore_ascii_case(name)
                    && restore_kind_matches(record.req.kind, record_req.kind) =>
            {
                // An interrupted/manual recovery may already have revoked or
                // outlived the exact old cert. Issuing is now an ordinary
                // one-live-checked enrollment, so there is nothing left to
                // revoke a second time.
                None
            }
            Some(record) => {
                return Ok(Signed {
                    resp: reject(&format!(
                        "replacement serial {serial} belongs to {:?} {:?}, not {:?} {:?}",
                        record.req.kind, record.name, record_req.kind, name
                    )),
                    push: None,
                    replacement_crl: None,
                });
            }
            None => {
                return Ok(Signed {
                    resp: reject(&format!(
                        "replacement serial {serial} is not a live certificate for {name:?}"
                    )),
                    push: None,
                    replacement_crl: None,
                });
            }
        }
    } else {
        None
    };
    if one_live {
        let live =
            store.live_for_name(name).await.context("checking the issuance index")?;
        if live.iter().any(|record| Some(record.serial) != replacement_of) {
            return Ok(Signed {
                resp: reject(&one_live_refusal(name, &live)),
                push: None,
                replacement_crl: None,
            });
        }
    }
    // A verified renewal is a continuation of an identity that was live at
    // enqueue. Re-validate before committing that the cert it renews is still
    // live: if it was revoked (or expired) in between, the renewal must NOT
    // re-mint it — otherwise revocation, the only containment tool, could be
    // outrun by an in-flight renewal (worst case re-minting the serving
    // SAN). Refusing here also covers the auto-renew sweep, which signs
    // through this same path.
    if let Some(serial) = renewal_of {
        let live =
            store.live_for_name(name).await.context("checking the issuance index")?;
        if !live.iter().any(|r| r.serial == serial) {
            return Ok(Signed {
                resp: reject(&format!(
                    "the certificate being renewed (serial {serial} for {name:?}) is no \
                     longer live — it may have been revoked or expired; this renewal is \
                     refused"
                )),
                push: None,
                replacement_crl: None,
            });
        }
    }
    // Opportunistic CA-cert renewal — rare, and only allocates a serial
    // when actually renewing, so the common path burns nothing. An
    // externally-signed CA cert cannot be self-renewed (netidx doesn't
    // hold the external issuer's key); warn instead so the operator
    // re-signs out of band. (Warning fires only within the renewal
    // window; a rate limit could reduce it further if it proves noisy.)
    let renewal_dir = dir.clone();
    let renewal_threshold = ca.lifetimes.ca_renew_threshold;
    let needs_renewal = tokio::task::spawn_blocking(move || {
        crate::ca::ca_cert_needs_renewal(&renewal_dir, renewal_threshold)
    })
    .await
    .unwrap_or(false);
    if needs_renewal {
        if ca.lifetimes.externally_signed {
            warn!(
                "admin-server: the externally-signed CA certificate is within its \
                 renewal threshold and will NOT auto-renew — obtain a re-signed \
                 cert from your PKI and run `netidx admin ca external install`"
            );
        } else {
            let rs = store.alloc_serial();
            let renewal_dir = dir.clone();
            let config_lock = config_lock.clone();
            let ca_key_pem = signing.ca_key_pem.to_vec();
            let renewed = tokio::task::spawn_blocking(move || {
                crate::ca::maybe_renew_ca_cert(
                    &config_lock,
                    &renewal_dir,
                    &ca_key_pem,
                    rs,
                    renewal_threshold,
                )
            })
            .await;
            match renewed {
                Err(e) => warn!("admin-server: CA renewal task panicked: {e}"),
                Ok(result) => match result {
                    Ok(true) => info!(
                        "admin-server: renewed the CA certificate (same key; glyph unchanged)"
                    ),
                    Ok(false) => (),
                    Err(e) => warn!("admin-server: CA renewal check failed: {e:#}"),
                },
            }
        }
    }
    let serial = store.alloc_serial();
    let serving_identity = match (serving_identity, renewal_of) {
        (Some(identity), _) => Some(identity),
        (None, Some(serial)) if name.eq_ignore_ascii_case(SERVING_SAN) => store
            .live_for_name(name)
            .await
            .ok()
            .and_then(|records| records.into_iter().find(|r| r.serial == serial))
            .and_then(|record| {
                crate::tls::admin_cert_identity_from_pem(record.cert_pem.as_bytes()).ok()
            }),
        (None, _) => None,
    };
    if renewal_of.is_some()
        && name.eq_ignore_ascii_case(SERVING_SAN)
        && serving_identity.is_none()
    {
        return Ok(Signed {
            resp: reject(
                "the serving certificate being renewed has no valid protocol-v6 identity",
            ),
            push: None,
            replacement_crl: None,
        });
    }
    let mut san = vec![SanEntry::Dns(name.to_string())];
    if let Some(identity) = serving_identity {
        san.push(SanEntry::Uri(identity.server_id.uri()));
        if identity.controller {
            san.push(SanEntry::Uri(admin_proto::CONTROLLER_ROLE_URI.to_string()));
        }
    }
    let resp = sign_csr(
        &dir,
        store,
        &signing.ca_key_pem,
        &record_req.csr_pem,
        &san,
        validity,
        serial,
    )
    .await?;
    let mut replacement_crl = None;
    if let SignResponse::Ok(SignOk { ref signed_cert_pem, .. }) = resp {
        store
            .commit_issuance(record_req, serial, name, signed_cert_pem, &groups)
            .await
            .context("committing the issuance")?;
        if let Some(old) = replacement_record {
            let revoked = store
                .revoke(
                    old.serial,
                    ca_store::Revocation {
                        serial: old.serial,
                        revoked_unix: ca_store::now_unix(),
                        reason: format!("replaced during restore by serial {serial}"),
                    },
                )
                .await?;
            anyhow::ensure!(revoked, "replacement certificate stopped being live");
            store.write_crl(&signing.ca_key_pem).await?;
            replacement_crl = Some(tokio::fs::read_to_string(store.crl_path()).await?);
        }
    }
    audit(&dir, audit_admin, audit_op, name, validity).await;
    Ok(Signed {
        resp,
        push: if groups.is_empty() {
            None
        } else {
            Some(PushPlan { id: record_req.id.clone(), name: name.to_string(), groups })
        },
        replacement_crl,
    })
}

/// Handle a admin-server enrollment: authenticate the admin, require the
/// scoped enrollment policy, and sign the CSR with the reserved
/// [`SERVING_SAN`].

#[derive(Clone)]
struct IdentityPusher {
    controller: admin_proto::AdminServerId,
    client: admin_client::AuthenticatedPkiClient,
    home_ca: CertificateDer<'static>,
}

impl IdentityPusher {
    async fn new(state: &Server) -> Result<Self> {
        Ok(IdentityPusher {
            controller: state.read(move |state| state.map.controller).await,
            client: state.outbound_client().await?,
            home_ca: state.home_ca_der.clone(),
        })
    }

    async fn push(
        &self,
        server: admin_proto::AdminServerId,
        addr: SocketAddr,
        req: &AddIdentityRequest,
    ) -> Result<Option<u32>> {
        tokio::time::timeout(
            PUSH_TIMEOUT,
            admin_client::push_identity(
                &self.client,
                addr,
                server,
                server == self.controller,
                self.home_ca.clone(),
                req,
            ),
        )
        .await
        .map_err(|_| anyhow!("timed out after {}s", PUSH_TIMEOUT.as_secs()))?
    }
}

/// Fan the freshly signed identity out to every id-map host we know of:
/// the local map directly, configured peers and mDNS-discovered admin
/// servers over authenticated TLS. Returns warnings for the failures —
/// the sign itself already succeeded.
pub(super) async fn push_registrations(
    state: &Arc<Server>,
    plan: &PushPlan,
) -> (admin_proto::OperationId, Vec<String>) {
    let Some((primary, secondary)) = plan.groups.split_first() else {
        return (admin_proto::OperationId::new(), Vec::new());
    };
    let operation_id = admin_proto::OperationId::new();
    if let Some(dir) = state.ca_dir().await {
        audit(
            &dir,
            "controller",
            "fanout-id-map",
            &format!("operation {operation_id}: {}", plan.name),
            Duration::ZERO,
        )
        .await;
    }
    let req = AddIdentityRequest {
        operation_id,
        san: plan.name.clone(),
        primary_group: primary.clone(),
        groups: secondary.to_vec(),
    };
    let mut warnings = Vec::new();
    let (my_id, targets) = state
        .read(move |state| {
            let mut targets: Vec<_> = state
                .map
                .servers
                .iter()
                .filter(|s| {
                    s.roles.contains(Role::IdMap)
                        && s.state == admin_proto::ServerState::Registered
                })
                .map(|s| (s.id, s.addr))
                .collect();
            targets.sort_by_key(|(id, _)| *id);
            (state.cfg.server_id, targets)
        })
        .await;
    // Local id-map first (no TLS loopback).
    if targets.iter().any(|(id, _)| *id == my_id) {
        match state.add_identity(&req).await {
            AddIdentityResponse::Ok(_) => (),
            AddIdentityResponse::Err { reason } => {
                warnings.push(format!("local id-map registration failed: {reason}"))
            }
        }
    }
    let pusher = match IdentityPusher::new(state).await {
        Ok(pusher) => pusher,
        Err(e) => {
            warnings.push(format!("loading outbound identity failed: {e:#}"));
            return (operation_id, warnings);
        }
    };
    let mut results: Vec<_> = stream::iter(
        targets.into_iter().filter(|(id, _)| *id != my_id).map(|(id, addr)| {
            let req = req.clone();
            let pusher = pusher.clone();
            async move {
                let result = pusher.push(id, addr, &req).await;
                (id, addr, result)
            }
        }),
    )
    .buffer_unordered(32)
    .collect()
    .await;
    results.sort_by_key(|(id, _, _)| *id);
    for (id, addr, result) in results {
        match result {
            Ok(Some(uid)) => {
                info!("admin-server: registered {} (uid {uid}) on {addr}", plan.name)
            }
            Ok(None) => (),
            Err(e) => warnings.push(format!(
                "id-map registration on server {id} at {addr} failed: {e:#}"
            )),
        }
    }
    // Mark the issuance's id-map push complete only when nothing failed at
    // all — local *or* remote (a local failure is a real failure, not part
    // of the baseline). A partial push leaves a warning, so the record
    // stays in the recovery set (`pending_pushes`) and is retried on the
    // next poll or daemon restart. The `set_push_done` read-modify-write
    // is serialized with `handle_revoke` by the state write lock, so the two
    // can't clobber each other's field on the same record.
    if warnings.is_empty() {
        let id = plan.id.clone();
        state
            .write_async(async move |state| {
                if let Some(ca) = state.ca.as_mut() {
                    let _ = ca.store.set_push_done(&id).await;
                }
            })
            .await;
    }
    (operation_id, warnings)
}

fn reconcile_identity_at(
    records: &[ca_store::IssuedRecord],
    index: usize,
    now: u64,
) -> bool {
    let record = &records[index];
    !record.groups.is_empty()
        && records.iter().any(|candidate| {
            candidate.name.eq_ignore_ascii_case(&record.name) && candidate.live(now)
        })
        && !records[index + 1..].iter().any(|candidate| {
            candidate.name.eq_ignore_ascii_case(&record.name)
                && !candidate.groups.is_empty()
        })
}

pub(super) async fn reconcile_identities_to_target(
    state: &Server,
    server: admin_proto::AdminServerId,
    addr: SocketAddr,
) -> Result<()> {
    let mut records = state
        .read_async(async move |state| {
            state
                .ca
                .as_ref()
                .context("this host does not hold the CA")?
                .store
                .list_signed()
                .await
        })
        .await?;
    records.sort_by_key(|record| record.serial);
    let now = ca_store::now_unix();
    let operation_id = admin_proto::OperationId::new();
    let pusher = IdentityPusher::new(state).await?;
    let mut audited = false;
    for (index, record) in records.iter().enumerate() {
        if !reconcile_identity_at(&records, index, now) {
            continue;
        }
        let (primary, secondary) = record
            .groups
            .split_first()
            .expect("reconciliation predicate requires groups");
        if !audited {
            if let Some(dir) = state.ca_dir().await {
                audit(
                    &dir,
                    "controller",
                    "reconcile-id-map",
                    &format!("operation {operation_id}: server {server} at {addr}"),
                    Duration::ZERO,
                )
                .await;
            }
            audited = true;
        }
        let req = AddIdentityRequest {
            operation_id,
            san: record.name.clone(),
            primary_group: primary.clone(),
            groups: secondary.to_vec(),
        };
        match pusher.push(server, addr, &req).await? {
            Some(uid) => info!(
                "admin-server: reconciled {} (uid {uid}) on server {server} at {addr}",
                record.name
            ),
            None => bail!(
                "server {server} at {addr} was granted IdMap but does not advertise that role"
            ),
        }
    }
    Ok(())
}
