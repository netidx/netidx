#[cfg(test)]
#[path = "queue_tests.rs"]
mod tests;

use super::{
    AUTORENEW_ADMIN, MutableState, Server, audit,
    auth::{
        PreparedAdminAuthentication, PreparedServerUnlock, admin_authority_over,
        authenticate, one_live_refusal, server_unlock, signing_slot,
    },
    ca_dir,
    enrollment::{authorize_enrollment, finish_enrollment, stage_enrollment},
    issuance::{
        Issuance, IssuanceMode, PushPlan, handle_sign_request_op, issue_serialized,
        one_live_name, propagate_issuance, push_registrations, restore_kind_matches,
    },
    request::PeerIdent,
};
use crate::{
    admin_proto::{
        self, AdminDomainMap, ApproveOk, ApproveRequest, ApproveResponse, DenyRequest,
        DenyResponse, EnqueueRequest, EnqueueResponse, IssuedEntry, ListIssuedRequest,
        ListIssuedResponse, ListQueueRequest, ListQueueResponse, PollRequest,
        PollResponse, QueueEntry, QueuedOk, SERVING_SAN, SignOk, SignRequest,
        SignResponse,
    },
    ca_store, transport,
};
use anyhow::{Context, Result};
use log::{info, warn};
use std::{net::SocketAddr, sync::Arc, time::Duration};

pub(super) async fn handle_list_issued<S>(
    stream: &mut S,
    state: &Arc<Server>,
    req: &ListIssuedRequest,
    authentication: &PreparedAdminAuthentication,
) -> Result<()>
where
    S: tokio::io::AsyncWrite + Unpin,
{
    let mut records = match state
        .write_async(async move |state| {
            let Some(ca) = state.ca.as_mut() else {
                return Err("this host does not hold the CA".to_string());
            };
            start_list_issued(ca, req, authentication).await
        })
        .await
    {
        Ok(records) => records,
        Err(reason) => {
            return admin_proto::write_msg(stream, &ListIssuedResponse::Err { reason })
                .await
                .context("writing ListIssuedResponse");
        }
    };
    let now = ca_store::now_unix();
    loop {
        match records.next().await {
            Ok(Some(record)) if record.not_after_unix > now => {
                let entry = IssuedEntry {
                    serial: record.serial,
                    name: record.name,
                    spki_fp: record.spki_fp,
                    not_after_unix: record.not_after_unix,
                    revoked: record.revoked.is_some(),
                };
                admin_proto::write_msg_unflushed(
                    &mut *stream,
                    &ListIssuedResponse::Entry { entry },
                )
                .await
                .context("writing ListIssuedResponse entry")?;
            }
            Ok(Some(_)) => {}
            Ok(None) => {
                break admin_proto::write_msg(&mut *stream, &ListIssuedResponse::End)
                    .await
                    .context("writing ListIssuedResponse end");
            }
            Err(e) => {
                break admin_proto::write_msg(
                    &mut *stream,
                    &ListIssuedResponse::Err {
                        reason: format!("listing issued certs: {e:#}"),
                    },
                )
                .await
                .context("writing ListIssuedResponse error");
            }
        }
    }
}

pub(super) async fn handle_approve_request(
    state: &Arc<Server>,
    req: &ApproveRequest,
    authentication: &PreparedAdminAuthentication,
    prepared_server_unlock: &PreparedServerUnlock,
) -> ApproveResponse {
    if ca_dir(state).await.is_none() {
        return ApproveResponse::Err {
            reason: "this host does not hold the CA".to_string(),
        };
    }
    let approved = state
        .write_async(async move |state| {
            let map = state.map.clone();
            handle_approve(
                state.ca.as_mut().expect("CA role held"),
                req,
                authentication,
                prepared_server_unlock,
                Some(&map),
            )
            .await
        })
        .await;
    let Approved { resp, push, enrollment, replacement_crl } = match approved {
        Ok(approved) => approved,
        Err(reason) => return ApproveResponse::Err { reason },
    };
    let mut warnings = match resp {
        SignResponse::Err { reason } => return ApproveResponse::Err { reason },
        SignResponse::Ok(SignOk {
            signed_cert_pem: _,
            trusted_pem: _,
            warnings,
            operation_id: _,
        }) => warnings,
    };
    let operation_id =
        propagate_issuance(state, &mut warnings, push, replacement_crl).await;
    if let Some((server_id, enrollment)) = enrollment
        && let Err(e) =
            finish_enrollment(state, server_id, &enrollment, prepared_server_unlock).await
    {
        warnings.push(format!("recording enrollment grant: {e:#}"));
    }
    ApproveResponse::Ok(ApproveOk { operation_id, warnings })
}

pub(super) async fn handle_poll(state: &Arc<Server>, req: &PollRequest) -> PollResponse {
    if ca_dir(state).await.is_none() {
        return PollResponse::Unknown;
    }
    let id = req.request_id.clone();
    let (mut resp, repush) = state
        .read_async(async move |state| {
            let store = &state.ca.as_ref().expect("CA role held").store;
            match store.status(&id).await {
                Ok(ca_store::Status::Pending(_)) => (PollResponse::Pending, None),
                Ok(ca_store::Status::Signed(s)) => {
                    let repush = match store.read_issued(&id).await {
                        Ok(Some(r)) if !r.groups.is_empty() && !r.push_done => {
                            Some(PushPlan { id, name: r.name, groups: r.groups })
                        }
                        // Nothing to re-push, or it already went out.
                        Ok(Some(_)) | Ok(None) => None,
                        // The record we just reported as Signed won't read
                        // back. Say so — silently skipping the id-map push
                        // leaves an identity the admin domain can't authorize.
                        Err(e) => {
                            warn!(
                                "admin-server: reading issued record {id} for id-map \
                                 re-push failed: {e:#}"
                            );
                            None
                        }
                    };
                    let resp = PollResponse::Signed(SignOk {
                        signed_cert_pem: s.signed_cert_pem,
                        trusted_pem: s.trusted_pem,
                        warnings: s.warnings,
                        operation_id: None,
                    });
                    (resp, repush)
                }
                Ok(ca_store::Status::Denied(d)) => {
                    (PollResponse::Denied { reason: d.reason }, None)
                }
                Ok(ca_store::Status::Unknown) => (PollResponse::Unknown, None),
                Err(e) => {
                    warn!("admin-server: queue status failed: {e:#}");
                    (PollResponse::Unknown, None)
                }
            }
        })
        .await;
    if let Some(plan) = repush {
        let (operation_id, push_warnings) = push_registrations(state, &plan).await;
        if let PollResponse::Signed(SignOk {
            warnings,
            operation_id: response_operation_id,
            ..
        }) = &mut resp
        {
            warnings.extend(push_warnings);
            *response_operation_id = Some(operation_id);
        }
    }
    resp
}

pub(super) async fn autorenew_sweep(
    ca: &mut ca_store::CaDir,
    password: &str,
    authentication: &PreparedAdminAuthentication,
    prepared_server_unlock: &PreparedServerUnlock,
) -> usize {
    let pending = match ca.store.pending().await {
        Ok(p) => p,
        Err(e) => {
            warn!("autorenew: scanning the queue failed: {e:#}");
            return 0;
        }
    };
    let mut approved = 0;
    for q in pending.iter().filter(|q| q.renewal_of.is_some()) {
        let req = ApproveRequest {
            credential: admin_proto::AdminCredential::password(AUTORENEW_ADMIN, password),
            request_id: q.id.clone(),
            id_map_groups: Vec::new(),
        };
        let result =
            handle_approve(ca, &req, authentication, prepared_server_unlock, None).await;
        match result {
            Ok(Approved { resp: SignResponse::Ok(_), .. }) => {
                approved += 1;
                info!("autorenew: approved renewal of {:?}", q.requested_name);
            }
            Ok(Approved { resp: SignResponse::Err { reason }, .. }) => {
                warn!(
                    "autorenew: signing renewal of {:?} failed: {reason}",
                    q.requested_name
                )
            }
            Err(reason) => {
                warn!(
                    "autorenew: approving renewal of {:?} failed: {reason}",
                    q.requested_name
                )
            }
        }
    }
    approved
}

async fn start_list_issued(
    ca: &mut ca_store::CaDir,
    req: &ListIssuedRequest,
    authentication: &PreparedAdminAuthentication,
) -> std::result::Result<ca_store::IssuedRecords, String> {
    authenticate(ca, &req.credential, authentication)?;
    ca.store
        .compact_issued(ca_store::now_unix())
        .await
        .map_err(|e| format!("compacting issued certs: {e:#}"))?;
    ca.store.issued_records().await.map_err(|e| format!("listing issued certs: {e:#}"))
}

/// A successful [`handle_approve`]: the signed outcome plus what the
/// dispatch arm needs to finish the job — the push plan for id-map
/// registration, and the peer address to record when the approved
/// entry was an admin-server enrollment.
struct Approved {
    resp: SignResponse,
    push: Option<PushPlan>,
    enrollment: Option<(admin_proto::AdminServerId, admin_proto::EnrollmentRequest)>,
    replacement_crl: Option<String>,
}

/// Approve a queued request: look it up, then sign it through the
/// exact same checks a synchronous [`SignRequest`] goes through (the
/// admin's SAN globs, validity cap, id-map group allowed-set), audited
/// as `op=approve`. The outer `Err` is a safe wire reason for
/// before-the-sign failures.
async fn handle_approve(
    ca: &mut ca_store::CaDir,
    req: &ApproveRequest,
    authentication: &PreparedAdminAuthentication,
    prepared_server_unlock: &PreparedServerUnlock,
    map: Option<&AdminDomainMap>,
) -> std::result::Result<Approved, String> {
    // This cheap precheck (no auth) rejects an already-terminal request;
    // issuance checks it again before committing.
    let queued = match ca.store.status(&req.request_id).await {
        Ok(ca_store::Status::Pending(q)) => q,
        Ok(ca_store::Status::Signed(_)) => {
            return Err("that request was already approved".to_string());
        }
        Ok(ca_store::Status::Denied(_)) => {
            return Err("that request was already denied".to_string());
        }
        Ok(ca_store::Status::Unknown) => {
            return Err("no such pending request (expired or never queued)".to_string());
        }
        Err(e) => return Err(format!("reading the queue: {e:#}")),
    };
    approve_serialized(ca, req, authentication, prepared_server_unlock, queued, map).await
}

/// Sign a queued request through the same checks a synchronous Sign goes
/// through (or, for a verified renewal / admin-server enrollment, the
/// narrower continuation gate), committing the issuance atomically.
/// `queued` came from the cheap precheck; `issue_serialized` re-checks it
/// is still pending before commit.
async fn approve_serialized(
    ca: &mut ca_store::CaDir,
    req: &ApproveRequest,
    authentication: &PreparedAdminAuthentication,
    prepared_server_unlock: &PreparedServerUnlock,
    queued: ca_store::QueuedReq,
    map: Option<&AdminDomainMap>,
) -> std::result::Result<Approved, String> {
    // A queued admin-server enrollment: gated on the approving admin's
    // scoped enrollment authority; signs the reserved serving SAN; no one-live
    // check and no id-map groups (an admin server isn't a user).
    if let Some(enrollment) = queued.enrollment.clone() {
        let authd = authenticate(ca, &req.credential, authentication)?;
        authorize_enrollment(&authd, &enrollment, map)?;
        let server_id = admin_proto::AdminServerId::new();
        let mut staged = map
            .cloned()
            .ok_or_else(|| "the CA-owned admin domain map is unavailable".to_string())?;
        stage_enrollment(&mut staged, server_id, &enrollment)
            .map_err(|e| format!("invalid enrollment grant: {e:#}"))?;
        let signing = server_unlock(ca, prepared_server_unlock).await?;
        let signed = issue_serialized(
            ca,
            &signing,
            Issuance {
                audit_admin: &authd.admin,
                audit_op: "enroll",
                record_req: &queued,
                name: SERVING_SAN,
                validity: ca.lifetimes.leaf_validity,
                mode: IssuanceMode::Enrollment {
                    identity: crate::tls::AdminCertIdentity { server_id, ca: false },
                },
                pending_request: Some(&req.request_id),
            },
        )
        .await
        .map_err(|e| format!("internal error: {e:#}"))?;
        let enrollment = matches!(&signed.resp, SignResponse::Ok(_))
            .then_some((server_id, enrollment));
        return Ok(Approved {
            resp: signed.resp,
            push: signed.push,
            enrollment,
            replacement_crl: signed.replacement_crl,
        });
    }
    // A verified renewal: continuation of an already-approved identity —
    // possession of the live key was proven at enqueue. The SAN-scope and
    // one-live-cert checks don't apply (a renewal's name *does* have a live
    // cert), the reserved serving name is allowed (admin servers renew
    // themselves), and the id-map is untouched (requested groups ignored).
    // `issue_serialized` re-checks the renewed serial is still live before
    // commit, so a revocation since enqueue refuses the renewal.
    if let Some(orig_serial) = queued.renewal_of {
        let authd = authenticate(ca, &req.credential, authentication)?;
        let validity = queued
            .requested_validity
            .min(authd.policy.max_validity)
            .max(Duration::from_secs(1));
        let signing = server_unlock(ca, prepared_server_unlock).await?;
        let signed = issue_serialized(
            ca,
            &signing,
            Issuance {
                audit_admin: &authd.admin,
                audit_op: "renew",
                record_req: &queued,
                name: &queued.requested_name,
                validity,
                mode: IssuanceMode::Renewal { serial: orig_serial },
                pending_request: Some(&req.request_id),
            },
        )
        .await
        .map_err(|e| format!("internal error: {e:#}"))?;
        return Ok(Approved {
            resp: signed.resp,
            push: signed.push,
            enrollment: None,
            replacement_crl: signed.replacement_crl,
        });
    }
    // Ordinary approval: the full policy checks under the *approving*
    // admin's slot, audited as `op=approve`, the record keyed by the
    // queued request's id so the enrollee polls it back.
    let sign_req = SignRequest {
        kind: queued.kind,
        credential: req.credential.clone(),
        csr_pem: queued.csr_pem.clone(),
        requested_name: queued.requested_name.clone(),
        requested_validity: queued.requested_validity,
        id_map_groups: req.id_map_groups.clone(),
        replaces_serial: queued.replaces_serial,
    };
    let signed = handle_sign_request_op(
        ca,
        &sign_req,
        authentication,
        prepared_server_unlock,
        "approve",
        &queued,
        Some(&req.request_id),
    )
    .await;
    Ok(Approved {
        resp: signed.resp,
        push: signed.push,
        enrollment: None,
        replacement_crl: signed.replacement_crl,
    })
}

pub(super) async fn handle_enqueue(
    ca: &mut ca_store::CaDir,
    req: &EnqueueRequest,
    peer: SocketAddr,
    peer_ident: Option<&PeerIdent>,
) -> EnqueueResponse {
    // Every queued request's security code IS its CSR's SPKI fingerprint, so a
    // CSR that doesn't parse yields a code-less queue entry that no admin can
    // ever select — to approve OR deny — leaving only TTL expiry to clear it.
    // Reject an unparseable CSR now (covers both the admin-server enrollment and
    // the plain signing paths below) so the enrollee hears it immediately and
    // the queue can't be clogged with un-actionable entries.
    if let Err(e) = transport::csr_fingerprint(&req.csr_pem) {
        return EnqueueResponse::Err {
            reason: format!("the certificate request (CSR) could not be parsed: {e:#}"),
        };
    }
    let store = &mut ca.store;
    // Enqueue is unauthenticated, and the checks below scan and parse every
    // record in `issued/`. Refuse a full queue first — that costs only the
    // bounded `queue/` and `denied/` directories — so a flood can't turn cheap
    // connections into an unbounded amount of the CA's disk I/O.
    match store.has_queue_capacity().await {
        Ok(true) => (),
        Ok(false) => {
            return EnqueueResponse::Err {
                reason: format!(
                    "the signing queue is full ({} pending requests)",
                    ca_store::MAX_PENDING
                ),
            };
        }
        Err(e) => {
            return EnqueueResponse::Err {
                reason: format!("checking the signing queue: {e:#}"),
            };
        }
    }
    // Admin-server enrollment: the name is the reserved serving SAN by
    // definition, so none of the name rules below apply — not the
    // reserved-name refusal (this is the sanctioned way to request it)
    // and not one-live-cert (every admin server on the admin domain holds the
    // same name). The real gate — the approving admin's
    // scoped enrollment authorization runs at approval; this entry just waits in
    // the queue under the same code-matching ceremony as any other.
    if let Some(enrollment) = &req.enrollment {
        let queued = ca_store::QueuedReq::new(
            req.kind,
            req.csr_pem.clone(),
            SERVING_SAN.to_string(),
            req.requested_validity,
            peer.to_string(),
            None,
            Some(enrollment.clone()),
        );
        return match store.enqueue(&queued).await {
            Ok(()) => {
                info!(
                    "admin-server: queued enrollment {} (listen {}) from {peer}",
                    queued.id, enrollment.listen,
                );
                EnqueueResponse::Ok(QueuedOk { request_id: queued.id })
            }
            Err(e) => EnqueueResponse::Err { reason: format!("{e:#}") },
        };
    }
    let name = req.requested_name.trim();
    if name.is_empty() {
        return EnqueueResponse::Err { reason: "requested name is empty".to_string() };
    }
    if req.requested_validity.is_zero() {
        return EnqueueResponse::Err { reason: "validity must be > 0".to_string() };
    }
    // A renewal must prove possession of *our* live cert for this exact
    // name: the presented leaf's serial AND its key fingerprint must match
    // a live record in our index. Binding the key (not just the serial)
    // stops a co-trusted foreign CA's cert with a colliding serial from
    // passing as a renewal of ours. `renewal_of` carries the originating
    // serial forward so approval can re-check it is still live.
    let renewal_of: Option<u64> = match peer_ident {
        Some(PeerIdent {
            san,
            serial: Some(serial),
            spki_fp: Some(fp),
            admin,
            home_ca,
        }) if san.eq_ignore_ascii_case(name)
            && (!name.eq_ignore_ascii_case(SERVING_SAN)
                || (*home_ca && admin.is_some())) =>
        {
            match store.live_for_name(name).await {
                Ok(live)
                    if live.iter().any(|s| s.serial == *serial && &s.spki_fp == fp) =>
                {
                    Some(*serial)
                }
                Ok(_) => None,
                Err(e) => {
                    warn!("admin-server: index lookup during enqueue failed: {e:#}");
                    None
                }
            }
        }
        // Not a renewal: no peer cert, or one whose SAN/serial/key don't
        // match the name being requested.
        Some(_) | None => None,
    };
    let verified_renewal = renewal_of.is_some();
    let replacement_of = if verified_renewal {
        None
    } else if let Some(serial) = req.replaces_serial {
        match store.list_signed().await {
            Ok(records) => {
                match records.into_iter().find(|record| record.serial == serial) {
                    Some(record)
                        if record.name.eq_ignore_ascii_case(name)
                            && restore_kind_matches(record.req.kind, req.kind)
                            && record.live(ca_store::now_unix()) =>
                    {
                        Some(serial)
                    }
                    Some(record)
                        if record.name.eq_ignore_ascii_case(name)
                            && restore_kind_matches(record.req.kind, req.kind) =>
                    {
                        None
                    }
                    Some(record) => {
                        return EnqueueResponse::Err {
                            reason: format!(
                                "replacement serial {serial} belongs to {:?} {:?}, not {:?} {:?}",
                                record.req.kind, record.name, req.kind, name
                            ),
                        };
                    }
                    None => {
                        return EnqueueResponse::Err {
                            reason: format!(
                                "replacement serial {serial} is not a live certificate for {name:?}"
                            ),
                        };
                    }
                }
            }
            Err(e) => {
                return EnqueueResponse::Err {
                    reason: format!("checking replacement serial: {e:#}"),
                };
            }
        }
    } else {
        None
    };
    if !verified_renewal {
        // Fail fast on the reserved name — approval would refuse it
        // anyway, but the enrollee should hear it now, not after the
        // admin clicked through. (An admin server renewing its own
        // serving cert is the legitimate exception above.)
        if name.eq_ignore_ascii_case(SERVING_SAN) {
            return EnqueueResponse::Err {
                reason: "that name is reserved for the admin server and cannot be \
                         issued"
                    .to_string(),
            };
        }
        // Same one-live-cert-per-name rule as the sign path, checked
        // here too so the enrollee hears it immediately instead of
        // after the admin clicked through an approval that would only
        // be refused. Resolver replicas are the deliberate exception: each
        // has its own key but presents the resolver cluster's shared TLS server name.
        if one_live_name(req.kind) && replacement_of.is_none() {
            match store.live_for_name(name).await {
                Ok(live) if !live.is_empty() => {
                    return EnqueueResponse::Err {
                        reason: one_live_refusal(name, &live),
                    };
                }
                Ok(_) => (),
                Err(e) => {
                    return EnqueueResponse::Err {
                        reason: format!("checking the issuance index: {e:#}"),
                    };
                }
            }
        }
    }
    let mut queued = ca_store::QueuedReq::new(
        req.kind,
        req.csr_pem.clone(),
        name.to_string(),
        req.requested_validity,
        peer.to_string(),
        renewal_of,
        None,
    );
    queued.replaces_serial = replacement_of;
    match store.enqueue(&queued).await {
        Ok(()) => {
            info!(
                "admin-server: queued {} {} for {name:?} from {peer}",
                if verified_renewal { "verified renewal" } else { "signing request" },
                queued.id
            );
            EnqueueResponse::Ok(QueuedOk { request_id: queued.id })
        }
        Err(e) => EnqueueResponse::Err { reason: format!("{e:#}") },
    }
}

/// Human/user identities are one-live-per-name. Resolver replicas instead
/// share one verified TLS server name while retaining independent private keys.
pub(super) async fn handle_list_queue(
    state: &Server,
    req: &ListQueueRequest,
    authentication: &PreparedAdminAuthentication,
) -> ListQueueResponse {
    let req = req.clone();
    state
        .write_async(async move |state| {
            handle_list_queue_inner(state, &req, authentication).await
        })
        .await
}

async fn handle_list_queue_inner(
    state: &mut MutableState,
    req: &ListQueueRequest,
    authentication: &PreparedAdminAuthentication,
) -> ListQueueResponse {
    let MutableState { map, ca, .. } = state;
    let ca = ca.as_mut().expect("CA role held");
    if let Err(reason) = authenticate(ca, &req.credential, authentication) {
        return ListQueueResponse::Err { reason };
    }
    match ca.store.pending().await {
        Ok(reqs) => ListQueueResponse::Ok(
            reqs.into_iter()
                .map(|q| {
                    let cluster_base =
                        q.enrollment.as_ref().and_then(|e| match &e.cluster {
                            admin_proto::ResolverClusterPlacement::Create { base } => {
                                Some(base.clone())
                            }
                            admin_proto::ResolverClusterPlacement::Join { cluster } => {
                                map.resolver_clusters
                                    .iter()
                                    .find(|c| c.id == *cluster)
                                    .map(|c| c.base.clone())
                            }
                        });
                    QueueEntry {
                        age_secs: q.age_secs(),
                        id: q.id,
                        kind: q.kind,
                        requested_name: q.requested_name,
                        requested_validity: q.requested_validity,
                        peer: q.peer,
                        csr_pem: q.csr_pem,
                        verified_renewal: q.renewal_of.is_some(),
                        enrollment: q.enrollment,
                        cluster_base,
                        replaces_serial: q.replaces_serial,
                    }
                })
                .collect(),
        ),
        Err(e) => ListQueueResponse::Err { reason: format!("listing the queue: {e:#}") },
    }
}

pub(super) async fn handle_deny(
    ca: &mut ca_store::CaDir,
    req: &DenyRequest,
    authentication: &PreparedAdminAuthentication,
    map: Option<&AdminDomainMap>,
) -> DenyResponse {
    // Cheap precheck (no auth) for an already-terminal/unknown request.
    let queued = match ca.store.status(&req.request_id).await {
        Ok(ca_store::Status::Pending(q)) => q,
        Ok(ca_store::Status::Signed(_)) => {
            return DenyResponse::Err {
                reason: "that request was already approved".to_string(),
            };
        }
        Ok(ca_store::Status::Denied(_)) => {
            return DenyResponse::Err {
                reason: "that request was already denied".to_string(),
            };
        }
        Ok(ca_store::Status::Unknown) => {
            return DenyResponse::Err {
                reason: "no such pending request (expired or never queued)".to_string(),
            };
        }
        Err(e) => {
            return DenyResponse::Err { reason: format!("reading the queue: {e:#}") };
        }
    };
    let authd = match authenticate(ca, &req.credential, authentication) {
        Ok(a) => a,
        Err(reason) => return DenyResponse::Err { reason },
    };
    // Denying a queued request blocks an issuance, so — like revoke — it is
    // scope-bound: an admin may deny only a request for a name it could have
    // signed (a serving-cert enrollment needs scoped enrollment authority).
    let authority = if let Some(enrollment) = &queued.enrollment {
        if signing_slot(&authd) {
            Ok(true)
        } else {
            authorize_enrollment(&authd, enrollment, map).map(|()| true)
        }
    } else {
        admin_authority_over(&authd, &queued.requested_name).map_err(|e| format!("{e:#}"))
    };
    match authority {
        Ok(true) => {}
        Ok(false) => {
            return DenyResponse::Err {
                reason: format!(
                    "admin {} is not authorized to deny requests for {:?}",
                    authd.admin, queued.requested_name
                ),
            };
        }
        Err(e) => {
            return DenyResponse::Err { reason: format!("evaluating authority: {e}") };
        }
    }
    let store = &mut ca.store;
    // Authoritative re-check immediately before the denial commit.
    match store.status(&req.request_id).await {
        Ok(ca_store::Status::Pending(_)) => {}
        Ok(ca_store::Status::Signed(_)) => {
            return DenyResponse::Err {
                reason: "that request was already approved".to_string(),
            };
        }
        Ok(ca_store::Status::Denied(_)) => {
            return DenyResponse::Err {
                reason: "that request was already denied".to_string(),
            };
        }
        Ok(ca_store::Status::Unknown) => {
            return DenyResponse::Err {
                reason: "no such pending request (expired or never queued)".to_string(),
            };
        }
        Err(e) => {
            return DenyResponse::Err { reason: format!("reading the queue: {e:#}") };
        }
    }
    match store.deny(&queued, &req.reason).await {
        Ok(()) => {
            audit(ca.dir(), &authd.admin, "deny", &queued.requested_name, Duration::ZERO)
                .await;
            DenyResponse::Ok(())
        }
        Err(e) => DenyResponse::Err { reason: format!("storing the denial: {e:#}") },
    }
}
