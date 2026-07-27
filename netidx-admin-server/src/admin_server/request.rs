#[cfg(test)]
#[path = "request_tests.rs"]
mod tests;

use super::{
    Server,
    admins::{
        handle_add_role_admin_prepared, handle_list_admins, handle_login, handle_logout,
        handle_remove_admin, handle_set_admin_policy,
    },
    auth::{
        PreparedAdminAuthentication, PreparedServerUnlock, prepare_admin_authentication,
        prepare_role_slot, prepare_server_unlock,
    },
    ca_dir,
    ca_ops::{
        handle_backup, handle_ca_status, handle_external_ca_csr,
        handle_external_ca_install, rotate_autorenew, rotate_recovery,
    },
    enrollment::handle_enroll,
    issuance::handle_sign,
    permissions::{
        handle_apply_perms_edit, handle_edit_perms, handle_get_perms, handle_read_perms,
    },
    queue::{
        handle_approve_request, handle_deny, handle_enqueue, handle_list_issued,
        handle_list_queue, handle_poll,
    },
    revocation::{handle_apply_crl, handle_get_crl, handle_revoke},
    service_control::{handle_apply_service_control, handle_control_service},
    topology::{
        get_info, handle_apply_ca_state, handle_apply_referral_edit,
        handle_approve_delegation, handle_deny_delegation, handle_deregister,
        handle_list_delegations, handle_poll_delegation, handle_reconcile_ca,
        handle_register, handle_remove_server, handle_request_delegation,
    },
};
use crate::admin_proto::{
    self, AddIdentityResponse, ApplyCaStateResponse, ApplyCrlResponse,
    ApplyPermsEditResponse, ApplyReferralEditResponse, ApplyServiceControlResponse,
    ClientHello, DelegationPollResponse, DelegationResponse, DenyResponse,
    EnqueueResponse, GetMapResponse, GetMapVersionResponse, ListDelegationsResponse,
    ListQueueResponse, MapVersion, PROTOCOL_VERSION, RegisterResponse, Request,
    RevokeResponse, ServerHello,
};
use anyhow::{Context, Result, bail};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::Semaphore;

pub(super) async fn serve_request<S>(
    mut tls: S,
    peer: SocketAddr,
    peer_ident: Option<PeerIdent>,
    local: bool,
    state: &Arc<Server>,
    signs: Arc<Semaphore>,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    let peer_admin = peer_ident.as_ref().filter(|p| p.home_ca).and_then(|p| p.admin);
    let peer_is_admin_server = peer_admin.is_some();
    let peer_is_ca = peer_admin.is_some_and(|p| p.ca);
    let hello: ClientHello =
        admin_proto::read_msg(&mut tls).await.context("reading ClientHello")?;
    let (domain, server_id, ca) = state
        .read(move |state| {
            (state.cfg.domain.clone(), state.cfg.server_id, state.cfg.roles.ca.is_some())
        })
        .await;
    admin_proto::write_msg(
        &mut tls,
        &ServerHello {
            protocol_version: PROTOCOL_VERSION,
            domain,
            roles: state.roles().await,
            server_id,
            ca,
        },
    )
    .await
    .context("writing ServerHello")?;
    anyhow::ensure!(
        hello.protocol_version == PROTOCOL_VERSION,
        "client speaks protocol version {} but we speak {PROTOCOL_VERSION}",
        hello.protocol_version
    );
    let req: Request =
        admin_proto::read_msg(&mut tls).await.context("reading Request")?;
    let requirements = request_requirements(&req);
    if let Err(reason) =
        authorize_request_origin(requirements, local, peer_is_admin_server, peer_is_ca)
    {
        bail!(reason);
    }
    // Only a CA can run a password KDF. Reserve the source before dispatch,
    // sleeping outside the global Argon2 semaphore when recent failures impose
    // a delay. Local-control requests are kernel-credential authorized and do
    // not participate in admin domain throttling.
    let credential = requirements.admin_credential();
    let password_attempt = if !local
        && state.has_ca().await
        && matches!(credential, Some(admin_proto::AdminCredential::Password { .. }))
    {
        let (attempt, delay) = state.begin_password_attempt(peer.ip()).await;
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        Some(attempt)
    } else {
        None
    };
    let authentication = match credential {
        Some(credential) => Some(
            prepare_admin_authentication(
                state,
                &signs,
                credential,
                password_attempt.clone(),
            )
            .await,
        ),
        None => None,
    };
    // Unlocking the server's own key is a full Argon2id (64 MiB) on the bounded
    // blocking pool. Never do it for a credential we already know is bad, or an
    // unauthenticated client gets to spend it at will.
    let server_unlock = if requirements.needs_server_unlock() {
        Some(
            if authentication
                .as_ref()
                .is_some_and(PreparedAdminAuthentication::credential_failed)
            {
                PreparedServerUnlock::failed("authentication failed")
            } else {
                prepare_server_unlock(state, &signs).await
            },
        )
    } else {
        None
    };
    let request_authentication =
        || authentication.as_ref().expect("admin request has prepared authentication");
    let server_unlock =
        || server_unlock.as_ref().expect("signing request has a prepared server key");
    let result = match req {
        Request::GetInfo => {
            let resp = get_info(state).await;
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing GetInfoResponse")
        }
        Request::Login(req) => {
            let resp = handle_login(state, &req, request_authentication()).await;
            admin_proto::write_msg(&mut tls, &resp).await.context("writing LoginResponse")
        }
        Request::Logout(req) => {
            let resp = handle_logout(state, &req).await;
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing LogoutResponse")
        }
        Request::Sign(req) => {
            let resp =
                handle_sign(state, &req, request_authentication(), server_unlock()).await;
            admin_proto::write_msg(&mut tls, &resp).await.context("writing SignResponse")
        }
        Request::Enroll(req) => {
            let resp = handle_enroll(
                state,
                &req,
                request_authentication(),
                server_unlock(),
                local,
            )
            .await;
            admin_proto::write_msg(&mut tls, &resp).await.context("writing SignResponse")
        }
        Request::AddIdentity(req) => {
            let resp = if !peer_is_admin_server {
                AddIdentityResponse::Err {
                    reason: "identity registration requires an admin-server peer \
                             certificate"
                        .to_string(),
                }
            } else {
                state.add_identity(&req).await
            };
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing AddIdentityResponse")
        }
        Request::Enqueue(req) => {
            let resp = match ca_dir(state).await {
                None => EnqueueResponse::Err {
                    reason: "this host does not hold the CA".to_string(),
                },
                Some(_) => {
                    state
                        .write_async(async move |state| {
                            handle_enqueue(
                                state.ca.as_mut().expect("CA role held"),
                                &req,
                                peer,
                                peer_ident.as_ref(),
                            )
                            .await
                        })
                        .await
                }
            };
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing EnqueueResponse")
        }
        Request::Poll(req) => {
            let resp = handle_poll(state, &req).await;
            admin_proto::write_msg(&mut tls, &resp).await.context("writing PollResponse")
        }
        Request::ListQueue(req) => {
            let resp = match ca_dir(state).await {
                None => ListQueueResponse::Err {
                    reason: "this host does not hold the CA".to_string(),
                },
                Some(_) => handle_list_queue(state, &req, request_authentication()).await,
            };
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing ListQueueResponse")
        }
        Request::Approve(req) => {
            let resp = handle_approve_request(
                state,
                &req,
                request_authentication(),
                server_unlock(),
            )
            .await;
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing ApproveResponse")
        }
        Request::Deny(req) => {
            let resp = match ca_dir(state).await {
                None => DenyResponse::Err {
                    reason: "this host does not hold the CA".to_string(),
                },
                Some(_) => {
                    state
                        .write_async(async move |state| {
                            let map = state.map.clone();
                            handle_deny(
                                state.ca.as_mut().expect("CA role held"),
                                &req,
                                request_authentication(),
                                Some(&map),
                            )
                            .await
                        })
                        .await
                }
            };
            admin_proto::write_msg(&mut tls, &resp).await.context("writing DenyResponse")
        }
        Request::Revoke(req) => {
            let resp = match ca_dir(state).await {
                None => RevokeResponse::Err {
                    reason: "this host does not hold the CA".to_string(),
                },
                Some(_) => {
                    handle_revoke(state, &req, request_authentication(), server_unlock())
                        .await
                }
            };
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing RevokeResponse")
        }
        Request::ListIssued(req) => {
            handle_list_issued(&mut tls, state, &req, request_authentication()).await
        }
        Request::RequestDelegation(req) => {
            let resp = match ca_dir(state).await {
                None => DelegationResponse::Err {
                    reason: "this host does not hold the CA".to_string(),
                },
                Some(_) => handle_request_delegation(state, &req, peer).await,
            };
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing DelegationResponse")
        }
        Request::PollDelegation(req) => {
            let resp = match ca_dir(state).await {
                None => DelegationPollResponse::Unknown,
                Some(dir) => handle_poll_delegation(&dir, &req).await,
            };
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing DelegationPollResponse")
        }
        Request::ListDelegations(req) => {
            let resp = match ca_dir(state).await {
                None => ListDelegationsResponse::Err {
                    reason: "this host does not hold the CA".to_string(),
                },
                Some(_) => {
                    handle_list_delegations(state, &req, request_authentication()).await
                }
            };
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing ListDelegationsResponse")
        }
        Request::ApproveDelegation(req) => {
            let resp =
                handle_approve_delegation(state, req, request_authentication()).await;
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing ApproveDelegationResponse")
        }
        Request::DenyDelegation(req) => {
            let resp =
                handle_deny_delegation(state, &req, request_authentication()).await;
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing DenyDelegationResponse")
        }
        Request::ApplyReferralEdit(req) => {
            let resp = if !peer_is_admin_server {
                ApplyReferralEditResponse::Err {
                    reason: "a referral edit requires an admin-server peer certificate"
                        .to_string(),
                }
            } else {
                handle_apply_referral_edit(state, &req).await
            };
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing ApplyReferralEditResponse")
        }
        Request::GetCrl => {
            let resp = handle_get_crl(state).await;
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing GetCrlResponse")
        }
        Request::Register(req) => {
            let resp = if !peer_is_admin_server {
                RegisterResponse::Err {
                    reason: "register requires an admin-server peer certificate"
                        .to_string(),
                }
            } else {
                let server_id = peer_admin.expect("NodeSelf authorized").server_id;
                handle_register(state, server_id, &req).await
            };
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing RegisterResponse")
        }
        Request::Deregister => {
            let resp = if !peer_is_admin_server {
                RegisterResponse::Err {
                    reason: "deregister requires an admin-server peer certificate"
                        .to_string(),
                }
            } else {
                let server_id = peer_admin.expect("NodeSelf authorized").server_id;
                handle_deregister(state, server_id).await
            };
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing RegisterResponse")
        }
        Request::GetMapVersion => {
            let resp = GetMapVersionResponse::Ok(MapVersion {
                version: state.read(move |state| state.map.version).await,
            });
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing GetMapVersionResponse")
        }
        Request::GetMap => {
            let resp =
                GetMapResponse::Ok(state.read(move |state| state.map.clone()).await);
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing GetMapResponse")
        }
        Request::RemoveServer(req) => {
            let resp = handle_remove_server(
                state,
                req,
                request_authentication(),
                server_unlock(),
            )
            .await;
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing RemoveServerResponse")
        }
        Request::ReadPerms(req) => {
            let resp =
                handle_read_perms(state, &req, request_authentication(), local).await;
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing ReadPermsResponse")
        }
        Request::GetPerms => {
            let resp = handle_get_perms(state).await;
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing GetPermsResponse")
        }
        Request::EditPerms(req) => {
            let resp =
                handle_edit_perms(state, &req, request_authentication(), local).await;
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing EditPermsResponse")
        }
        Request::ApplyPermsEdit(req) => {
            let resp = if !peer_is_admin_server {
                ApplyPermsEditResponse::Err {
                    reason: "a perms edit requires an admin-server peer certificate"
                        .to_string(),
                }
            } else {
                handle_apply_perms_edit(state, &req).await
            };
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing ApplyPermsEditResponse")
        }
        Request::ApplyCrl(req) => {
            let resp = if !peer_is_ca {
                ApplyCrlResponse::Err {
                    reason: "a CRL update requires the home CA certificate".to_string(),
                }
            } else {
                handle_apply_crl(state, &req).await
            };
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing ApplyCrlResponse")
        }
        Request::ApplyCaState(req) => {
            let resp = if !peer_is_ca {
                ApplyCaStateResponse::Err {
                    reason: "CA-state reconciliation requires the exact home CA \
                                     certificate"
                        .to_string(),
                }
            } else {
                handle_apply_ca_state(state, &req).await
            };
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing ApplyCaStateResponse")
        }
        Request::ReconcileCa(req) => {
            let resp = handle_reconcile_ca(
                state,
                &req,
                request_authentication(),
                server_unlock(),
                local,
            )
            .await;
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing ReconcileCaResponse")
        }
        Request::AddRoleAdmin(req) => {
            let authentication = request_authentication();
            let prepared = match authentication.credential_failure() {
                Some(reason) => Err(reason),
                None => prepare_role_slot(state, &signs, &req).await,
            };
            let resp = handle_add_role_admin_prepared(
                state,
                &req,
                authentication,
                local,
                prepared,
            )
            .await;
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing AdminMgmtResponse")
        }
        Request::SetAdminPolicy(req) => {
            let resp =
                handle_set_admin_policy(state, &req, request_authentication(), local)
                    .await;
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing AdminMgmtResponse")
        }
        Request::RemoveAdmin(req) => {
            let resp =
                handle_remove_admin(state, &req, request_authentication(), local).await;
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing AdminMgmtResponse")
        }
        Request::ListAdmins(req) => {
            let resp =
                handle_list_admins(state, &req, request_authentication(), local).await;
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing AdminListResponse")
        }
        Request::ControlService(req) => {
            let resp =
                handle_control_service(state, &req, request_authentication()).await;
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing ControlServiceResponse")
        }
        Request::ApplyServiceControl(req) => {
            let resp = if !peer_is_admin_server {
                ApplyServiceControlResponse::Err {
                    reason: "service control requires an admin-server peer certificate"
                        .to_string(),
                }
            } else {
                handle_apply_service_control(state, &req).await
            };
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing ApplyServiceControlResponse")
        }
        Request::RotateRecovery => {
            let resp = rotate_recovery(state, &signs, local).await;
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing RotateRecoveryResponse")
        }
        Request::RotateAutorenew => {
            let resp = rotate_autorenew(state, &signs, local).await;
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing RotateAutorenewResponse")
        }
        Request::Backup(req) => {
            let resp = handle_backup(state, &req, server_unlock()).await;
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing BackupResponse")
        }
        Request::ExternalCaCsr => {
            let resp = handle_external_ca_csr(state, server_unlock(), local).await;
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing ExternalCaCsrResponse")
        }
        Request::ExternalCaInstall(req) => {
            let resp =
                handle_external_ca_install(state, &req, server_unlock(), local).await;
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing ExternalCaInstallResponse")
        }
        Request::CaStatus => {
            let resp = handle_ca_status(state, local).await;
            admin_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing CaStatusResponse")
        }
    };
    drop(password_attempt);
    result
}

pub(super) struct PeerIdent {
    pub(super) san: String,
    pub(super) serial: Option<u64>,
    pub(super) spki_fp: Option<String>,
    pub(super) admin: Option<crate::tls::AdminCertIdentity>,
    pub(super) home_ca: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServerKeyRequirement {
    NotNeeded,
    Required,
}

#[derive(Debug, Clone, Copy)]
enum RequestRequirements<'a> {
    Public,
    Admin {
        credential: &'a admin_proto::AdminCredential,
        server_key: ServerKeyRequirement,
    },
    Logout,
    CaOnly,
    NodeSelf,
    LocalOnly {
        server_key: ServerKeyRequirement,
    },
}

impl<'a> RequestRequirements<'a> {
    fn admin_credential(self) -> Option<&'a admin_proto::AdminCredential> {
        match self {
            Self::Admin { credential, .. } => Some(credential),
            Self::Public
            | Self::Logout
            | Self::CaOnly
            | Self::NodeSelf
            | Self::LocalOnly { .. } => None,
        }
    }

    fn needs_server_unlock(self) -> bool {
        match self {
            Self::Admin { server_key, .. } | Self::LocalOnly { server_key } => {
                server_key == ServerKeyRequirement::Required
            }
            Self::Public | Self::Logout | Self::CaOnly | Self::NodeSelf => false,
        }
    }
}

fn request_requirements(req: &Request) -> RequestRequirements<'_> {
    use Request::*;
    use ServerKeyRequirement::{NotNeeded, Required};
    match req {
        GetInfo | Enqueue(_) | Poll(_) | GetCrl | RequestDelegation(_)
        | PollDelegation(_) | GetMapVersion | GetMap => RequestRequirements::Public,
        AddIdentity(_)
        | ApplyCrl(_)
        | ApplyCaState(_)
        | GetPerms
        | ApplyPermsEdit(_)
        | ApplyReferralEdit(_)
        | ApplyServiceControl(_) => RequestRequirements::CaOnly,
        Register(_) | Deregister => RequestRequirements::NodeSelf,
        RotateRecovery | RotateAutorenew | CaStatus => {
            RequestRequirements::LocalOnly { server_key: NotNeeded }
        }
        Backup(_) | ExternalCaCsr | ExternalCaInstall(_) => {
            RequestRequirements::LocalOnly { server_key: Required }
        }
        Logout(_) => RequestRequirements::Logout,
        Login(req) => RequestRequirements::Admin {
            credential: &req.credential,
            server_key: NotNeeded,
        },
        Sign(req) => RequestRequirements::Admin {
            credential: &req.credential,
            server_key: Required,
        },
        Enroll(req) => RequestRequirements::Admin {
            credential: &req.credential,
            server_key: Required,
        },
        ListQueue(req) => RequestRequirements::Admin {
            credential: &req.credential,
            server_key: NotNeeded,
        },
        Approve(req) => RequestRequirements::Admin {
            credential: &req.credential,
            server_key: Required,
        },
        Deny(req) => RequestRequirements::Admin {
            credential: &req.credential,
            server_key: NotNeeded,
        },
        Revoke(req) => RequestRequirements::Admin {
            credential: &req.credential,
            server_key: Required,
        },
        ListIssued(req) => RequestRequirements::Admin {
            credential: &req.credential,
            server_key: NotNeeded,
        },
        ListDelegations(req) => RequestRequirements::Admin {
            credential: &req.credential,
            server_key: NotNeeded,
        },
        ApproveDelegation(req) => RequestRequirements::Admin {
            credential: &req.credential,
            server_key: NotNeeded,
        },
        DenyDelegation(req) => RequestRequirements::Admin {
            credential: &req.credential,
            server_key: NotNeeded,
        },
        RemoveServer(req) => RequestRequirements::Admin {
            credential: &req.credential,
            server_key: Required,
        },
        ReadPerms(req) => RequestRequirements::Admin {
            credential: &req.credential,
            server_key: NotNeeded,
        },
        EditPerms(req) => RequestRequirements::Admin {
            credential: &req.credential,
            server_key: NotNeeded,
        },
        AddRoleAdmin(req) => RequestRequirements::Admin {
            credential: &req.credential,
            server_key: NotNeeded,
        },
        SetAdminPolicy(req) => RequestRequirements::Admin {
            credential: &req.credential,
            server_key: NotNeeded,
        },
        RemoveAdmin(req) => RequestRequirements::Admin {
            credential: &req.credential,
            server_key: NotNeeded,
        },
        ListAdmins(req) => RequestRequirements::Admin {
            credential: &req.credential,
            server_key: NotNeeded,
        },
        ControlService(req) => RequestRequirements::Admin {
            credential: &req.credential,
            server_key: NotNeeded,
        },
        ReconcileCa(req) => RequestRequirements::Admin {
            credential: &req.credential,
            server_key: Required,
        },
    }
}

fn authorize_request_origin(
    requirements: RequestRequirements<'_>,
    local: bool,
    peer_is_admin_server: bool,
    peer_is_ca: bool,
) -> std::result::Result<(), &'static str> {
    match requirements {
        RequestRequirements::Public
        | RequestRequirements::Admin { .. }
        | RequestRequirements::Logout => Ok(()),
        RequestRequirements::CaOnly if peer_is_ca => Ok(()),
        RequestRequirements::NodeSelf if peer_is_admin_server => Ok(()),
        RequestRequirements::LocalOnly { .. } if local => Ok(()),
        RequestRequirements::CaOnly => Err("request requires the home CA certificate"),
        RequestRequirements::NodeSelf => {
            Err("request requires a protocol-v6 home-CA node certificate")
        }
        RequestRequirements::LocalOnly { .. } => {
            Err("request is available only over the protected local control socket")
        }
    }
}

pub(super) fn cert_signed_by(leaf_der: &[u8], ca_der: &[u8]) -> bool {
    use x509_parser::prelude::{FromDer, X509Certificate};
    let Ok((_, leaf)) = X509Certificate::from_der(leaf_der) else { return false };
    let Ok((_, ca)) = X509Certificate::from_der(ca_der) else { return false };
    leaf.verify_signature(Some(ca.public_key())).is_ok()
}
