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
        REQUEST_AUTHENTICATION, REQUEST_SERVER_UNLOCK, prepare_password_authentication,
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
        get_info, handle_apply_controller_state, handle_apply_referral_edit,
        handle_approve_delegation, handle_deny_delegation, handle_deregister,
        handle_list_delegations, handle_poll_delegation, handle_reconcile_controller,
        handle_register, handle_remove_server, handle_request_delegation,
    },
};
use crate::admin_proto::{
    self, AddIdentityResponse, ApplyControllerStateResponse, ApplyCrlResponse,
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
    let peer_is_controller = peer_admin.is_some_and(|p| p.controller);
    let hello: ClientHello =
        admin_proto::read_msg(&mut tls).await.context("reading ClientHello")?;
    let (domain, server_id, controller) = state
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
            controller,
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
    if let Err(reason) = authorize_request_class(
        request_authorization(&req),
        local,
        peer_is_admin_server,
        peer_is_controller,
    ) {
        bail!(reason);
    }
    // Only a CA can run a password KDF. Reserve the source before dispatch,
    // sleeping outside the global Argon2 semaphore when recent failures impose
    // a delay. Local-control requests are kernel-credential authorized and do
    // not participate in network throttling.
    let password_attempt =
        if !local && state.has_ca().await && password_credential(&req).is_some() {
            let (attempt, delay) = state.begin_password_attempt(peer.ip()).await?;
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            Some(attempt)
        } else {
            None
        };
    let credential = password_credential(&req).cloned();
    let needs_server_unlock = request_needs_server_unlock(&req);
    let authentication = prepare_password_authentication(
        state,
        &signs,
        credential,
        password_attempt.clone(),
    )
    .await;
    let unlock = if matches!(&authentication, Some(Err(_))) {
        None
    } else {
        prepare_server_unlock(state, &signs, needs_server_unlock).await
    };
    let request = async move {
        match req {
            Request::GetInfo => {
                let resp = get_info(state).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing GetInfoResponse")
            }
            Request::Login(req) => {
                let resp = handle_login(state, &req).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing LoginResponse")
            }
            Request::Logout(req) => {
                let resp = handle_logout(state, &req).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing LogoutResponse")
            }
            Request::Sign(req) => {
                let resp = handle_sign(state, &req).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing SignResponse")
            }
            Request::Enroll(req) => {
                let resp = handle_enroll(state, &req, local).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing SignResponse")
            }
            Request::AddIdentity(req) => {
                let resp = if !peer_is_admin_server {
                    AddIdentityResponse::Err {
                        reason: "identity registration requires a admin-server peer \
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
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing PollResponse")
            }
            Request::ListQueue(req) => {
                let resp = match ca_dir(state).await {
                    None => ListQueueResponse::Err {
                        reason: "this host does not hold the CA".to_string(),
                    },
                    Some(_) => handle_list_queue(state, &req).await,
                };
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing ListQueueResponse")
            }
            Request::Approve(req) => {
                let resp = handle_approve_request(state, &req).await;
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
                                    Some(&map),
                                )
                                .await
                            })
                            .await
                    }
                };
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing DenyResponse")
            }
            Request::Revoke(req) => {
                let resp = match ca_dir(state).await {
                    None => RevokeResponse::Err {
                        reason: "this host does not hold the CA".to_string(),
                    },
                    Some(_) => handle_revoke(state, &req).await,
                };
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing RevokeResponse")
            }
            Request::ListIssued(req) => handle_list_issued(&mut tls, state, &req).await,
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
                    Some(_) => handle_list_delegations(state, &req).await,
                };
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing ListDelegationsResponse")
            }
            Request::ApproveDelegation(req) => {
                let resp = handle_approve_delegation(state, req).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing ApproveDelegationResponse")
            }
            Request::DenyDelegation(req) => {
                let resp = handle_deny_delegation(state, &req).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing DenyDelegationResponse")
            }
            Request::ApplyReferralEdit(req) => {
                let resp = if !peer_is_admin_server {
                    ApplyReferralEditResponse::Err {
                        reason:
                            "a referral edit requires a admin-server peer certificate"
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
                        reason: "register requires a admin-server peer certificate"
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
                        reason: "deregister requires a admin-server peer certificate"
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
                let resp = handle_remove_server(state, req).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing RemoveServerResponse")
            }
            Request::ReadPerms(req) => {
                let resp = handle_read_perms(state, &req, local).await;
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
                let resp = handle_edit_perms(state, &req, local).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing EditPermsResponse")
            }
            Request::ApplyPermsEdit(req) => {
                let resp = if !peer_is_admin_server {
                    ApplyPermsEditResponse::Err {
                        reason: "a perms edit requires a admin-server peer certificate"
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
                let resp = if !peer_is_controller {
                    ApplyCrlResponse::Err {
                        reason:
                            "a CRL update requires the home CA controller certificate"
                                .to_string(),
                    }
                } else {
                    handle_apply_crl(state, &req).await
                };
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing ApplyCrlResponse")
            }
            Request::ApplyControllerState(req) => {
                let resp = if !peer_is_controller {
                    ApplyControllerStateResponse::Err {
                        reason:
                            "controller-state reconciliation requires the exact home CA \
                                     controller certificate"
                                .to_string(),
                    }
                } else {
                    handle_apply_controller_state(state, &req).await
                };
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing ApplyControllerStateResponse")
            }
            Request::ReconcileController(req) => {
                let resp = handle_reconcile_controller(state, &req, local).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing ReconcileControllerResponse")
            }
            Request::AddRoleAdmin(req) => {
                let prepared =
                    match REQUEST_AUTHENTICATION.try_with(Clone::clone).unwrap_or(None) {
                        Some(Err(reason)) => Err(reason),
                        _ => prepare_role_slot(state, &signs, &req).await,
                    };
                let resp =
                    handle_add_role_admin_prepared(state, &req, local, prepared).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing AdminMgmtResponse")
            }
            Request::SetAdminPolicy(req) => {
                let resp = handle_set_admin_policy(state, &req, local).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing AdminMgmtResponse")
            }
            Request::RemoveAdmin(req) => {
                let resp = handle_remove_admin(state, &req, local).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing AdminMgmtResponse")
            }
            Request::ListAdmins(req) => {
                let resp = handle_list_admins(state, &req, local).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing AdminListResponse")
            }
            Request::ControlService(req) => {
                let resp = handle_control_service(state, &req).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing ControlServiceResponse")
            }
            Request::ApplyServiceControl(req) => {
                let resp = if !peer_is_admin_server {
                    ApplyServiceControlResponse::Err {
                        reason:
                            "service control requires a admin-server peer certificate"
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
                let resp = handle_backup(state, &req).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing BackupResponse")
            }
            Request::ExternalCaCsr => {
                let resp = handle_external_ca_csr(state, local).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing ExternalCaCsrResponse")
            }
            Request::ExternalCaInstall(req) => {
                let resp = handle_external_ca_install(state, &req, local).await;
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
        }
    };
    let request = REQUEST_SERVER_UNLOCK.scope(unlock, request);
    let result = REQUEST_AUTHENTICATION.scope(authentication, request).await;
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
enum RequestAuthorization {
    Public,
    ControllerOnly,
    NodeSelf,
    AdminAuthenticated,
    LocalOnly,
}

fn request_authorization(req: &Request) -> RequestAuthorization {
    use Request::*;
    match req {
        GetInfo | Enqueue(_) | Poll(_) | GetCrl | RequestDelegation(_)
        | PollDelegation(_) | GetMapVersion | GetMap => RequestAuthorization::Public,
        AddIdentity(_)
        | ApplyCrl(_)
        | ApplyControllerState(_)
        | GetPerms
        | ApplyPermsEdit(_)
        | ApplyReferralEdit(_)
        | ApplyServiceControl(_) => RequestAuthorization::ControllerOnly,
        Register(_) | Deregister => RequestAuthorization::NodeSelf,
        RotateRecovery | RotateAutorenew | Backup(_) | ExternalCaCsr
        | ExternalCaInstall(_) | CaStatus => RequestAuthorization::LocalOnly,
        Login(_)
        | Logout(_)
        | Sign(_)
        | Enroll(_)
        | ListQueue(_)
        | Approve(_)
        | Deny(_)
        | Revoke(_)
        | ListIssued(_)
        | ListDelegations(_)
        | ApproveDelegation(_)
        | DenyDelegation(_)
        | RemoveServer(_)
        | ReadPerms(_)
        | EditPerms(_)
        | AddRoleAdmin(_)
        | SetAdminPolicy(_)
        | RemoveAdmin(_)
        | ListAdmins(_)
        | ControlService(_)
        | ReconcileController(_) => RequestAuthorization::AdminAuthenticated,
    }
}

/// The credential of a request that can actually invoke the password KDF.
/// `Logout` deliberately is not included: it accepts only a session token and
/// rejects a password without consulting the vault.
fn password_credential(req: &Request) -> Option<&admin_proto::AdminCredential> {
    use Request::*;
    let credential = match req {
        Login(req) => &req.credential,
        Sign(req) => &req.credential,
        Enroll(req) => &req.credential,
        ListQueue(req) => &req.credential,
        Approve(req) => &req.credential,
        Deny(req) => &req.credential,
        Revoke(req) => &req.credential,
        ListIssued(req) => &req.credential,
        ListDelegations(req) => &req.credential,
        ApproveDelegation(req) => &req.credential,
        DenyDelegation(req) => &req.credential,
        RemoveServer(req) => &req.credential,
        ReadPerms(req) => &req.credential,
        EditPerms(req) => &req.credential,
        AddRoleAdmin(req) => &req.credential,
        SetAdminPolicy(req) => &req.credential,
        RemoveAdmin(req) => &req.credential,
        ListAdmins(req) => &req.credential,
        ControlService(req) => &req.credential,
        GetInfo
        | Logout(_)
        | AddIdentity(_)
        | Enqueue(_)
        | Poll(_)
        | GetCrl
        | RequestDelegation(_)
        | PollDelegation(_)
        | ApplyReferralEdit(_)
        | ApplyCrl(_)
        | Backup(_)
        | ApplyControllerState(_)
        | Register(_)
        | Deregister
        | GetMapVersion
        | GetMap
        | GetPerms
        | ApplyPermsEdit(_)
        | ApplyServiceControl(_)
        | RotateRecovery
        | RotateAutorenew
        | ExternalCaCsr
        | ExternalCaInstall(_)
        | CaStatus => return None,
        ReconcileController(req) => &req.credential,
    };
    matches!(credential, admin_proto::AdminCredential::Password { .. })
        .then_some(credential)
}

fn request_needs_server_unlock(req: &Request) -> bool {
    matches!(
        req,
        Request::Sign(_)
            | Request::Enroll(_)
            | Request::Approve(_)
            | Request::Revoke(_)
            | Request::RemoveServer(_)
            | Request::ReconcileController(_)
            | Request::Backup(_)
            | Request::ExternalCaCsr
            | Request::ExternalCaInstall(_)
    )
}

fn authorize_request_class(
    class: RequestAuthorization,
    local: bool,
    peer_is_admin_server: bool,
    peer_is_controller: bool,
) -> std::result::Result<(), &'static str> {
    match class {
        RequestAuthorization::Public | RequestAuthorization::AdminAuthenticated => Ok(()),
        RequestAuthorization::ControllerOnly if peer_is_controller => Ok(()),
        RequestAuthorization::NodeSelf if peer_is_admin_server => Ok(()),
        RequestAuthorization::LocalOnly if local => Ok(()),
        RequestAuthorization::ControllerOnly => {
            Err("request requires the home CA controller certificate")
        }
        RequestAuthorization::NodeSelf => {
            Err("request requires a protocol-v6 home-CA node certificate")
        }
        RequestAuthorization::LocalOnly => {
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
