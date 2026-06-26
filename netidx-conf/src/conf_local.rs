//! Local control-socket client. The on-box `ca` CLI uses this to drive the
//! running conf daemon over its `0600` unix socket — no TLS and no admin
//! password, because the daemon trusts the local peer by `SO_PEERCRED` and
//! authorizes it as a superuser. Unix-only: the socket is a daemon feature
//! and the daemon is unix.
//!
//! The wire types are the same [`crate::conf_proto`] messages the network
//! [`crate::conf_client`] sends, so the daemon dispatches both through one
//! code path; only the transport (a unix socket) and the authorization
//! (local superuser vs. a pinned-TLS admin password) differ. The credential
//! fields the admin-management requests carry are sent empty and ignored by
//! the daemon on this socket.

use crate::conf_proto::{
    self, AddRoleAdminRequest, AdminListResponse, AdminMgmtResponse, ClientHello,
    ListAdminsRequest, NodeKind, PROTOCOL_VERSION, RemoveAdminRequest, Request,
    RotateAutorenewResponse, RotateRecoveryResponse, Secret, ServerHello,
    SetAdminPolicyRequest,
};
use anyhow::{Context, Result, bail};
use std::path::Path;
use tokio::net::UnixStream;
use zeroize::Zeroizing;

/// Connect to the daemon's local control socket (beside the conf-server
/// config) and complete the hello exchange, returning the stream positioned
/// to send one request. A connect failure almost always means the daemon
/// isn't running on this host — say so.
async fn connect(cfg_path: &Path) -> Result<UnixStream> {
    let path = crate::conf_server::local_socket_path(cfg_path);
    let mut stream = UnixStream::connect(&path).await.with_context(|| {
        format!(
            "connecting to the conf daemon's control socket at {} — start the conf \
             server on this host (the daemon owns the CA; the CLI talks to it)",
            path.display()
        )
    })?;
    conf_proto::write_msg(
        &mut stream,
        &ClientHello { protocol_version: PROTOCOL_VERSION, kind: NodeKind::Client },
    )
    .await
    .context("sending ClientHello")?;
    let _: ServerHello =
        conf_proto::read_msg(&mut stream).await.context("reading ServerHello")?;
    Ok(stream)
}

/// Whether the conf daemon is reachable on its local control socket — a
/// cheap connect probe. Commands that have an offline break-glass path
/// (`recovery rotate`, `auto-approve`) use this to choose the daemon-mediated
/// path when it's up and the offline flock path when it isn't. There's a
/// benign TOCTOU (the daemon could stop right after): the offline path takes
/// the CA flock, which fails cleanly if the daemon is in fact up.
pub async fn daemon_running(cfg_path: &Path) -> bool {
    let path = crate::conf_server::local_socket_path(cfg_path);
    UnixStream::connect(&path).await.is_ok()
}

/// Empty credentials. The daemon ignores the admin/password fields on the
/// local socket — the `SO_PEERCRED` check at accept is the authorization.
fn no_creds() -> (String, Secret) {
    (String::new(), Secret(String::new()))
}

/// Mint a new role admin `name` with `policy` and `new_password`.
pub async fn add_role_admin(
    cfg_path: &Path,
    name: &str,
    new_password: &str,
    policy: crate::ca_policy::Policy,
) -> Result<()> {
    let mut s = connect(cfg_path).await?;
    let (admin, password) = no_creds();
    conf_proto::write_msg(
        &mut s,
        &Request::AddRoleAdmin(AddRoleAdminRequest {
            admin,
            password,
            name: name.to_string(),
            new_password: Secret(new_password.to_string()),
            policy,
        }),
    )
    .await?;
    match conf_proto::read_msg::<_, AdminMgmtResponse>(&mut s).await? {
        AdminMgmtResponse::Ok => Ok(()),
        AdminMgmtResponse::Err { reason } => bail!("the CA refused: {reason}"),
    }
}

/// Replace role admin `target`'s policy.
pub async fn set_admin_policy(
    cfg_path: &Path,
    target: &str,
    policy: crate::ca_policy::Policy,
) -> Result<()> {
    let mut s = connect(cfg_path).await?;
    let (admin, password) = no_creds();
    conf_proto::write_msg(
        &mut s,
        &Request::SetAdminPolicy(SetAdminPolicyRequest {
            admin,
            password,
            target: target.to_string(),
            policy,
        }),
    )
    .await?;
    match conf_proto::read_msg::<_, AdminMgmtResponse>(&mut s).await? {
        AdminMgmtResponse::Ok => Ok(()),
        AdminMgmtResponse::Err { reason } => bail!("the CA refused: {reason}"),
    }
}

/// Remove role admin `target`.
pub async fn remove_admin(cfg_path: &Path, target: &str) -> Result<()> {
    let mut s = connect(cfg_path).await?;
    let (admin, password) = no_creds();
    conf_proto::write_msg(
        &mut s,
        &Request::RemoveAdmin(RemoveAdminRequest {
            admin,
            password,
            target: target.to_string(),
        }),
    )
    .await?;
    match conf_proto::read_msg::<_, AdminMgmtResponse>(&mut s).await? {
        AdminMgmtResponse::Ok => Ok(()),
        AdminMgmtResponse::Err { reason } => bail!("the CA refused: {reason}"),
    }
}

/// List the admin roster (tier + policy per admin).
pub async fn list_admins(cfg_path: &Path) -> Result<Vec<crate::ca_policy::AdminInfo>> {
    let mut s = connect(cfg_path).await?;
    let (admin, password) = no_creds();
    conf_proto::write_msg(
        &mut s,
        &Request::ListAdmins(ListAdminsRequest { admin, password }),
    )
    .await?;
    match conf_proto::read_msg::<_, AdminListResponse>(&mut s).await? {
        AdminListResponse::Ok { admins } => Ok(admins),
        AdminListResponse::Err { reason } => bail!("the CA refused: {reason}"),
    }
}

/// Mint a fresh recovery (off-box break-glass) password. The daemon
/// re-wraps the recovery slot using its own on-box autorenew credential and
/// returns the new password in grouped display form (shown to the operator
/// once, never stored).
pub async fn rotate_recovery(cfg_path: &Path) -> Result<Zeroizing<String>> {
    let mut s = connect(cfg_path).await?;
    conf_proto::write_msg(&mut s, &Request::RotateRecovery).await?;
    match conf_proto::read_msg::<_, RotateRecoveryResponse>(&mut s).await? {
        RotateRecoveryResponse::Ok { recovery_password } => {
            Ok(Zeroizing::new(recovery_password.0.clone()))
        }
        RotateRecoveryResponse::Err { reason } => bail!("the CA refused: {reason}"),
    }
}

/// Rotate the box's autorenew signing credential live (hot-swap, no
/// downtime). Returns an optional warning — e.g. the keytab was rewritten in
/// plaintext because this CA was set up `--insecure-no-tpm`.
pub async fn rotate_autorenew(cfg_path: &Path) -> Result<Option<String>> {
    let mut s = connect(cfg_path).await?;
    conf_proto::write_msg(&mut s, &Request::RotateAutorenew).await?;
    match conf_proto::read_msg::<_, RotateAutorenewResponse>(&mut s).await? {
        RotateAutorenewResponse::Ok { warning } => Ok(warning),
        RotateAutorenewResponse::Err { reason } => bail!("the CA refused: {reason}"),
    }
}
