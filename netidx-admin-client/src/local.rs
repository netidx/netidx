//! Local control-socket client. The on-box `ca` CLI uses this to drive the
//! running admin daemon over its `0600` unix socket — no TLS and no admin
//! password, because the daemon trusts the local peer by `SO_PEERCRED` and
//! authorizes it as a superuser. Unix-only: the socket is a daemon feature
//! and the daemon is unix.
//!
//! The wire types are the same [`netidx_admin_proto`] messages the network
//! [`crate::transport`] sends, so the daemon dispatches both through one
//! code path; only the transport (a unix socket) and the authorization
//! (local superuser vs. a pinned-TLS admin password) differ. The credential
//! fields the admin-management requests carry are sent empty and ignored by
//! the daemon on this socket.

use anyhow::{Context, Result, anyhow, bail};
use netidx_admin_proto::{
    self, AddRoleAdminRequest, AdminListResponse, AdminMgmtResponse, BackupOk,
    BackupRequest, BackupResponse, CaStatus, CaStatusResponse, ClientHello,
    EditPermsRequest, EditPermsResponse, EnrollRequest, ExternalCaCsrOk,
    ExternalCaCsrResponse, ExternalCaInstallOk, ExternalCaInstallRequest,
    ExternalCaInstallResponse, ListAdminsRequest, NodeKind, PROTOCOL_VERSION, PeerResult,
    PropagationOk, ReadPermsOk, ReadPermsRequest, ReadPermsResponse,
    ReconcileControllerRequest, ReconcileControllerResponse, RemoveAdminRequest, Request,
    RotateAutorenewResponse, RotateRecoveryResponse, Secret, ServerHello,
    SetAdminPolicyRequest, SignResponse,
};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::net::UnixStream;
use zeroize::Zeroizing;

#[derive(Debug, Clone)]
pub struct BackupOutcome {
    pub target: std::path::PathBuf,
    pub ca_fingerprint: String,
    pub controller: netidx_admin_proto::AdminServerId,
    pub map_version: u64,
    pub highest_serial: u64,
    pub files: u64,
    pub bytes: u64,
    pub manifest_sha256: String,
}

fn local_socket_path(cfg_path: &Path) -> PathBuf {
    cfg_path.parent().unwrap_or_else(|| Path::new(".")).join("admin.sock")
}

/// Bound on connect + hello. Connecting to a bound unix socket succeeds into
/// the listen backlog whether or not anyone is accepting, so without this a
/// wedged daemon hangs the caller forever — including the TUI, which probes
/// this socket. Deliberately covers only the handshake: the request and its
/// response are unbounded, because `Backup` legitimately takes a while.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Connect to the daemon's local control socket (beside the admin-server
/// config) and complete the hello exchange, returning the stream positioned
/// to send one request. A connect failure almost always means the daemon
/// isn't running on this host — say so.
async fn connect(cfg_path: &Path) -> Result<UnixStream> {
    let path = local_socket_path(cfg_path);
    tokio::time::timeout(CONNECT_TIMEOUT, connect_inner(&path)).await.map_err(|_| {
        anyhow!(
            "the admin daemon's control socket at {} accepted no connection within \
             {CONNECT_TIMEOUT:?} — the daemon is running but not responding",
            path.display()
        )
    })?
}

async fn connect_inner(path: &Path) -> Result<UnixStream> {
    let mut stream = UnixStream::connect(path).await.with_context(|| {
        format!(
            "connecting to the admin daemon's control socket at {} — start the admin \
             server on this host (the daemon owns the CA; the CLI talks to it)",
            path.display()
        )
    })?;
    netidx_admin_proto::write_msg(
        &mut stream,
        &ClientHello { protocol_version: PROTOCOL_VERSION, kind: NodeKind::Client },
    )
    .await
    .context("sending ClientHello")?;
    let _: ServerHello =
        netidx_admin_proto::read_msg(&mut stream).await.context("reading ServerHello")?;
    Ok(stream)
}

/// Whether the admin daemon is reachable on its local control socket — a
/// cheap connect probe. Commands that have an offline break-glass path
/// (`recovery rotate`, `auto-approve`) use this to choose the daemon-mediated
/// path when it's up and the offline guarded path when it isn't. There's a
/// benign TOCTOU (the daemon could stop right after): the offline path takes
/// the config-directory guard, which fails cleanly if the daemon is in fact up.
pub async fn daemon_running(cfg_path: &Path) -> bool {
    let path = local_socket_path(cfg_path);
    UnixStream::connect(&path).await.is_ok()
}

/// Emit a renewal CSR for this running externally-signed controller CA. The
/// controller unlocks its in-memory vault credential; no recovery password or
/// intentional outage is required.
pub async fn external_ca_csr(cfg_path: &Path) -> Result<(String, String)> {
    let mut s = connect(cfg_path).await?;
    netidx_admin_proto::write_msg(&mut s, &Request::ExternalCaCsr).await?;
    match netidx_admin_proto::read_msg::<_, ExternalCaCsrResponse>(&mut s).await? {
        ExternalCaCsrResponse::Ok(ExternalCaCsrOk { common_name, csr_pem }) => {
            Ok((common_name, csr_pem))
        }
        ExternalCaCsrResponse::Err { reason } => {
            bail!("the controller refused to emit an external-CA CSR: {reason}")
        }
    }
}

/// Install a hardware/external-PKI-signed renewal while the controller keeps
/// serving. Only the protected local socket exposes this operation.
pub async fn external_ca_install(
    cfg_path: &Path,
    signed_cert_pem: String,
    root_pem: Option<String>,
) -> Result<String> {
    let mut s = connect(cfg_path).await?;
    netidx_admin_proto::write_msg(
        &mut s,
        &Request::ExternalCaInstall(ExternalCaInstallRequest {
            signed_cert_pem,
            root_pem,
        }),
    )
    .await?;
    match netidx_admin_proto::read_msg::<_, ExternalCaInstallResponse>(&mut s).await? {
        ExternalCaInstallResponse::Ok(ExternalCaInstallOk { ca_fingerprint }) => {
            Ok(ca_fingerprint)
        }
        ExternalCaInstallResponse::Err { reason } => {
            bail!("the controller refused the external-CA certificate: {reason}")
        }
    }
}

pub async fn ca_status(cfg_path: &Path) -> Result<CaStatus> {
    let mut s = connect(cfg_path).await?;
    netidx_admin_proto::write_msg(&mut s, &Request::CaStatus).await?;
    match netidx_admin_proto::read_msg::<_, CaStatusResponse>(&mut s).await? {
        CaStatusResponse::Ok(status) => Ok(status),
        CaStatusResponse::Err { reason } => bail!("the CA refused status: {reason}"),
    }
}

/// Ask the running controller to publish a point-in-time-consistent recovery
/// bundle. This request exists only on the protected local control socket; the
/// target is a path on this host and the daemon refuses to overwrite it.
pub async fn backup(cfg_path: &Path, target: &Path) -> Result<BackupOutcome> {
    let mut s = connect(cfg_path).await?;
    netidx_admin_proto::write_msg(
        &mut s,
        &Request::Backup(BackupRequest { target: target.to_string_lossy().into_owned() }),
    )
    .await?;
    match netidx_admin_proto::read_msg::<_, BackupResponse>(&mut s).await? {
        BackupResponse::Ok(BackupOk {
            target,
            ca_fingerprint,
            controller,
            map_version,
            highest_serial,
            files,
            bytes,
            manifest_sha256,
        }) => Ok(BackupOutcome {
            target: target.into(),
            ca_fingerprint,
            controller,
            map_version,
            highest_serial,
            files,
            bytes,
            manifest_sha256,
        }),
        BackupResponse::Err { reason } => bail!("the CA refused backup: {reason}"),
    }
}

/// Re-push the restored controller route, authoritative map, CRL, and resolver
/// topology over the protected local control socket.
pub async fn reconcile_controller(
    cfg_path: &Path,
) -> Result<(netidx_admin_proto::OperationId, Vec<PeerResult>)> {
    let mut s = connect(cfg_path).await?;
    let (admin, password) = no_creds();
    netidx_admin_proto::write_msg(
        &mut s,
        &Request::ReconcileController(ReconcileControllerRequest {
            credential: netidx_admin_proto::AdminCredential::Password { admin, password },
        }),
    )
    .await?;
    match netidx_admin_proto::read_msg::<_, ReconcileControllerResponse>(&mut s).await? {
        ReconcileControllerResponse::Ok(PropagationOk { operation_id, peers }) => {
            Ok((operation_id, peers))
        }
        ReconcileControllerResponse::Err { reason } => {
            bail!("the CA refused controller reconciliation: {reason}")
        }
    }
}

/// Empty credentials. The daemon ignores the admin/password fields on the
/// local socket — the `SO_PEERCRED` check at accept is the authorization.
fn no_creds() -> (String, Secret) {
    (String::new(), Secret(String::new()))
}

/// Re-mint the admin server's own serving cert (the reserved
/// [`netidx_admin_proto::SERVING_SAN`]) over the local control socket.
/// renewd on the CA host uses this instead of a TLS enrollment to
/// localhost: the serving cert is the linchpin of TLS-to-self, so once it
/// expires a TLS renewal can never connect to renew it (a deadlock). The
/// local socket is plain (no TLS), so it works regardless of the current
/// serving cert's validity. `csr_pem` is a fresh CSR for the serving SAN;
/// `listen` is the admin server's own listen address (the daemon no-ops
/// recording itself as a peer). Auth is the `SO_PEERCRED` superuser check
/// at accept — the admin/password fields are sent empty and ignored.
pub async fn enroll(
    cfg_path: &Path,
    csr_pem: &str,
    listen: SocketAddr,
) -> Result<SignResponse> {
    let cfg = crate::admin_server_config::load_async(cfg_path).await?;
    let mut s = connect(cfg_path).await?;
    let (admin, password) = no_creds();
    netidx_admin_proto::write_msg(
        &mut s,
        &Request::Enroll(EnrollRequest {
            credential: netidx_admin_proto::AdminCredential::Password { admin, password },
            csr_pem: csr_pem.to_string(),
            listen,
            roles: netidx_admin_proto::Role::Resolver.into(),
            resolver_member: None,
            resolver_members: Vec::new(),
            cluster: netidx_admin_proto::ResolverClusterPlacement::Create {
                base: "/".to_string(),
            },
            renew_identity: Some(cfg.server_id),
            replaces: None,
        }),
    )
    .await
    .context("sending Enroll")?;
    netidx_admin_proto::read_msg::<_, SignResponse>(&mut s)
        .await
        .context("reading SignResponse")
}

/// Mint a new role admin `name` with `policy` and `new_password`.
pub async fn add_role_admin(
    cfg_path: &Path,
    name: &str,
    new_password: &str,
    policy: netidx_admin_proto::policy::Policy,
) -> Result<()> {
    let mut s = connect(cfg_path).await?;
    let (admin, password) = no_creds();
    netidx_admin_proto::write_msg(
        &mut s,
        &Request::AddRoleAdmin(AddRoleAdminRequest {
            credential: netidx_admin_proto::AdminCredential::Password { admin, password },
            name: name.to_string(),
            new_password: Secret(new_password.to_string()),
            policy,
        }),
    )
    .await?;
    match netidx_admin_proto::read_msg::<_, AdminMgmtResponse>(&mut s).await? {
        AdminMgmtResponse::Ok(()) => Ok(()),
        AdminMgmtResponse::Err { reason } => bail!("the CA refused: {reason}"),
    }
}

/// Replace role admin `target`'s policy.
pub async fn set_admin_policy(
    cfg_path: &Path,
    target: &str,
    policy: netidx_admin_proto::policy::Policy,
) -> Result<()> {
    let mut s = connect(cfg_path).await?;
    let (admin, password) = no_creds();
    netidx_admin_proto::write_msg(
        &mut s,
        &Request::SetAdminPolicy(SetAdminPolicyRequest {
            credential: netidx_admin_proto::AdminCredential::Password { admin, password },
            target: target.to_string(),
            policy,
        }),
    )
    .await?;
    match netidx_admin_proto::read_msg::<_, AdminMgmtResponse>(&mut s).await? {
        AdminMgmtResponse::Ok(()) => Ok(()),
        AdminMgmtResponse::Err { reason } => bail!("the CA refused: {reason}"),
    }
}

/// Remove role admin `target`.
pub async fn remove_admin(cfg_path: &Path, target: &str) -> Result<()> {
    let mut s = connect(cfg_path).await?;
    let (admin, password) = no_creds();
    netidx_admin_proto::write_msg(
        &mut s,
        &Request::RemoveAdmin(RemoveAdminRequest {
            credential: netidx_admin_proto::AdminCredential::Password { admin, password },
            target: target.to_string(),
        }),
    )
    .await?;
    match netidx_admin_proto::read_msg::<_, AdminMgmtResponse>(&mut s).await? {
        AdminMgmtResponse::Ok(()) => Ok(()),
        AdminMgmtResponse::Err { reason } => bail!("the CA refused: {reason}"),
    }
}

/// List the admin roster (tier + policy per admin).
pub async fn list_admins(
    cfg_path: &Path,
) -> Result<Vec<netidx_admin_proto::policy::AdminInfo>> {
    let mut s = connect(cfg_path).await?;
    let (admin, password) = no_creds();
    netidx_admin_proto::write_msg(
        &mut s,
        &Request::ListAdmins(ListAdminsRequest {
            credential: netidx_admin_proto::AdminCredential::Password { admin, password },
        }),
    )
    .await?;
    match netidx_admin_proto::read_msg::<_, AdminListResponse>(&mut s).await? {
        AdminListResponse::Ok(admins) => Ok(admins),
        AdminListResponse::Err { reason } => bail!("the CA refused: {reason}"),
    }
}

/// Read this resolver host's own permissions over the protected local socket.
/// The daemon confines `target_path` to its configured resolver level; no
/// network map or remote discovery hint participates in this operation.
pub async fn read_perms(cfg_path: &Path, target_path: &str) -> Result<String> {
    let mut s = connect(cfg_path).await?;
    let (admin, password) = no_creds();
    netidx_admin_proto::write_msg(
        &mut s,
        &Request::ReadPerms(ReadPermsRequest {
            credential: netidx_admin_proto::AdminCredential::Password { admin, password },
            target_path: target_path.to_string(),
        }),
    )
    .await?;
    match netidx_admin_proto::read_msg::<_, ReadPermsResponse>(&mut s).await? {
        ReadPermsResponse::Ok(ReadPermsOk { perms_json, .. }) => Ok(perms_json),
        ReadPermsResponse::Err { reason } => bail!("the admin daemon refused: {reason}"),
    }
}

/// Replace the `target_path` cluster's permissions with `perms_json`. The
/// daemon authorizes this local caller as a signing superuser (the
/// `SO_PEERCRED` gate is the authorization — no admin password), then routes
/// by the network map and propagates the edit to every member of the target
/// cluster exactly as a remote signing-admin edit would. Returns the per-peer
/// results so a partial (cluster-inconsistent) failure surfaces.
pub async fn edit_perms(
    cfg_path: &Path,
    target_path: &str,
    perms_json: &str,
) -> Result<Vec<PeerResult>> {
    let mut s = connect(cfg_path).await?;
    let (admin, password) = no_creds();
    netidx_admin_proto::write_msg(
        &mut s,
        &Request::EditPerms(EditPermsRequest {
            credential: netidx_admin_proto::AdminCredential::Password { admin, password },
            target_path: target_path.to_string(),
            perms_json: perms_json.to_string(),
        }),
    )
    .await?;
    match netidx_admin_proto::read_msg::<_, EditPermsResponse>(&mut s).await? {
        EditPermsResponse::Ok(PropagationOk { peers, .. }) => Ok(peers),
        EditPermsResponse::Err { reason } => bail!("the CA refused: {reason}"),
    }
}

/// Mint a fresh recovery (off-box break-glass) password. The daemon
/// re-wraps the recovery slot using its own on-box autorenew credential and
/// returns the new password in grouped display form (shown to the operator
/// once, never stored).
pub async fn rotate_recovery(cfg_path: &Path) -> Result<Zeroizing<String>> {
    let mut s = connect(cfg_path).await?;
    netidx_admin_proto::write_msg(&mut s, &Request::RotateRecovery).await?;
    match netidx_admin_proto::read_msg::<_, RotateRecoveryResponse>(&mut s).await? {
        RotateRecoveryResponse::Ok(recovery_password) => {
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
    netidx_admin_proto::write_msg(&mut s, &Request::RotateAutorenew).await?;
    match netidx_admin_proto::read_msg::<_, RotateAutorenewResponse>(&mut s).await? {
        RotateAutorenewResponse::Ok(warning) => Ok(warning),
        RotateAutorenewResponse::Err { reason } => bail!("the CA refused: {reason}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Connecting to a bound unix socket succeeds into the listen backlog even
    /// when nobody is accepting, so a wedged daemon used to hang the caller
    /// forever in the ServerHello read — including the TUI, which probes this
    /// socket on startup and after every install.
    #[tokio::test(start_paused = true)]
    async fn a_daemon_that_never_accepts_times_out_instead_of_hanging() {
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("admin-server.json");
        // Bind but never accept: exactly a daemon stuck before its accept loop.
        let _listener =
            tokio::net::UnixListener::bind(local_socket_path(&cfg_path)).unwrap();
        let e = ca_status(&cfg_path).await.unwrap_err();
        assert!(format!("{e:#}").contains("not responding"), "{e:#}");
    }
}
