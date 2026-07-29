//! Opening and shutting one member's read gate.
//!
//! Scoped and routed exactly like service control — the same
//! `service_control_scopes`, the same one-target-per-request shape — because
//! it is the same kind of act: taking one member in or out of service without
//! touching anything else.

use super::{
    PUSH_TIMEOUT, Server, audit,
    auth::{PreparedAdminAuthentication, authenticate, safe_auth_failure},
    service_control::{
        base_for_server, registered_server_addr, service_control_authority,
    },
};
use crate::{
    admin_proto::{
        self, ApplySetReadGateRequest, ApplySetReadGateResponse, SetReadGateOk,
        SetReadGateRequest, SetReadGateResponse,
    },
    transport,
};
use anyhow::Context;
use log::info;
use netidx::resolver_server::config::ReadGate;
use std::{net::SocketAddr, sync::Arc, time::Duration};

/// CA-side: authenticate, authorize by the target's resolver cluster base, and
/// forward one peer-cert-gated [`Request::ApplySetReadGate`] — or apply
/// locally when the target is this CA host.
pub(super) async fn handle_set_read_gate(
    state: &Arc<Server>,
    req: &SetReadGateRequest,
    authentication: &PreparedAdminAuthentication,
) -> SetReadGateResponse {
    let err = |reason: String| SetReadGateResponse::Err { reason };
    if !state.has_ca().await {
        return err("a read-gate request must be sent to the CA host".to_string());
    }
    let auth = {
        let credential = req.credential.clone();
        state
            .write(move |state| {
                authenticate(
                    state.ca.as_mut().expect("CA role held"),
                    &credential,
                    authentication,
                )
            })
            .await
    };
    let authd = match auth {
        Ok(a) => a,
        Err(reason) => return err(safe_auth_failure(&req.credential, reason)),
    };
    let target_server = req.target_server;
    let base = state.read(move |state| base_for_server(&state.map, target_server)).await;
    let authorized = match &base {
        Some(base) => service_control_authority(&authd, base),
        None => super::auth::signing_slot(&authd),
    };
    if !authorized {
        return err(format!(
            "admin {:?} is not authorized to set the read gate on {}",
            authd.admin, req.target_server
        ));
    }
    let operation_id = admin_proto::OperationId::new();
    let ca_dir = state.ca_dir().await.expect("CA role held");
    audit(
        &ca_dir,
        &authd.admin,
        "set-read-gate",
        &format!("operation {operation_id}: {:?} on {}", req.gate, req.target_server),
        Duration::ZERO,
    )
    .await;
    match push(state, req.target_server, req.gate, operation_id).await {
        Ok(()) => SetReadGateResponse::Ok(SetReadGateOk { operation_id, gate: req.gate }),
        Err(reason) => err(format!(
            "operation {operation_id} on server {}: {reason}",
            req.target_server
        )),
    }
}

/// Deliver a gate to one registered member, locally if that member is us.
///
/// Also the decommission path: `RemoveServer` calls this to stop a departing
/// member answering subscribers before it starts tearing down its identity.
pub(super) async fn push(
    state: &Arc<Server>,
    target_server: admin_proto::AdminServerId,
    gate: ReadGate,
    operation_id: admin_proto::OperationId,
) -> std::result::Result<(), String> {
    let (my_id, target_addr) = state
        .read(move |state| {
            (state.cfg.server_id, registered_server_addr(&state.map, target_server))
        })
        .await;
    let Some(target_addr) = target_addr else {
        return Err(
            "target server is not registered in the authoritative map".to_string()
        );
    };
    push_to(state, target_server, target_addr, my_id, gate, operation_id).await
}

/// As `push`, but with the target's address supplied. `RemoveServer` needs
/// this: by the time it can report a result the target is out of the map, so
/// it captures the address before committing.
pub(super) async fn push_to(
    state: &Arc<Server>,
    target_server: admin_proto::AdminServerId,
    target_addr: SocketAddr,
    my_id: admin_proto::AdminServerId,
    gate: ReadGate,
    operation_id: admin_proto::OperationId,
) -> std::result::Result<(), String> {
    if target_server == my_id {
        let apply = ApplySetReadGateRequest { operation_id, gate };
        return match handle_apply_set_read_gate(state, &apply).await {
            ApplySetReadGateResponse::Ok(()) => Ok(()),
            ApplySetReadGateResponse::Err { reason } => Err(reason),
        };
    }
    let client = match state.outbound_client().await {
        Ok(client) => client,
        Err(e) => return Err(format!("loading outbound identity: {e:#}")),
    };
    let target_is_ca = state.read(move |state| target_server == state.map.ca).await;
    tokio::time::timeout(
        PUSH_TIMEOUT,
        transport::push_set_read_gate(
            &client,
            target_addr,
            target_server,
            target_is_ca,
            state.home_ca_der.clone(),
            operation_id,
            gate,
        ),
    )
    .await
    .map_err(|_| format!("timed out after {}s", PUSH_TIMEOUT.as_secs()))
    .and_then(|result| result.map_err(|e| format!("{e:#}")))
}

/// Server-to-server (peer-cert-gated): write the gate into this host's own
/// resolver config. The resolver picks it up from there, so it applies
/// without a restart and survives one.
pub(super) async fn handle_apply_set_read_gate(
    state: &Server,
    req: &ApplySetReadGateRequest,
) -> ApplySetReadGateResponse {
    info!(
        "admin-server: applying read gate operation {}: {:?}",
        req.operation_id, req.gate
    );
    apply_local(state, req.gate).await
}

async fn apply_local(state: &Server, gate: ReadGate) -> ApplySetReadGateResponse {
    let config_lock = state.config_lock.clone();
    state
        .write_async(async move |state| match state.cfg.roles.resolver.as_ref() {
            Some(role) => match write_gate(&config_lock, &role.config, gate).await {
                Ok(()) => ApplySetReadGateResponse::Ok(()),
                Err(e) => ApplySetReadGateResponse::Err { reason: format!("{e:#}") },
            },
            None => ApplySetReadGateResponse::Err {
                reason: "this host has no resolver role to gate".to_string(),
            },
        })
        .await
}

async fn write_gate(
    config_lock: &crate::config_lock::ConfigDirLock,
    path: &std::path::Path,
    gate: ReadGate,
) -> anyhow::Result<()> {
    let path = config_lock.require_contained(path)?;
    let mut rc = crate::resolver::ResolverConfig::load_async(&path)
        .await
        .with_context(|| format!("loading resolver config {}", path.display()))?;
    // Every member block in a host's own config is a launch choice of that
    // host, so the gate applies to all of them. In practice there is one.
    for member in rc.as_file_mut().member_servers.iter_mut() {
        member.read_gated = gate;
    }
    rc.save_async(&path).await.context("saving the resolver config")
}
