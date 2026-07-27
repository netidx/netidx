#[cfg(test)]
#[path = "service_control_tests.rs"]
mod tests;

use super::{
    PUSH_TIMEOUT, Server, audit,
    auth::{
        PreparedAdminAuthentication, authenticate, safe_auth_failure, scope_covers,
        signing_slot,
    },
};
use crate::{
    admin_proto::{
        self, AdminDomainMap, ApplyServiceControlRequest, ApplyServiceControlResponse,
        ControlServiceOk, ControlServiceRequest, ControlServiceResponse, ServiceUnit,
        ServiceUnitDef,
    },
    ca_vault, transport,
};
use log::info;
use std::{net::SocketAddr, sync::Arc, time::Duration};

/// Whether `authd` may control services on an admin server whose resolver cluster base
/// is `base`.
fn service_control_authority(authd: &ca_vault::Authenticated, base: &str) -> bool {
    signing_slot(authd) || scope_covers(&authd.policy.service_control_scopes, base)
}

/// The resolver cluster base of the admin server whose listen address is `addr`, from
/// the map — the authorization scope for controlling that server's services.
/// `None` if the server isn't in the map or runs no resolver cluster.
fn base_for_server(
    map: &AdminDomainMap,
    id: admin_proto::AdminServerId,
) -> Option<String> {
    let cluster = map
        .admin_servers
        .iter()
        .find(|s| s.id == id && s.state == admin_proto::ServerState::Registered)?
        .cluster?;
    map.resolver_clusters.iter().find(|c| c.id == cluster).map(|c| c.base.clone())
}

/// Resolve only an immutable registered identity to its current routing
/// address. Addresses are deliberately never accepted as lookup keys here.
fn registered_server_addr(
    map: &AdminDomainMap,
    id: admin_proto::AdminServerId,
) -> Option<SocketAddr> {
    map.admin_servers
        .iter()
        .find(|server| {
            server.id == id && server.state == admin_proto::ServerState::Registered
        })
        .map(|server| server.addr)
}

/// CA-side: authenticate the admin, authorize by service-control scope, and
/// apply the op to **one** admin server (`req.target_server`) — forwarding a
/// single peer-cert-gated [`Request::ApplyServiceControl`], or applying locally
/// when the target is the CA itself. Per-server by design: restart is never
/// resolver cluster-wide, so a careful operator restarts one resolver at a time.
pub(super) async fn handle_control_service(
    state: &Arc<Server>,
    req: &ControlServiceRequest,
    authentication: &PreparedAdminAuthentication,
) -> ControlServiceResponse {
    let err = |reason: String| ControlServiceResponse::Err { reason };
    if !state.has_ca().await {
        return err("a service-control request must be sent to the CA host".to_string());
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
    // The target server's resolver cluster base is its authorization scope. A server not
    // in the map (or running no resolver) has no base — only a signing slot may
    // control it, so an unknown target can't be reached by a scoped role admin.
    let target_server = req.target_server;
    let base = state.read(move |state| base_for_server(&state.map, target_server)).await;
    let authorized = match &base {
        Some(base) => service_control_authority(&authd, base),
        None => signing_slot(&authd),
    };
    if !authorized {
        return err(format!(
            "admin {:?} is not authorized to control services on {}",
            authd.admin, req.target_server
        ));
    }
    // start/stop/restart must name explicit units, so a fat-finger can't take a
    // server down blind; read-only `status` may omit them (the supervisor
    // expands an empty unit list to all its units).
    if req.units.is_empty()
        && !matches!(req.op, netidx_activation::control::ControlOp::Status)
    {
        return err("no units specified — start/stop/restart require explicit units \
             (status with no units reports them all)"
            .to_string());
    }
    let operation_id = admin_proto::OperationId::new();
    // Audit the *intent* before acting — a slow op can outlive the connection.
    let ca_dir = state.ca_dir().await.expect("CA role held");
    audit(
        &ca_dir,
        &authd.admin,
        "control-service",
        &format!(
            "operation {operation_id}: {:?} {} on {}",
            req.op,
            req.units.join(","),
            req.target_server
        ),
        Duration::ZERO,
    )
    .await;
    let (my_id, target_addr) = state
        .read(move |state| {
            (state.cfg.server_id, registered_server_addr(&state.map, target_server))
        })
        .await;
    let Some(target_addr) = target_addr else {
        return err(
            "target server is not registered in the authoritative map".to_string()
        );
    };
    // Apply to the one target: locally when it's this CA host, else one
    // peer-cert-gated hop to that admin server (its real map listen address).
    let applied = if req.target_server == my_id {
        let apply = ApplyServiceControlRequest {
            operation_id,
            units: req.units.clone(),
            op: req.op,
        };
        match handle_apply_service_control(state, &apply).await {
            ApplyServiceControlResponse::Ok(units) => Ok(units),
            ApplyServiceControlResponse::Err { reason } => Err(reason),
        }
    } else {
        let client = match state.outbound_client().await {
            Ok(client) => client,
            Err(e) => return err(format!("loading outbound identity: {e:#}")),
        };
        let target_is_ca = state.read(move |state| target_server == state.map.ca).await;
        tokio::time::timeout(
            PUSH_TIMEOUT,
            transport::push_service_control(
                &client,
                target_addr,
                req.target_server,
                target_is_ca,
                state.home_ca_der.clone(),
                operation_id,
                req.units.clone(),
                req.op,
            ),
        )
        .await
        .map_err(|_| format!("timed out after {}s", PUSH_TIMEOUT.as_secs()))
        .and_then(|result| result.map_err(|e| format!("{e:#}")))
    };
    match applied {
        Ok(units) => ControlServiceResponse::Ok(ControlServiceOk { operation_id, units }),
        Err(reason) => err(format!(
            "operation {operation_id} on server {} at {target_addr}: {reason}",
            req.target_server
        )),
    }
}

/// Server-to-server (peer-cert-gated): apply a service-control op to THIS
/// host's local activation supervisor, via its control socket.
pub(super) async fn handle_apply_service_control(
    state: &Server,
    req: &ApplyServiceControlRequest,
) -> ApplyServiceControlResponse {
    info!(
        "admin-server: applying service-control operation {}: {:?} {:?}",
        req.operation_id, req.op, req.units
    );
    use netidx_activation::control;
    let err = |reason: String| ApplyServiceControlResponse::Err { reason };
    // Use the configured activation unit directory if set (the supervisor may
    // run with a custom `--units` dir), else the default search location —
    // the same resolution the supervisor itself uses to place the socket.
    let units_dir = state
        .read(move |state| state.cfg.activation_units_dir.clone())
        .await
        .or_else(netidx_activation::runtime::default_units_dir);
    let dir = match units_dir {
        Some(dir) => dir,
        None => {
            return err(
                "no activation supervisor on this host (no unit directory found)"
                    .to_string(),
            );
        }
    };
    let creq = control::ControlRequest { op: req.op, units: req.units.clone() };
    let statuses = match control::control(&dir, &creq).await {
        Ok(control::ControlResponse::Ok { units }) => units,
        Ok(control::ControlResponse::Err { reason }) => return err(reason),
        Err(e) => {
            return err(format!("contacting the local activation supervisor: {e:#}"));
        }
    };
    // Merge each reported unit's on-disk definition (this member holds the unit
    // files) so the operator's panel shows the same status + definition the
    // local Services surface does. A read failure just omits definitions.
    let defs = crate::activation::ActivationDir::open(Some(&dir))
        .and_then(|ad| ad.list())
        .unwrap_or_default();
    let units = statuses
        .into_iter()
        .map(|u| {
            let definition = defs.get(&u.unit).map(|unit| ServiceUnitDef {
                exe: unit.process.exe.clone(),
                args: unit.process.args.clone(),
                trigger: unit.trigger.to_string(),
                restart: unit.process.restart.to_string(),
            });
            ServiceUnit { unit: u.unit, state: u.state, definition }
        })
        .collect();
    ApplyServiceControlResponse::Ok(units)
}
