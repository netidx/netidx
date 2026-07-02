//! Remote service control over the admin plane.
//!
//! `netidx admin activation {restart,start,stop,status} --server …` controls the
//! activation units of the cluster serving a netidx path, RBAC-gated by the
//! caller's `service_control_scopes` covering that path. Only this pinned,
//! admin-authenticated **remote** path lives here — the **local** path (this
//! host's own supervisor, over its cross-platform control socket in
//! `netidx-activation`) needs no CA and works on Windows, so it stays in the
//! tools `activation` module rather than this unix-only group.

use super::open_admin_session;
use crate::{
    admin_client,
    admin_proto::{NodeKind, Secret, ServiceControlResult, UnitTarget},
    answer::Answerer,
};
use anyhow::{Context, Result};
use netidx_activation::control::ControlOp;
use std::{net::SocketAddr, path::PathBuf};

/// Parse `unit[:member]` tokens into [`UnitTarget`]s. A trailing `:<n>` pins the
/// unit to cluster member `n` (so an admin can stagger a rolling restart);
/// otherwise it targets every member.
pub fn parse_unit_targets(toks: &[String]) -> Result<Vec<UnitTarget>> {
    toks.iter()
        .map(|t| match t.rsplit_once(':') {
            Some((unit, idx)) => {
                let member = idx
                    .parse::<u32>()
                    .with_context(|| format!("invalid member index in {t:?}"))?;
                Ok(UnitTarget { unit: unit.to_string(), member: Some(member) })
            }
            None => Ok(UnitTarget { unit: t.clone(), member: None }),
        })
        .collect()
}

/// Remote service control over the admin plane: glyph-confirm + authenticate,
/// then apply `op` to `targets` on the cluster serving `path`. The CA enforces
/// the caller's `service_control_scopes` covering `path`. Returns one result per
/// targeted cluster member, so a partial failure surfaces per member.
pub async fn control_remote(
    ans: &mut dyn Answerer,
    server: SocketAddr,
    ca_dir: Option<PathBuf>,
    admin: Option<String>,
    password: Option<Secret>,
    path: &str,
    targets: Vec<UnitTarget>,
    op: ControlOp,
) -> Result<Vec<ServiceControlResult>> {
    let sess = open_admin_session(ans, Some(server), ca_dir, admin, password).await?;
    admin_client::control_service(
        sess.server,
        NodeKind::Client,
        &sess.identity,
        &sess.admin,
        sess.password.as_str(),
        path,
        targets,
        op,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_target_parsing() {
        let t = parse_unit_targets(&[
            "resolver".to_string(),
            "resolver:0".to_string(),
            "id-map:12".to_string(),
        ])
        .unwrap();
        assert_eq!(t[0].unit, "resolver");
        assert_eq!(t[0].member, None);
        assert_eq!(t[1].unit, "resolver");
        assert_eq!(t[1].member, Some(0));
        assert_eq!(t[2].unit, "id-map");
        assert_eq!(t[2].member, Some(12));
        // A non-numeric index is a clear error, not a silent unit name.
        assert!(parse_unit_targets(&["resolver:abc".to_string()]).is_err());
    }
}
