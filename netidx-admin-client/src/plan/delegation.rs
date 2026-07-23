//! The child/client half of resolver-hierarchy delegation: connect to a
//! parent admin server, glyph-confirm it (the one human trust decision), queue
//! a delegation request for a subtree, and poll until the parent admin
//! approves. Shared by the resolver install's `--parent-admin-server` branch
//! and the standalone `resolver add-parent` command. Driven through the
//! [`Answerer`] seam.

use crate::{
    admin_proto::{
        AdminServerId, DelegationPollResponse, InfoAuth, NodeKind, ResolverAddr, Role,
    },
    answer::{Answerer, Progress, Stage},
    template::ReferralAuth,
    transport::{self, CaIdentity},
};
use anyhow::{Context, Result, anyhow, bail};
use arcstr::ArcStr;
use compact_str::format_compact;
use std::{collections::BTreeSet, net::SocketAddr, time::Duration};

/// How often a waiting child checks on its queued delegation request.
const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Existing resolver members selected to remain on the parent side of a
/// split. The addresses are only UI input; immutable server IDs are resolved
/// from a controller-verified map before the request is created.
#[derive(Debug, Clone)]
pub struct DelegationSelection {
    pub parent_resolvers: Vec<SocketAddr>,
}

#[derive(Debug)]
struct DelegationServers {
    pub parent: Vec<AdminServerId>,
    pub child: Vec<AdminServerId>,
}

fn selected_server_sets(
    map: &netidx_admin_proto::NetworkMap,
    local_member: &ResolverAddr,
    selected: &DelegationSelection,
) -> Result<DelegationServers> {
    if selected.parent_resolvers.is_empty() {
        bail!("select at least one resolver server for the parent cluster");
    }
    let selected_addrs: BTreeSet<_> = selected.parent_resolvers.iter().copied().collect();
    if selected_addrs.len() != selected.parent_resolvers.len() {
        bail!("a parent resolver was selected more than once");
    }
    let mut parent = Vec::with_capacity(selected_addrs.len());
    let mut parent_cluster = None;
    for addr in selected_addrs {
        let server = map
            .servers
            .iter()
            .find(|server| {
                server.resolver.as_ref().is_some_and(|resolver| resolver.addr == addr)
            })
            .with_context(|| {
                format!("selected parent resolver {addr} is not CA-owned")
            })?;
        if server.state != netidx_admin_proto::ServerState::Registered
            || !server.roles.contains(Role::Resolver)
        {
            bail!("selected parent resolver {addr} is not a registered routing target");
        }
        let cluster =
            server.cluster.context("selected parent resolver has no cluster")?;
        match parent_cluster {
            None => parent_cluster = Some(cluster),
            Some(expected) if expected == cluster => {}
            Some(_) => bail!("selected parent resolvers do not belong to one cluster"),
        }
        parent.push(server.id);
    }
    let parent_cluster = parent_cluster.expect("nonempty selection");
    let local = map
        .servers
        .iter()
        .find(|server| server.resolver.as_ref() == Some(local_member))
        .context("this admin server's local resolver member is not in the CA map")?;
    let local_cluster =
        local.cluster.context("the local resolver has no CA-owned cluster")?;
    let mut child: Vec<_> = map
        .servers
        .iter()
        .filter(|server| {
            server.cluster == Some(local_cluster)
                && server.roles.contains(Role::Resolver)
                && (parent_cluster != local_cluster || !parent.contains(&server.id))
        })
        .map(|server| server.id)
        .collect();
    if !child.contains(&local.id) {
        bail!("the local resolver must remain in the delegated child cluster");
    }
    if parent_cluster != local_cluster {
        let mut complete_parent: Vec<_> = map
            .servers
            .iter()
            .filter(|server| {
                server.cluster == Some(parent_cluster)
                    && server.roles.contains(Role::Resolver)
            })
            .map(|server| server.id)
            .collect();
        complete_parent.sort();
        parent.sort();
        if parent != complete_parent {
            bail!(
                "attaching an existing child cluster requires selecting every resolver in the parent cluster"
            );
        }
    }
    parent.sort();
    child.sort();
    Ok(DelegationServers { parent, child })
}

/// Map a parent resolver's advertised [`InfoAuth`] into the [`ReferralAuth`]
/// the child bakes into its `parent` referral.
pub fn info_to_referral_auth(a: &InfoAuth) -> ReferralAuth {
    match a {
        InfoAuth::Anonymous => ReferralAuth::Anonymous,
        InfoAuth::Krb5 { spn } => ReferralAuth::Krb5(ArcStr::from(spn.as_str())),
        InfoAuth::Tls { name } => ReferralAuth::Tls(ArcStr::from(name.as_str())),
    }
}

/// Delegate this resolver under a parent: connect to the parent admin server,
/// glyph-confirm its CA (unless `confirmed` carries an identity the caller
/// already confirmed — the install probe confirms the parent up front, so we
/// don't ask twice), resolve the selected resolver addresses to stable IDs in
/// the controller map, queue the server-set proposal, show its request code,
/// and poll until the parent admin approves (or denies / expires). Returns the parent cluster's resolver
/// address(es) for the child's `parent` referral.
pub async fn delegate_under_parent(
    ans: &mut dyn Answerer,
    parent_conf_addr: SocketAddr,
    proposed_path: &str,
    child: &[ResolverAddr],
    selected: Option<DelegationSelection>,
    confirmed: Option<&CaIdentity>,
) -> Result<Vec<ResolverAddr>> {
    // Reuse an already-confirmed identity (install probe), else fetch +
    // glyph-confirm here (the standalone `add-parent` path). Either way it is
    // the one human trust decision for this delegation.
    let identity = match confirmed {
        Some(id) => id.clone(),
        None => {
            let id = transport::fetch_identity(parent_conf_addr, NodeKind::Client)
                .await
                .with_context(|| {
                    format!("contacting parent admin server {parent_conf_addr}")
                })?;
            if !ans.confirm_identity(&id).await? {
                bail!("the parent network identity was not confirmed; nothing was sent");
            }
            id
        }
    };
    let map =
        transport::get_map_pinned(parent_conf_addr, NodeKind::Client, &identity).await?;
    let parent_id = map
        .servers
        .iter()
        .find(|s| s.id == identity.server_id)
        .and_then(|s| s.cluster)
        .context("the selected parent server has no resolver cluster")?;
    let servers = match selected {
        Some(selected) => {
            let local_member =
                child.first().context("the local resolver has no advertisable member")?;
            selected_server_sets(&map, local_member, &selected)?
        }
        None => {
            let child_id = map
                .clusters
                .iter()
                .find(|c| c.base == proposed_path && c.members == child)
                .map(|c| c.id)
                .context(
                    "the child must be enrolled as a pending CA-owned cluster before delegation",
                )?;
            DelegationServers {
                parent: map
                    .servers
                    .iter()
                    .filter(|s| {
                        s.cluster == Some(parent_id)
                            && s.roles.contains(Role::Resolver)
                            && s.state == netidx_admin_proto::ServerState::Registered
                    })
                    .map(|s| s.id)
                    .collect(),
                child: map
                    .servers
                    .iter()
                    .filter(|s| {
                        s.cluster == Some(child_id) && s.roles.contains(Role::Resolver)
                    })
                    .map(|s| s.id)
                    .collect(),
            }
        }
    };
    let request_id = transport::request_delegation(
        parent_conf_addr,
        proposed_path,
        servers.parent.clone(),
        servers.child.clone(),
        &identity,
    )
    .await?;
    let code = transport::delegation_code(proposed_path, &servers.parent, &servers.child);
    ans.show_verification_code("delegation request", &code);
    ans.progress(Progress::new(
        Stage::WaitingApproval,
        "waiting for the parent admin to approve this delegation…",
    ));
    let settled = loop {
        tokio::time::sleep(POLL_INTERVAL).await;
        match transport::poll_delegation(parent_conf_addr, &request_id, &identity).await {
            Ok(DelegationPollResponse::Pending) => continue,
            Ok(DelegationPollResponse::Approved { parent }) => break Ok(parent),
            Ok(DelegationPollResponse::Denied { reason }) => {
                break Err(anyhow!("the parent admin denied the delegation: {reason}"));
            }
            Ok(DelegationPollResponse::Unknown) => {
                break Err(anyhow!(
                    "the delegation request expired before approval; re-run to try again"
                ));
            }
            // A comms failure here is transient: the parent admin server can be
            // restarting or briefly unreachable during the (human-paced) wait
            // for approval. The request is durable server-side, so keep polling
            // — only a definitive protocol response (Approved / Denied /
            // expired) ends the wait. Giving up on the first error would derail
            // an already-approved delegation on a single connection blip, with
            // no config written and no clear way to recover but a manual re-run.
            Err(e) => {
                ans.progress(Progress::new(
                    Stage::WaitingApproval,
                    format_compact!(
                        "parent admin server unreachable ({e:#}); still waiting…"
                    ),
                ));
                continue;
            }
        }
    };
    ans.clear_verification_code();
    settled
}

#[cfg(test)]
mod tests {
    use super::*;
    use netidx_admin_proto::{
        ClusterEntry, ClusterState, NetworkMap, ResolverClusterId, ServerEntry,
        ServerState,
    };

    fn resolver(addr: &str) -> ResolverAddr {
        ResolverAddr { addr: addr.parse().unwrap(), auth: InfoAuth::Anonymous }
    }

    fn server(
        id: AdminServerId,
        admin: &str,
        resolver_addr: &str,
        cluster: ResolverClusterId,
    ) -> ServerEntry {
        ServerEntry {
            id,
            addr: admin.parse().unwrap(),
            roles: Role::Resolver.into(),
            resolver: Some(resolver(resolver_addr)),
            cluster: Some(cluster),
            state: ServerState::Registered,
        }
    }

    #[test]
    fn selected_peers_partition_one_cluster() {
        let us1 = AdminServerId::new();
        let us2 = AdminServerId::new();
        let ap1 = AdminServerId::new();
        let ap2 = AdminServerId::new();
        let root = ResolverClusterId::new();
        let servers = vec![
            server(us1, "10.0.0.1:4565", "10.0.0.1:4564", root),
            server(us2, "10.0.0.2:4565", "10.0.0.2:4564", root),
            server(ap1, "10.0.60.1:4565", "10.0.60.1:4564", root),
            server(ap2, "10.0.60.2:4565", "10.0.60.2:4564", root),
        ];
        let map = NetworkMap {
            version: 1,
            controller: us1,
            clusters: vec![ClusterEntry {
                id: root,
                base: "/".into(),
                state: ClusterState::Active,
                members: servers.iter().filter_map(|s| s.resolver.clone()).collect(),
                parent: None,
                children: vec![],
            }],
            servers,
        };
        let sets = selected_server_sets(
            &map,
            &resolver("10.0.60.1:4564"),
            &DelegationSelection {
                parent_resolvers: vec![
                    "10.0.0.2:4564".parse().unwrap(),
                    "10.0.0.1:4564".parse().unwrap(),
                ],
            },
        )
        .unwrap();
        assert_eq!(sets.parent.into_iter().collect::<BTreeSet<_>>(), [us1, us2].into());
        assert_eq!(sets.child.into_iter().collect::<BTreeSet<_>>(), [ap1, ap2].into());
    }

    #[test]
    fn distinct_parent_cluster_must_be_selected_completely() {
        let us1 = AdminServerId::new();
        let us2 = AdminServerId::new();
        let ap1 = AdminServerId::new();
        let root = ResolverClusterId::new();
        let ap = ResolverClusterId::new();
        let servers = vec![
            server(us1, "10.0.0.1:4565", "10.0.0.1:4564", root),
            server(us2, "10.0.0.2:4565", "10.0.0.2:4564", root),
            server(ap1, "10.0.60.1:4565", "10.0.60.1:4564", ap),
        ];
        let map = NetworkMap {
            version: 1,
            controller: us1,
            clusters: vec![
                ClusterEntry {
                    id: root,
                    base: "/".into(),
                    state: ClusterState::Active,
                    members: vec![resolver("10.0.0.1:4564"), resolver("10.0.0.2:4564")],
                    parent: None,
                    children: vec![],
                },
                ClusterEntry {
                    id: ap,
                    base: "/ap".into(),
                    state: ClusterState::Pending,
                    members: vec![resolver("10.0.60.1:4564")],
                    parent: None,
                    children: vec![],
                },
            ],
            servers,
        };
        let err = selected_server_sets(
            &map,
            &resolver("10.0.60.1:4564"),
            &DelegationSelection {
                parent_resolvers: vec!["10.0.0.1:4564".parse().unwrap()],
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("every resolver in the parent"));
    }
}
