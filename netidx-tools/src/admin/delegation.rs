//! `netidx admin resolver {add-parent, list-delegations, approve-delegation,
//! deny-delegation}` — thin CLI adapters over
//! [`netidx_admin_client::ops::delegation`]. The parent-admin review is a
//! query + action (code-as-id) split: `list-delegations` shows each pending
//! request by its code; `approve-delegation <code>` / `deny-delegation <code>`
//! act on the one whose recomputed code matches.

use anyhow::{Context, Result};
use clap::Args;
use netidx_admin_client::{
    config_lock::ConfigDirLock,
    ops::delegation::{self as ops, AddParentCompletion, ResolverClusterPropagation},
    paths,
    plan::delegation::DelegationSelection,
};
use netidx_admin_proto::{PeerResult, ResolverAddr};

use super::{answer_cli::RemoteAuthFlags, ca::fmt_age, init};

fn runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Runtime::new().context("starting tokio runtime")
}

fn describe_child(child: &[ResolverAddr]) -> String {
    child.iter().map(|r| r.addr.to_string()).collect::<Vec<_>>().join(", ")
}

fn describe_ids(ids: &[netidx_admin_proto::AdminServerId]) -> String {
    ids.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
}

/// Report a cluster-propagation push (idempotent — re-run to converge if a peer
/// was down).
fn report_peers(subject: &str, peers: &[PeerResult]) {
    let failed: Vec<_> = peers.iter().filter(|p| p.error.is_some()).collect();
    if failed.is_empty() {
        println!("{subject}: {} cluster peer(s) updated.", peers.len());
        return;
    }
    println!(
        "{subject}: {} of {} cluster peer(s) could NOT be updated:",
        failed.len(),
        peers.len()
    );
    for p in &failed {
        println!(
            "  ! server {} at {} : {}",
            p.server,
            p.addr,
            p.error.as_deref().unwrap_or("?")
        );
    }
    println!(
        "  the cluster is INCONSISTENT until every peer is updated. The push is \
         idempotent — re-run this command once the failed peer(s) are back."
    );
}

#[derive(Args, Debug)]
pub(crate) struct AddParentFlags {
    /// The parent's admin-server address: a hostname or IP, with or without a
    /// `:port` (the admin port defaults to 4565).
    #[arg(long = "server")]
    server: String,
    /// The subtree this resolver will own under the parent (e.g. `/eu`).
    #[arg(long = "path")]
    path: String,
    /// The parent network's CA fingerprint, obtained out of band (view it with
    /// `netidx admin ca fingerprint <ip:port>`).
    #[arg(long = "accept-glyph")]
    accept_glyph: Option<String>,
    /// A resolver address that should remain in the parent cluster. Repeat for
    /// every parent member when splitting an existing peer cluster. Omit only
    /// for install-time attachment of an already-enrolled pending child.
    #[arg(long = "parent-resolver", value_name = "ADDR")]
    parent_resolver: Vec<std::net::SocketAddr>,
}

/// `resolver add-parent` — attach this standalone resolver under a parent by
/// delegation, then write (and propagate) its `parent` referral.
pub(crate) fn add_parent(f: AddParentFlags) -> Result<()> {
    let rpath = paths::discover_resolver_config().context(
        "no resolver config found — `add-parent` operates on an installed resolver",
    )?;
    let server = init::resolve_admin_server_addr(&f.server)?;
    let mut ans =
        super::answer_cli::make_flag_answerer(None, false, f.accept_glyph.as_deref())?;
    let selection = (!f.parent_resolver.is_empty())
        .then_some(DelegationSelection { parent_resolvers: f.parent_resolver });
    let out = runtime()?.block_on(async {
        match ops::prepare_add_parent(&mut ans, &rpath, server, &f.path, selection)
            .await?
        {
            AddParentCompletion::Complete(outcome) => Ok(outcome),
            AddParentCompletion::LocalWrite(pending) => {
                let lock = ConfigDirLock::acquire_for_file_async(&rpath).await?;
                pending.apply(&lock, &mut ans)
            }
        }
    })?;
    match out.propagation {
        ResolverClusterPropagation::ControllerManaged => {}
    }
    println!(
        "ok — configuration for {:?} is written; no service was restarted",
        out.proposed_path
    );
    println!(
        "roll the affected cluster one member at a time: restart one member, wait the resolver delay-reads period for publishers to republish, then restart the next"
    );
    Ok(())
}

#[derive(Args, Debug)]
pub(crate) struct ListDelegationFlags {
    #[command(flatten)]
    auth: RemoteAuthFlags,
}

/// `resolver list-delegations` — pending and approved delegation requests,
/// keyed by code. Approved entries can be re-approved to reconcile fanout.
pub(crate) fn list_delegations(f: ListDelegationFlags) -> Result<()> {
    let mut ans = f.auth.answerer()?;
    let server = f.auth.server_addr()?;
    let items = runtime()?.block_on(ops::list_pending_delegations(
        &mut ans,
        server,
        f.auth.ca_dir.clone(),
        f.auth.admin.clone(),
        None,
    ))?;
    if items.is_empty() {
        println!("there are no reviewable delegation requests");
        return Ok(());
    }
    println!("delegation requests:");
    for e in &items {
        println!(
            "  [{}] delegate {:?}  (age {}, from {})",
            if e.approved { "approved; reconcile with approve" } else { "pending" },
            e.proposed_path,
            fmt_age(e.age_secs),
            e.peer,
        );
        println!(
            "    parent {} [{}]: {}",
            e.parent_base,
            e.parent_cluster,
            describe_child(&e.parent)
        );
        println!("      server IDs: {}", describe_ids(&e.parent_servers));
        println!(
            "    child  {} [{}]: {}",
            e.child_base,
            e.child_cluster,
            describe_child(&e.child)
        );
        println!("      server IDs: {}", describe_ids(&e.child_servers));
        println!("    code {}", e.code.text());
    }
    println!(
        "\napprove with `netidx admin resolver approve-delegation <code>` after \
         matching the code out of band."
    );
    Ok(())
}

#[derive(Args, Debug)]
pub(crate) struct ApproveDelegationFlags {
    /// The request code to approve (as shown by `list-delegations`; the groups
    /// may be passed space-separated without quoting). Selects and asserts the
    /// request — a mismatch or ambiguous prefix is refused.
    #[arg(value_name = "CODE", num_args = 1..)]
    code: Vec<String>,
    #[command(flatten)]
    auth: RemoteAuthFlags,
}

/// `resolver approve-delegation <code>` — approve the one pending request whose
/// recomputed code matches, cluster-wide.
pub(crate) fn approve_delegation(f: ApproveDelegationFlags) -> Result<()> {
    let mut ans = f.auth.answerer()?;
    let server = f.auth.server_addr()?;
    let code = f.code.join(" ");
    let d = runtime()?.block_on(ops::approve_delegation(
        &mut ans,
        server,
        f.auth.ca_dir.clone(),
        f.auth.admin.clone(),
        None,
        &code,
    ))?;
    println!("approved delegation of {:?}.", d.proposed_path);
    report_peers("approval", &d.peers);
    println!("no resolver service was restarted.");
    println!(
        "roll each affected cluster one member at a time: restart one member, wait the resolver delay-reads period for publishers to republish, then restart the next"
    );
    Ok(())
}

#[derive(Args, Debug)]
pub(crate) struct DenyDelegationFlags {
    /// The request code to deny (as shown by `list-delegations`).
    #[arg(value_name = "CODE", num_args = 1..)]
    code: Vec<String>,
    /// The reason shown to the child admin.
    #[arg(long = "reason")]
    reason: String,
    #[command(flatten)]
    auth: RemoteAuthFlags,
}

/// `resolver deny-delegation <code> --reason <text>`.
pub(crate) fn deny_delegation(f: DenyDelegationFlags) -> Result<()> {
    let mut ans = f.auth.answerer()?;
    let server = f.auth.server_addr()?;
    let code = f.code.join(" ");
    let d = runtime()?.block_on(ops::deny_delegation(
        &mut ans,
        server,
        f.auth.ca_dir.clone(),
        f.auth.admin.clone(),
        None,
        &code,
        &f.reason,
    ))?;
    println!("denied delegation of {:?}.", d.proposed_path);
    Ok(())
}
