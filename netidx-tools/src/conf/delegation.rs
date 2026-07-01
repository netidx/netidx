//! `netidx conf resolver add-parent` / `review-delegation` — the
//! client/CLI half of resolver hierarchy delegation. The child queues a
//! request with the parent's conf server (glyph-confirming it first) and
//! polls; the parent admin reviews pending requests, matches the
//! out-of-band code, and approves (which edits the parent's `children`
//! cluster-wide) or denies. Mirrors the CA enrollment `ca approve`
//! ceremony.

use anyhow::{Context, Result};
use arcstr::ArcStr;
use clap::Args;
use netidx_admin::{
    conf_client,
    conf_proto::{
        DelegationPollResponse, InfoAuth, NodeKind, PeerResult, ReferralEdit,
        ResolverAddr,
    },
    conf_server, conf_server_config,
    fingerprint::ColorMode,
    paths,
    resolver::ResolverConfig,
    template::{self, ParentRef, ReferralAuth},
};
use std::{net::SocketAddr, time::Duration};
use zeroize::Zeroizing;

use super::{
    ca::{collect_existing_password, env_user_name, fmt_age, local_conf_server_listen},
    init, prompt,
};

/// How often a waiting child checks on its queued delegation request.
const POLL_INTERVAL: Duration = Duration::from_secs(5);

pub(super) fn info_to_referral_auth(a: &InfoAuth) -> ReferralAuth {
    match a {
        InfoAuth::Anonymous => ReferralAuth::Anonymous,
        InfoAuth::Krb5 { spn } => ReferralAuth::Krb5(ArcStr::from(spn.as_str())),
        InfoAuth::Tls { name } => ReferralAuth::Tls(ArcStr::from(name.as_str())),
    }
}

/// The shared client half of delegation, used by both `add-parent` and
/// the install child branch: connect to the parent conf server,
/// glyph-confirm its CA (the one human trust decision), queue a request
/// for `proposed_path` carrying the child cluster's address(es), show the
/// request code, and poll until the parent admin approves (or
/// denies/expires). Returns the parent cluster's resolver address(es) for
/// the child's `parent` referral.
pub(crate) fn delegate_under_parent(
    parent_conf_addr: SocketAddr,
    proposed_path: &str,
    child: Vec<ResolverAddr>,
    confirmed: Option<&conf_client::CaIdentity>,
) -> Result<Vec<ResolverAddr>> {
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    // Reuse an identity the caller already glyph-confirmed (the resolver
    // install probe confirms the parent up front, so don't ask twice);
    // otherwise fetch + confirm here (the standalone `add-parent` path).
    // Either way it is the one human trust decision for this delegation.
    let identity = match confirmed {
        Some(id) => (*id).clone(),
        None => {
            let id = rt
                .block_on(conf_client::fetch_identity(parent_conf_addr, NodeKind::Client))
                .with_context(|| {
                    format!("contacting parent conf server {parent_conf_addr}")
                })?;
            init::show_network_identity(parent_conf_addr, &id);
            if !prompt::confirm(
                "does this match the parent network's admin glyph?",
                false,
            )? {
                bail!("the parent network identity was not confirmed; nothing was sent");
            }
            id
        }
    };
    let request_id = rt.block_on(conf_client::request_delegation(
        parent_conf_addr,
        proposed_path,
        child.clone(),
        &identity,
    ))?;
    let code = conf_client::delegation_code(proposed_path, &child);
    println!("delegation requested for {proposed_path:?}. Your request code is:");
    println!("  SHA256  {}", code.text());
    println!("{}", code.identicon(ColorMode::detect()));
    println!(
        "send this code to the parent admin (chat, phone — any channel you \
         trust); they approve with `netidx conf resolver review-delegation` after \
         matching it. Waiting for approval (Ctrl-C to abort; the request expires \
         on its own)..."
    );
    loop {
        std::thread::sleep(POLL_INTERVAL);
        match rt.block_on(conf_client::poll_delegation(
            parent_conf_addr,
            &request_id,
            &identity,
        ))? {
            DelegationPollResponse::Pending => continue,
            DelegationPollResponse::Approved { parent } => break Ok(parent),
            DelegationPollResponse::Denied { reason } => {
                bail!("the parent admin denied the delegation: {reason}")
            }
            DelegationPollResponse::Unknown => bail!(
                "the delegation request expired before approval; re-run to try again"
            ),
        }
    }
}

#[derive(Args, Debug)]
pub(crate) struct AddParentFlags {
    /// The parent's conf-server address: a hostname or IP, with or
    /// without a `:port` (the conf port defaults to 4565). Prompted when
    /// omitted — over a WAN you type it (no mDNS).
    #[arg(long = "server")]
    server: Option<String>,
    /// The subtree this resolver will own under the parent (e.g. `/eu`).
    /// Prompted when omitted.
    #[arg(long = "path")]
    path: Option<String>,
}

/// `resolver add-parent` — attach an existing standalone resolver under a
/// parent by delegation, then write its `parent` referral.
pub(crate) fn add_parent(f: AddParentFlags) -> Result<()> {
    let rpath = paths::discover_resolver_config().context(
        "no resolver config found — `add-parent` operates on an installed resolver",
    )?;
    let rcfg = ResolverConfig::load(&rpath)?;
    if rcfg.as_file().parent.is_some() {
        bail!(
            "this resolver already has a parent referral — re-parenting isn't \
             supported (uninstall + reinstall to switch networks)."
        );
    }
    let child = rcfg.resolver_addrs();
    if child.is_empty() {
        bail!(
            "this resolver advertises no network address (Local-only?) — it cannot \
             be delegated a subtree."
        );
    }
    let n_members = child.len();
    let server = match f.server {
        Some(s) => init::resolve_conf_server_addr(&s)?,
        None => prompt::required_with(
            "parent conf-server address (host or ip, optional :port, e.g. \
             203.0.113.1:4565)",
            init::resolve_conf_server_addr,
        )?,
    };
    let proposed_path = prompt::required_string(
        "the subtree this resolver will own under the parent (e.g. /eu)",
        f.path,
    )?;
    // Standalone add-parent: no prior confirm, so delegate_under_parent does
    // the fetch + glyph-confirm itself.
    let parent = delegate_under_parent(server, &proposed_path, child, None)?;
    let parent_ref = ParentRef {
        path: ArcStr::from(proposed_path.as_str()),
        ttl: None,
        addrs: parent.iter().map(|r| (r.addr, info_to_referral_auth(&r.auth))).collect(),
    };
    let rt = template::set_parent_referral(&rpath, parent_ref)?;
    println!("{}", rt.describe());
    rt.apply().context("writing the parent referral")?;
    // When the child is itself a cluster, the parent referral just written
    // locally must reach every other member or those members won't refer up
    // to the parent. Propagate it cluster-wide via the same ApplyReferralEdit
    // push the parent side uses for AddChild.
    if n_members > 1 {
        propagate_parent_to_child_cluster(&rcfg, &proposed_path, &parent)?;
    }
    println!("ok — restart your resolver server(s) to attach under {proposed_path:?}");
    Ok(())
}

/// Push the child's freshly-written `parent` referral to every other member
/// of the child cluster (symmetric to the parent-side `AddChild` push). This
/// host's conf server supplies the peer-cert identity needed to authenticate
/// to the other members' conf servers. Without a local conf server we can't
/// authenticate as a cluster peer, so we fall back to a loud "copy it
/// manually" warning.
fn propagate_parent_to_child_cluster(
    rcfg: &ResolverConfig,
    proposed_path: &str,
    parent: &[ResolverAddr],
) -> Result<()> {
    let conf_path = match paths::discover_conf_server_config() {
        Ok(p) => p,
        Err(_) => {
            let n = rcfg.as_file().member_servers.len();
            eprintln!(
                "WARNING: this is a {n}-member cluster but this host has no conf \
                 server, so the parent referral could not be propagated \
                 automatically. Copy the `parent` block from this host's \
                 resolver.json into every other member's resolver.json, or they \
                 won't refer up to the parent."
            );
            return Ok(());
        }
    };
    let cfg = conf_server_config::ConfServerConfig::load(&conf_path).context(
        "loading this host's conf-server config to propagate the parent referral",
    )?;
    let cert = std::fs::read(&cfg.serving_cert).with_context(|| {
        format!("reading serving cert {}", cfg.serving_cert.display())
    })?;
    let key = std::fs::read(&cfg.serving_key)
        .with_context(|| format!("reading serving key {}", cfg.serving_key.display()))?;
    let trusted = std::fs::read(&cfg.trusted)
        .with_context(|| format!("reading trust bundle {}", cfg.trusted.display()))?;
    let roots = conf_server::load_roots(&trusted)?;
    let member_addrs: Vec<SocketAddr> =
        rcfg.as_file().member_servers.iter().map(|m| m.addr).collect();
    let edit = ReferralEdit::SetParent {
        path: proposed_path.to_string(),
        parent: parent.to_vec(),
    };
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    println!("propagating the parent referral to the other cluster member(s)...");
    sync_cluster_peers("parent referral", || {
        Ok(rt.block_on(conf_server::push_referral_edit_to_peers(
            &edit,
            &member_addrs,
            cfg.listen,
            &cert,
            &key,
            roots.clone(),
        )))
    })
}

/// Loudly report a cluster push and offer an in-session, idempotent retry
/// until every peer is consistent or the operator gives up. `attempt`
/// performs one push (approve+push, or a bare `SetParent` push) and returns
/// the per-peer results; `subject` names what's being synced. Shared by the
/// parent-side approve loop and the child-side parent-referral push — both
/// are idempotent, so retrying after a peer recovers converges the cluster.
fn sync_cluster_peers(
    subject: &str,
    mut attempt: impl FnMut() -> Result<Vec<PeerResult>>,
) -> Result<()> {
    loop {
        let peers = match attempt() {
            Ok(peers) => peers,
            Err(e) => {
                println!("{subject}: push failed: {e:#}");
                return Ok(());
            }
        };
        let failed: Vec<_> = peers.iter().filter(|p| p.error.is_some()).collect();
        if failed.is_empty() {
            println!("{subject}: {} cluster peer(s) updated.", peers.len());
            return Ok(());
        }
        println!(
            "{subject}: {} of {} cluster peer(s) could NOT be updated:",
            failed.len(),
            peers.len()
        );
        for p in &failed {
            println!("  ! {} : {}", p.addr, p.error.as_deref().unwrap_or("?"));
        }
        println!(
            "  the cluster is INCONSISTENT until every peer is updated. The push \
             is idempotent — once the failed peer(s) are back, retry safely to \
             converge."
        );
        if !prompt::confirm("retry the push to the failed peer(s) now?", true)? {
            println!(
                "  left INCONSISTENT — retry from here once the peer(s) are back \
                 (closing this command gives up the only in-session re-sync handle)."
            );
            return Ok(());
        }
    }
}

#[derive(Args, Debug)]
pub(crate) struct ReviewFlags {
    /// The conf server whose delegation queue to work: a hostname or IP,
    /// with or without a `:port` (the conf port defaults to 4565).
    /// Defaults to this host's own conf server.
    #[arg(long = "server")]
    server: Option<String>,
}

/// `resolver review-delegation` — the parent admin reviews pending
/// delegation requests, matches the out-of-band code, and approves
/// (cluster-wide) or denies.
pub(crate) fn review_delegation(f: ReviewFlags) -> Result<()> {
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    let server = match f.server {
        Some(s) => init::resolve_conf_server_addr(&s)?,
        None => local_conf_server_listen()
            .context("no conf server found; pass --server <host-or-ip[:port]>")?,
    };
    let identity = rt
        .block_on(conf_client::fetch_identity(server, NodeKind::Client))
        .with_context(|| format!("contacting conf server {server}"))?;
    init::show_network_identity(server, &identity);
    if !prompt::confirm("is this your network's conf server?", false)? {
        bail!("conf-server identity was not confirmed; nothing was sent");
    }
    let admin = match env_user_name() {
        Some(user) => prompt::string_with_default("admin name", None, &user)?,
        None => prompt::required_string("admin name", None)?,
    };
    let password = Zeroizing::new(collect_existing_password(&format!(
        "CA password for admin {admin:?}"
    ))?);
    loop {
        let queue = rt.block_on(conf_client::list_delegations(
            server,
            &admin,
            password.as_str(),
            &identity,
        ))?;
        if queue.is_empty() {
            println!("the delegation queue is empty");
            return Ok(());
        }
        println!();
        println!("pending delegation requests:");
        for (i, e) in queue.iter().enumerate() {
            let code = conf_client::delegation_code(&e.proposed_path, &e.child).short();
            println!(
                "  {}) delegate {:?} to {}  code {}  age {}  from {}",
                i + 1,
                e.proposed_path,
                describe_child(&e.child),
                code,
                fmt_age(e.age_secs),
                e.peer,
            );
        }
        let answer =
            prompt::required_string("request # to review (or 'q' to quit)", None)?;
        if answer.eq_ignore_ascii_case("q") {
            return Ok(());
        }
        let entry = match answer.parse::<usize>() {
            Ok(n) if (1..=queue.len()).contains(&n) => &queue[n - 1],
            _ => {
                eprintln!("enter a number between 1 and {}, or 'q'", queue.len());
                continue;
            }
        };
        let code = conf_client::delegation_code(&entry.proposed_path, &entry.child);
        println!();
        println!("  delegate subtree: {}", entry.proposed_path);
        println!("  to child resolver(s): {}", describe_child(&entry.child));
        println!("  from: {}", entry.peer);
        println!("  request code:");
        println!("  SHA256  {}", code.text());
        println!("{}", code.identicon(ColorMode::detect()));
        if !prompt::confirm("does this code match what the child admin sent you?", false)?
        {
            if prompt::confirm("deny this request?", true)? {
                let reason = prompt::string_with_default(
                    "denial reason (shown to the child admin)",
                    None,
                    "request code mismatch",
                )?;
                rt.block_on(conf_client::deny_delegation(
                    server,
                    &admin,
                    password.as_str(),
                    &entry.id,
                    &reason,
                    &identity,
                ))?;
                println!("denied.");
            }
            continue;
        }
        // Approve, then re-push to any unreachable peer in-session. The
        // approval commits (and leaves the pending queue) on the first call,
        // so it can't be re-selected from a later `review-delegation`; but
        // ApproveDelegation is idempotent for an already-approved request
        // (the server re-applies + re-pushes without re-committing), so
        // sync_cluster_peers keeps offering to retry here until every peer is
        // consistent. This is the only path to the idempotent re-sync.
        sync_cluster_peers("approval", || {
            rt.block_on(conf_client::approve_delegation(
                server,
                &admin,
                password.as_str(),
                &entry.id,
                &identity,
            ))
        })?;
        println!("  restart your resolver server(s) to serve the new child.");
    }
}

fn describe_child(child: &[ResolverAddr]) -> String {
    child.iter().map(|r| r.addr.to_string()).collect::<Vec<_>>().join(", ")
}
