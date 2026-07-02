//! The child/client half of resolver-hierarchy delegation: connect to a
//! parent admin server, glyph-confirm it (the one human trust decision), queue
//! a delegation request for a subtree, and poll until the parent admin
//! approves. Shared by the resolver install's `--parent-admin-server` branch
//! and the standalone `resolver add-parent` command. Driven through the
//! [`Answerer`] seam.

use crate::{
    admin_client::{self, CaIdentity},
    admin_proto::{DelegationPollResponse, InfoAuth, NodeKind, ResolverAddr},
    answer::{Answerer, Progress, Stage},
    template::ReferralAuth,
};
use anyhow::{Context, Result, bail};
use arcstr::ArcStr;
use std::{net::SocketAddr, time::Duration};

/// How often a waiting child checks on its queued delegation request.
const POLL_INTERVAL: Duration = Duration::from_secs(5);

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
/// don't ask twice), queue a request for `proposed_path` carrying the child
/// cluster's address(es), show the request code, and poll until the parent
/// admin approves (or denies / expires). Returns the parent cluster's resolver
/// address(es) for the child's `parent` referral.
pub async fn delegate_under_parent(
    ans: &mut dyn Answerer,
    parent_conf_addr: SocketAddr,
    proposed_path: &str,
    child: Vec<ResolverAddr>,
    confirmed: Option<&CaIdentity>,
) -> Result<Vec<ResolverAddr>> {
    // Reuse an already-confirmed identity (install probe), else fetch +
    // glyph-confirm here (the standalone `add-parent` path). Either way it is
    // the one human trust decision for this delegation.
    let identity = match confirmed {
        Some(id) => id.clone(),
        None => {
            let id = admin_client::fetch_identity(parent_conf_addr, NodeKind::Client)
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
    let request_id = admin_client::request_delegation(
        parent_conf_addr,
        proposed_path,
        child.clone(),
        &identity,
    )
    .await?;
    let code = admin_client::delegation_code(proposed_path, &child);
    ans.show_verification_code("delegation request", &code);
    ans.progress(Progress::new(
        Stage::WaitingApproval,
        "waiting for the parent admin to approve this delegation…",
    ));
    loop {
        tokio::time::sleep(POLL_INTERVAL).await;
        match admin_client::poll_delegation(parent_conf_addr, &request_id, &identity).await? {
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
