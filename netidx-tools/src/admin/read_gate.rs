//! `netidx admin resolver read-gate` — take one member in or out of service
//! without stopping it.
//!
//! Remote-only and unix-only, like every other admin-plane operation: the
//! session needs the CA module. On the box itself the gate is just a field in
//! `resolver.json`, which the resolver applies live.

use super::answer_cli::RemoteAuthFlags;
use anyhow::{Context, Result};
use chrono::Utc;
use clap::{ArgGroup, Args};
use netidx::resolver_server::config::ReadGate;
use netidx_admin::ops;
use netidx_admin_proto::AdminServerId;

#[derive(Args, Debug)]
#[command(group(ArgGroup::new("gate").required(true).args(["shut", "open", "until"])))]
pub(crate) struct ReadGateFlags {
    /// Immutable UUID of the ONE member to gate. `netidx admin ca servers`
    /// lists them, with each one's current gate.
    #[arg(long, value_name = "SERVER-ID")]
    pub target: AdminServerId,
    /// Stop answering read clients until someone opens the gate again.
    /// Subscribers stop resolving through it; publishers keep writing to it,
    /// so its records stay fresh — it just stops answering.
    #[arg(long)]
    pub shut: bool,
    /// Answer read clients. Use once you are satisfied that every publisher
    /// has found a newly added member — until they have, it answers from a
    /// namespace they have not finished rebuilding.
    #[arg(long)]
    pub open: bool,
    /// Stop answering read clients for this long, then start. Takes a
    /// duration such as `30m` or `2h`.
    #[arg(long, value_name = "DURATION")]
    pub until: Option<humantime::Duration>,
    #[command(flatten)]
    pub auth: RemoteAuthFlags,
}

pub(crate) fn read_gate(f: ReadGateFlags) -> Result<()> {
    let server = f
        .auth
        .server_addr()?
        .context("setting a read gate requires --server <ADMIN-SERVER>")?;
    let gate = if f.shut {
        ReadGate::Yes
    } else if f.open {
        ReadGate::No
    } else {
        let until = f.until.expect("clap requires one of shut/open/until");
        ReadGate::Until(
            Utc::now()
                + chrono::Duration::from_std(*until)
                    .context("the duration is too long")?,
        )
    };
    let mut ans = f.auth.answerer()?;
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    rt.block_on(ops::service::set_read_gate(
        &mut ans,
        server,
        f.auth.ca_dir.clone(),
        f.auth.admin.clone(),
        None,
        f.target,
        gate,
    ))?;
    match gate {
        ReadGate::No => println!("{} is now answering read clients", f.target),
        ReadGate::Yes => println!("{} is no longer answering read clients", f.target),
        ReadGate::Until(t) => {
            println!("{} will start answering read clients at {t}", f.target)
        }
    }
    Ok(())
}
