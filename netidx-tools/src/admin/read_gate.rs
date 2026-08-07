//! `netidx admin resolver read-gate` — take one member in or out of service
//! without stopping it.
//!
//! Remote-only and unix-only, like every other admin-plane operation: the
//! session needs the CA module. On the box itself the gate is just a field in
//! `resolver.json`, which the resolver applies live.

use super::answer_cli::RemoteAuthFlags;
use anyhow::{Context, Result};
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
    // No `--server` means this host's own admin server, as it does everywhere
    // else — `set_read_gate` resolves it. The interlock on an operation that
    // takes a member out of service is `--target`, an immutable UUID the
    // operator has to look up; making them also type the address of the
    // machine they are standing on adds ceremony, not safety.
    let server = f.auth.server_addr()?;
    let gate = if f.shut {
        ReadGate::Yes
    } else if f.open {
        ReadGate::No
    } else {
        let until = f.until.expect("clap requires one of shut/open/until");
        ops::servers::read_gate_for(*until)?
    };
    let mut ans = f.auth.answerer()?;
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    // The risk is stated by `set_read_gate`, which has the member's current
    // gate. The strict CLI has no confirm ceremony, so it arrives as a warning
    // rather than a gate — the same rule the TUI stops on.
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
