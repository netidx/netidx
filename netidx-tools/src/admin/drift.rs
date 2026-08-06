//! `netidx admin drift` — which servers have caught up with the CA.
//!
//! The counterpart to an edit reporting a version rather than a list of hosts
//! it reached. Nothing is pushed: an edit records a version, and every server
//! converges on it at its next register. So "has it landed everywhere" is a
//! question about right now, not a result the edit could have returned — and
//! asking it again after a poll interval gives a different, better answer.

use anyhow::{Context, Result};
use clap::Args;
use netidx_admin::ops::drift::{Agreement, ServerDrift, drift};

use super::answer_cli::RemoteAuthFlags;

#[derive(Args, Debug)]
pub(crate) struct Flags {
    /// Only print servers that are behind. The exit status is unaffected —
    /// this is a display filter, not a check.
    #[arg(long)]
    behind: bool,
    #[command(flatten)]
    auth: RemoteAuthFlags,
}

pub(crate) fn run(f: Flags) -> Result<()> {
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    let mut ans = f.auth.answerer()?;
    let target = rt.block_on(netidx_admin::ops::resolve_admin_target(
        &mut ans,
        f.auth.server_addr()?,
        f.auth.ca_dir.clone(),
        f.auth.admin.clone(),
        None,
    ))?;
    let rows = rt.block_on(drift(&target))?;
    print(&rows, f.behind);
    Ok(())
}

fn describe(a: Agreement) -> String {
    match a {
        Agreement::Current { version } => format!("v{version}"),
        Agreement::Behind { reported: Some(have), want } => {
            format!("v{have} < v{want} BEHIND")
        }
        Agreement::Behind { reported: None, want } => format!("none < v{want} BEHIND"),
        Agreement::Unrecorded => "(never edited)".to_string(),
        Agreement::Ahead { reported, ca } => format!("v{reported} > v{ca} AHEAD"),
    }
}

fn print(rows: &[ServerDrift], only_behind: bool) {
    let lagging = |r: &ServerDrift| {
        r.config_drift
            || [r.perms, r.id_map, r.config].iter().flatten().any(|a| !a.is_current())
    };
    let shown: Vec<_> = rows.iter().filter(|r| !only_behind || lagging(r)).collect();
    if shown.is_empty() {
        println!(
            "{}",
            if only_behind {
                "every registered server is current."
            } else {
                "no registered servers."
            }
        );
        return;
    }
    println!(
        "{:<38}  {:<22}  {:<22}  {:<22}  {}",
        "SERVER", "ADDRESS", "PERMS", "ID-MAP", "CONFIG"
    );
    for r in shown {
        let cell = |a: Option<Agreement>| a.map(describe).unwrap_or_else(|| "-".into());
        let mut config = cell(r.config);
        if r.config_drift {
            config.push_str(" DRIFT");
        }
        println!(
            "{:<38}  {:<22}  {:<22}  {:<22}  {}",
            r.server.to_string(),
            r.addr.to_string(),
            cell(r.perms),
            cell(r.id_map),
            config,
        );
    }
    // Drift is not lag: the version can be current while the file has been
    // changed underneath it. Saying which it is saves an operator from
    // waiting out a poll for something a poll will not fix.
    if rows.iter().any(|r| r.config_drift) {
        println!(
            "\nDRIFT means the resolver config that server reported does not match the \
             cluster\nthe CA granted it — a hand edit, or a document that would not \
             load. The CA sends\nit a correct one on its next register; if DRIFT \
             persists, look at that host's logs."
        );
    }
    // An "AHEAD" line is not a lag, it is a contradiction — say so rather than
    // leaving an operator to read past it as noise.
    if rows
        .iter()
        .flat_map(|r| [r.perms, r.id_map, r.config])
        .flatten()
        .any(|a| matches!(a, Agreement::Ahead { .. }))
    {
        println!(
            "\nA server reporting a version above the CA's is not lagging — it holds \
             state the CA does not.\nUsual causes: a restored backup, or two CAs. \
             Investigate before editing."
        );
    }
}
