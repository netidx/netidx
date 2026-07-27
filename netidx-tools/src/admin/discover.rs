//! `netidx admin discover` — browse mDNS for netidx admin domains and print each
//! one's admin-server address(es) + CA glyph. A pure read-only query: unlike
//! the interactive install discovery (which strict mode disables), this is
//! valid in strict/scripted mode, so a script can discover a admin domain and feed
//! the address + glyph to `--admin-server` / `--accept-glyph`.

use anyhow::{Context, Result};
use clap::Args;
use netidx_admin_client::plan::enroll::{self, DiscoveredAdminDomainReport};
use netidx_admin_proto::{NodeKind, Role, fingerprint::ColorMode};
use std::time::Duration;

#[derive(Args, Debug)]
pub(crate) struct DiscoverArgs {
    /// How long to browse mDNS, in seconds.
    #[arg(long, default_value = "3")]
    timeout: u64,
    /// Emit a JSON array (domain, admin_servers, roles, glyph) for scripting,
    /// instead of the human-readable report.
    #[arg(long)]
    json: bool,
    /// Also print each reachable admin domain's identicon (human output only).
    #[arg(long)]
    identicon: bool,
}

pub(crate) fn run(a: DiscoverArgs) -> Result<()> {
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    let admin_domains = rt.block_on(enroll::discover_admin_domains(
        Duration::from_secs(a.timeout),
        NodeKind::Client,
        None,
    ));
    if a.json {
        print_json(&admin_domains);
    } else {
        print_human(&admin_domains, a.identicon);
    }
    Ok(())
}

fn role_str(role: Role) -> &'static str {
    match role {
        Role::Ca => "ca",
        Role::Resolver => "resolver",
        Role::IdMap => "id-map",
    }
}

fn print_human(admin_domains: &[DiscoveredAdminDomainReport], identicon: bool) {
    if admin_domains.is_empty() {
        println!("no netidx admin domains discovered on the local network.");
        return;
    }
    for n in admin_domains {
        println!("admin domain {:?}", n.domain);
        let addrs =
            n.admin_servers.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(", ");
        println!("  admin server(s): {addrs}");
        match &n.identity {
            Ok(id) => {
                let roles = if id.roles.is_empty() {
                    "none".to_string()
                } else {
                    id.roles.into_iter().map(role_str).collect::<Vec<_>>().join(", ")
                };
                println!("  roles:           {roles}");
                println!("  glyph:           {}", id.fingerprint.text());
                if identicon {
                    println!("{}", id.fingerprint.identicon(ColorMode::detect()));
                }
            }
            Err(e) => println!("  (could not fetch identity: {e})"),
        }
    }
}

fn print_json(admin_domains: &[DiscoveredAdminDomainReport]) {
    use serde_json::{Map, Value};
    let arr = admin_domains
        .iter()
        .map(|n| {
            let mut obj = Map::new();
            obj.insert("domain".into(), Value::from(n.domain.clone()));
            obj.insert(
                "admin_servers".into(),
                Value::from(
                    n.admin_servers.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
                ),
            );
            match &n.identity {
                Ok(id) => {
                    obj.insert("reachable".into(), Value::from(true));
                    obj.insert(
                        "roles".into(),
                        Value::from(
                            id.roles.into_iter().map(role_str).collect::<Vec<_>>(),
                        ),
                    );
                    obj.insert("glyph".into(), Value::from(id.fingerprint.text()));
                }
                Err(e) => {
                    obj.insert("reachable".into(), Value::from(false));
                    obj.insert("error".into(), Value::from(e.clone()));
                }
            }
            Value::Object(obj)
        })
        .collect::<Vec<_>>();
    println!(
        "{}",
        serde_json::to_string_pretty(&Value::Array(arr)).unwrap_or_else(|_| "[]".into())
    );
}
