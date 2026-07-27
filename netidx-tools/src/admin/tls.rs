//! `netidx admin component tls …` — client-side TLS identity commands: obtain a
//! certificate for *this* host (generate a key + CSR for offline
//! signing, or join an admin server's CA over the network) and keep it
//! fresh (the auto-renew daemon). CA-operator commands — issuing,
//! signing, approving, revoking — live under `netidx admin ca`.
//!
//! A thin router: `request`/`join` are implemented in the (unix-only,
//! openssl-backed) `ca` module — the certificate-operations engine
//! shared by both command groups — and the auto-renew daemon in
//! `renew`. This module only carves the client-facing surface out of
//! that shared impl so the command tree reads as client vs. CA.

use anyhow::Result;
use clap::Subcommand;

// `ca::request`/`ca::join` (and their arg structs) live in the
// unix-gated `ca` module; the auto-renew daemon is cross-platform.
#[cfg(unix)]
use super::ca;
use super::renew;

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// generate a private key + CSR locally, to be signed by a CA elsewhere
    #[cfg(unix)]
    Request(ca::RequestArgs),
    /// request a certificate from an admin server's CA and install it
    #[cfg(unix)]
    Join(ca::JoinArgs),
    /// keep this host's TLS identities fresh (the renewal daemon)
    AutoRenew {
        #[command(subcommand)]
        cmd: renew::Cmd,
    },
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        #[cfg(unix)]
        Cmd::Request(p) => ca::request(p),
        #[cfg(unix)]
        Cmd::Join(p) => ca::join(p),
        Cmd::AutoRenew { cmd } => renew::run(cmd),
    }
}
