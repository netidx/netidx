//! `netidx admin component tls …` — client-side TLS identity commands:
//! obtain a certificate for *this* host, either by generating a key + CSR
//! for offline signing or by joining an admin server's CA over the network.
//! CA-operator commands — issuing, signing, approving, revoking — live under
//! `netidx admin ca`; keeping an installed certificate fresh is the admin
//! agent's job (`netidx admin component admin-agent`).
//!
//! A thin router: both commands are implemented in the (unix-only,
//! openssl-backed) `ca` module — the certificate-operations engine shared by
//! both command groups. This module only carves the client-facing surface
//! out of that shared impl so the command tree reads as client vs. CA.

use anyhow::Result;
use clap::Subcommand;

// `ca::request`/`ca::join` (and their arg structs) live in the
// unix-gated `ca` module.
#[cfg(unix)]
use super::ca;

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// generate a private key + CSR locally, to be signed by a CA elsewhere
    #[cfg(unix)]
    Request(ca::RequestArgs),
    /// request a certificate from an admin server's CA and install it
    #[cfg(unix)]
    Join(ca::JoinArgs),
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        #[cfg(unix)]
        Cmd::Request(p) => ca::request(p),
        #[cfg(unix)]
        Cmd::Join(p) => ca::join(p),
    }
}
