//! `netidx admin host tls …` — client-side TLS identity commands:
//! obtain a certificate for *this* host, either by generating a key + CSR
//! for offline signing or by joining an admin server's CA over the network.
//! CA-operator commands — issuing, signing, approving, revoking — live under
//! `netidx admin ca`; keeping an installed certificate fresh is the admin
//! agent's job (`netidx admin host admin-agent`).
//!
//! A thin router: both commands are implemented in the (unix-only,
//! openssl-backed) `ca` module — the certificate-operations engine shared by
//! both command groups. This module only carves the client-facing surface
//! out of that shared impl so the command tree reads as client vs. CA.

use anyhow::Result;
use clap::Subcommand;

// `ca::request`/`ca::join` (and their arg structs) live in the
// unix-gated `ca` module.
use super::ca;

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// generate a private key + CSR locally, to be signed by a CA elsewhere
    Request(ca::RequestArgs),
    /// request a certificate from an admin server's CA and install it
    Join(ca::JoinArgs),
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Request(p) => ca::request(p),
        Cmd::Join(p) => ca::join(p),
    }
}
