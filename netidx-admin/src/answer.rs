//! The `Answerer` seam.
//!
//! The admin engine (install planner, remote-admin operations) needs to ask
//! the operator questions — an auth scheme, a listen address, an admin
//! password — and to report progress on long steps like discovery and
//! enrollment. It must do this *without knowing* whether it is driven by the
//! strict CLI, the ratatui TUI, or Atlas. So it never touches stdin/stdout or
//! a terminal: it calls [`Answerer`] methods, and a frontend supplies the
//! implementation. This preserves the crate's "library is free of user-IO"
//! rule while keeping the decision logic here, where every frontend can reuse
//! it.
//!
//! Three implementations exist (or will):
//! - a **strict-CLI** answerer that returns a flag's value or errors naming
//!   the missing flag — it never blocks and never discovers;
//! - a **TUI** answerer backed by widgets;
//! - an **Atlas** answerer in its own GUI.
//!
//! The trait is deliberately generic-free (answers are plain strings, parsed
//! by the caller) so it stays `dyn`-safe; [`async_trait`] provides the object
//! safety native `async fn` in traits still lacks.

use crate::{admin_client::CaIdentity, admin_proto::Secret, fingerprint::Fingerprint};
use anyhow::Result;
use compact_str::CompactString;

/// Every decision the admin surface can ask the operator to make.
///
/// A `Field` is the stable identity of a question. Its [`Field::info`]
/// descriptor maps it to the CLI flag a scripted caller would pass (for the
/// strict answerer's error messages) and the human label/help a TUI renders.
/// Adding a decision means adding a variant here; the exhaustive `info` match
/// then forces you to give it a flag, a label, and help — the compiler does
/// the bookkeeping.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Field {
    // -- install: identity & network -----------------------------------------
    /// Data-plane auth scheme (`anonymous` / `local` / `krb5` / `tls`).
    Auth,
    /// Kerberos service principal name for a krb5 resolver/publisher.
    Spn,
    /// The TLS certificate name (SAN) this node serves under.
    TlsName,
    /// The address a resolver listens on.
    Listen,
    /// An optional bind-address override (behind NAT / on cloud hosts).
    Bind,
    /// The namespace base path this role owns.
    Base,
    /// How the private key is protected (`seal` / `password` / `none`).
    KeyProtection,
    /// A typed password protecting a private key.
    KeyPassword,
    /// An askpass helper program for a password-protected key.
    Askpass,
    /// The id-map groups a newly enrolled identity is registered in.
    IdMapGroups,
    /// Whether a CA admin is present to authorize an enrollment now.
    AdminHere,
    /// Whether to join a discovered network.
    JoinNetwork,
    /// Which of several discovered networks to join.
    WhichNetwork,
    /// id-map source for a resolver (`platform` / `netidx` / `none`).
    IdMapMode,
    /// The owner principal a workstation grants admin over its subtree.
    Owner,
    /// OS service scope (`system` / `user`).
    Scope,
    /// Whether to register netidx as an OS service now.
    Service,
    /// The CA's common name.
    CaCommonName,
    /// The SAN glob an admin may issue certificates for.
    AllowSan,
    /// Whether an admin may enroll new admin servers.
    MayEnrollServers,
    /// The resolver-server port.
    ResolverPort,
    /// The TLS domain a resolver's certificate name is under.
    TlsDomain,
    /// The network domain (groups the network in discovery).
    NetworkDomain,
    /// The CA / network domain (e.g. `ryu-oh.org`).
    Domain,
    /// A publisher's resolver-server address(es).
    ResolverAddr,

    // -- install: parent referral / delegation -------------------------------
    /// Parent resolver address for a referral.
    ParentAddr,
    /// The subtree path handed to the parent referral.
    ParentPath,
    /// Parent's Kerberos SPN (krb5 referral).
    ParentSpn,
    /// Parent's TLS name (tls referral).
    ParentTlsName,
    /// The subtree this resolver asks a WAN parent to delegate (e.g. `/eu`).
    DelegateSubtree,

    // -- remote admin --------------------------------------------------------
    /// The admin-server address a remote operation targets (`--server`).
    AdminServerAddr,
    /// The admin (role) name authenticating a remote operation.
    AdminName,
    /// The admin password authenticating a remote operation.
    AdminPassword,
    /// The netidx path a perms / service-control operation acts on (`--at`).
    TargetPath,
    /// The name of an issued cert to revoke.
    RevokeName,
    /// The human-readable reason recorded for a revocation.
    RevokeReason,
}

/// The presentation descriptor for a [`Field`]: what flag a script passes,
/// and how a human sees it.
#[derive(Clone, Copy, Debug)]
pub struct FieldInfo {
    /// The CLI flag a scripted caller passes, e.g. `--tls-name`. The strict
    /// answerer names this flag when the value is missing.
    pub flag: &'static str,
    /// A short label for an interactive prompt, e.g. `TLS certificate name`.
    pub label: &'static str,
    /// One or two plain sentences a newcomer can act on.
    pub help: &'static str,
}

impl Field {
    /// The presentation descriptor for this field. Exhaustive by
    /// construction — a new `Field` variant won't compile until it has one.
    pub fn info(self) -> FieldInfo {
        use Field::*;
        match self {
            Auth => FieldInfo {
                flag: "--auth",
                label: "auth scheme",
                help: "How clients prove who they are: anonymous (no auth), \
                       local (unix peer creds), krb5 (Kerberos), or tls \
                       (certificates issued by this network's CA).",
            },
            Spn => FieldInfo {
                flag: "--spn",
                label: "Kerberos service principal",
                help: "The service principal this resolver runs as, e.g. \
                       netidx/host.example.com@REALM.",
            },
            TlsName => FieldInfo {
                flag: "--tls-name",
                label: "TLS certificate name",
                help: "The DNS name this node's serving certificate is issued \
                       for; subscribers verify it on connect.",
            },
            Listen => FieldInfo {
                flag: "--listen",
                label: "listen address",
                help: "The host:port the resolver server accepts connections \
                       on.",
            },
            Bind => FieldInfo {
                flag: "--bind",
                label: "bind address override",
                help: "Override the address advertised to clients when it \
                       differs from the listen address (NAT / cloud). Usually \
                       left unset.",
            },
            Base => FieldInfo {
                flag: "--base",
                label: "namespace base path",
                help: "The root of the namespace this role owns (resolvers \
                       default to /, workstations to /local).",
            },
            KeyProtection => FieldInfo {
                flag: "--key-protection",
                label: "private-key protection",
                help: "How the private key is kept at rest: seal (TPM-bound, \
                       passwordless), password (you type one), or none.",
            },
            KeyPassword => FieldInfo {
                flag: "--key-password-file",
                label: "private-key password",
                help: "A password to encrypt this private key at rest. Supplied \
                       from a file or stdin for scripts; never echoed.",
            },
            Askpass => FieldInfo {
                flag: "--askpass",
                label: "askpass program",
                help: "A helper the client runs to obtain the key password at \
                       startup when the keychain is unavailable. '-' for none.",
            },
            IdMapGroups => FieldInfo {
                flag: "--id-map-group",
                label: "id-map groups",
                help: "Groups to register this identity in (comma-separated, \
                       first is primary; '-' for none). The CA admin's policy \
                       caps which are allowed.",
            },
            AdminHere => FieldInfo {
                flag: "--admin-here",
                label: "admin present?",
                help: "Yes: a CA admin at this machine authorizes now with their \
                       password. No: queue the request for remote approval.",
            },
            JoinNetwork => FieldInfo {
                flag: "--join",
                label: "join this network?",
                help: "Whether to join the discovered netidx network.",
            },
            WhichNetwork => FieldInfo {
                flag: "--network",
                label: "network to join",
                help: "Which of the discovered netidx networks to join ('none' \
                       for manual setup).",
            },
            IdMapMode => FieldInfo {
                flag: "--id-map",
                label: "user/group id-map source",
                help: "Where the resolver maps users and groups from: platform \
                       (the OS), netidx (a shared network map), or none.",
            },
            Owner => FieldInfo {
                flag: "--owner",
                label: "subtree owner",
                help: "The principal granted admin over this workstation's \
                       subtree; defaults to the installing user.",
            },
            Scope => FieldInfo {
                flag: "--scope",
                label: "service scope",
                help: "Install the OS service system-wide (starts at boot) or \
                       for the current user only.",
            },
            Service => FieldInfo {
                flag: "--with-service",
                label: "register OS service?",
                help: "Register netidx as an OS service now so it starts \
                       automatically (else install it later).",
            },
            CaCommonName => FieldInfo {
                flag: "--cn",
                label: "CA common name",
                help: "The certificate authority's own name, conventionally \
                       ca.<domain>.",
            },
            AllowSan => FieldInfo {
                flag: "--allow-san",
                label: "issuable SAN glob",
                help: "The certificate names this admin may issue, as a glob, \
                       e.g. *.example.com.",
            },
            MayEnrollServers => FieldInfo {
                flag: "--may-enroll-servers",
                label: "may enroll admin servers?",
                help: "Whether this admin may approve new admin-server \
                       enrollments — more privileged than any SAN glob.",
            },
            ResolverPort => FieldInfo {
                flag: "--listen",
                label: "resolver port",
                help: "The port the resolver server listens on (conventionally \
                       4564).",
            },
            TlsDomain => FieldInfo {
                flag: "--tls-name",
                label: "TLS domain",
                help: "The domain part of this resolver's certificate name, \
                       e.g. ryu-oh.org.",
            },
            NetworkDomain => FieldInfo {
                flag: "--domain",
                label: "network domain",
                help: "The domain this network is grouped under in discovery, \
                       e.g. ryu-oh.org.",
            },
            Domain => FieldInfo {
                flag: "--domain",
                label: "network domain",
                help: "The domain this network's CA is named for, e.g. \
                       example.com.",
            },
            ResolverAddr => FieldInfo {
                flag: "--addr",
                label: "resolver address",
                help: "The address of a resolver server this publisher \
                       registers with.",
            },
            ParentAddr => FieldInfo {
                flag: "--parent-addr",
                label: "parent resolver address",
                help: "The parent resolver this node refers up to for paths \
                       outside its own subtree.",
            },
            ParentPath => FieldInfo {
                flag: "--parent-path",
                label: "referral subtree",
                help: "The subtree served under the parent referral.",
            },
            ParentSpn => FieldInfo {
                flag: "--parent-spn",
                label: "parent Kerberos SPN",
                help: "The parent resolver's Kerberos service principal.",
            },
            ParentTlsName => FieldInfo {
                flag: "--parent-tls-name",
                label: "parent TLS name",
                help: "The parent resolver's TLS certificate name.",
            },
            DelegateSubtree => FieldInfo {
                flag: "--delegate-subtree",
                label: "delegated subtree",
                help: "The subtree this resolver asks a WAN parent to delegate \
                       to it, e.g. /eu. The parent's admin approves it.",
            },
            AdminServerAddr => FieldInfo {
                flag: "--server",
                label: "admin server address",
                help: "The admin server to run this operation against; \
                       defaults to this host's own admin server when omitted.",
            },
            AdminName => FieldInfo {
                flag: "--admin",
                label: "admin name",
                help: "The role-admin name authorizing this operation on the \
                       CA.",
            },
            AdminPassword => FieldInfo {
                flag: "--password",
                label: "admin password",
                help: "The password for the named admin. Never echoed or \
                       stored.",
            },
            TargetPath => FieldInfo {
                flag: "--at",
                label: "target path",
                help: "The netidx path this operation acts on (routed to the \
                       cluster that owns it).",
            },
            RevokeName => FieldInfo {
                flag: "--name",
                label: "certificate name",
                help: "The name of the issued certificate to revoke.",
            },
            RevokeReason => FieldInfo {
                flag: "--reason",
                label: "revocation reason",
                help: "A short human-readable reason recorded with the \
                       revocation.",
            },
        }
    }

    /// The CLI flag a scripted caller passes for this field.
    pub fn flag(self) -> &'static str {
        self.info().flag
    }

    /// A short human label for an interactive prompt.
    pub fn label(self) -> &'static str {
        self.info().label
    }

    /// Newcomer-facing help for this field.
    pub fn help(self) -> &'static str {
        self.info().help
    }
}

/// The phase a long-running step is in, so a frontend can show a sensible
/// spinner/label without parsing the message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    /// Browsing the local network for an existing admin server.
    Discovering,
    /// Enrolling a certificate over the admin plane.
    Enrolling,
    /// Waiting for an admin to approve a queued request.
    WaitingApproval,
    /// Writing config, sealing keys, installing services.
    Applying,
    /// Terminal progress note.
    Done,
}

/// A progress report for a long or asynchronous step. Non-blocking: a CLI
/// prints it to stderr, a TUI updates a status pane.
#[derive(Clone, Debug)]
pub struct Progress {
    /// Which phase this note belongs to.
    pub stage: Stage,
    /// A one-line human message.
    pub message: CompactString,
}

impl Progress {
    /// Build a progress note.
    pub fn new(stage: Stage, message: impl Into<CompactString>) -> Self {
        Progress { stage, message: message.into() }
    }
}

/// How the admin engine asks the operator questions and reports progress,
/// abstracted over the frontend driving it (strict CLI / TUI / Atlas).
///
/// Answers are plain strings; the caller parses them into the target type,
/// which keeps this trait `dyn`-safe. `provided` carries a value already
/// supplied out-of-band (a CLI flag); an interactive frontend prompts only
/// when it is `None`, while the strict answerer errors when a required
/// `provided` is `None`.
#[async_trait::async_trait]
pub trait Answerer: Send {
    /// Whether this frontend prompts interactively. The strict-CLI answerer
    /// returns `false`, which tells the engine never to run an
    /// interactive-only step (network discovery, glyph confirm with no
    /// out-of-band value) — every value must come from a flag or error.
    fn interactive(&self) -> bool;

    /// Ask for free text. `default` (if any) is taken on blank input by an
    /// interactive frontend; `required` means a non-empty answer is
    /// mandatory. Returns `None` only when not required and left blank.
    async fn text(
        &mut self,
        field: Field,
        provided: Option<String>,
        default: Option<&str>,
        required: bool,
    ) -> Result<Option<String>>;

    /// Ask the operator to pick one of `choices`; `default` is pre-selected.
    async fn choice(
        &mut self,
        field: Field,
        provided: Option<String>,
        choices: &[&str],
        default: Option<&str>,
    ) -> Result<String>;

    /// Ask a yes/no question with the given default.
    async fn confirm(
        &mut self,
        field: Field,
        provided: Option<bool>,
        default: bool,
    ) -> Result<bool>;

    /// Ask for a secret (password). Never echoed, never defaulted.
    async fn secret(&mut self, field: Field, provided: Option<Secret>) -> Result<Secret>;

    /// The security gesture: present the admin server's identity (domain,
    /// roles, and CA fingerprint — which a frontend renders as text and an
    /// 8×8 identicon) and require an explicit accept before the engine trusts
    /// it. Everything after is pinned to the confirmed fingerprint. Returns
    /// whether the operator confirmed.
    async fn confirm_identity(&mut self, identity: &CaIdentity) -> Result<bool>;

    /// Show an out-of-band verification code (an enrollment request code, a
    /// delegation code) — a fingerprint the operator relays to an admin over
    /// a trusted channel so they can match it before approving. A CLI prints
    /// its text + identicon; a TUI renders the identicon.
    fn show_verification_code(&mut self, purpose: &str, code: &Fingerprint);

    /// Report progress on a long or asynchronous step (discovery, waiting for
    /// approval). Non-blocking.
    fn progress(&mut self, progress: Progress);

    /// An informational note — something the engine did or found that the
    /// operator should see but needn't act on. A CLI prints it; a TUI logs it.
    fn note(&mut self, message: &str);

    /// A non-fatal warning — a degraded outcome the operator should know
    /// about (sealing unavailable, keychain save failed, enrollment denied).
    fn warn(&mut self, message: &str);
}
