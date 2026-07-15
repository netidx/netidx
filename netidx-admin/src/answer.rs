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
use std::time::Duration;

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
    /// The publisher's required `BindCfg` routing/bind selection.
    PublisherBind,
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
    /// Whether to found a new cluster here or connect to an existing one
    /// (resolver install — the role that can found a cluster).
    ClusterMode,
    /// Whether to join an existing cluster or run this machine stand-alone,
    /// for a role that can't found a cluster (workstation, publisher).
    Membership,
    /// Which discovered cluster to connect to (or enter an address manually).
    SelectNetwork,
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
    InsecureNoTpm,
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
    /// A publisher's local-auth unix socket path (auth `local`).
    Socket,
    /// The leftmost label of a resolver's certificate name.
    ResolverName,
    /// Whether to set up an admin server (admin-plane CA) for this network.
    SetupAdminServer,
    /// Whether the controller CA certificate is signed by an external PKI.
    ExternalSign,
    /// The IP the admin server on this host listens on.
    AdminServerListenIp,
    /// The port the admin server on this host listens on.
    AdminServerListenPort,

    // -- install: parent referral / delegation -------------------------------
    /// Parent resolver address for a referral.
    ParentAddr,
    /// The subtree path handed to the parent referral.
    ParentPath,
    /// The auth scheme a node uses to authenticate to its parent resolver.
    ParentAuth,
    /// Parent's local-auth unix socket path (parent-auth `local`).
    ParentSocket,
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
    /// The off-box CA recovery password (unlocks the CA key offline).
    RecoveryPassword,
    /// The netidx path a perms / service-control operation acts on (`--at`).
    TargetPath,
    /// The name of an issued cert to revoke.
    RevokeName,
    /// The human-readable reason recorded for a revocation.
    RevokeReason,
    /// The unix uid a newly signed offline identity maps to.
    Uid,
    /// Whether to sign a CSR with the SAN it already carries (offline `ca sign`).
    AcceptCsrSan,
    /// Whether an admin may mint and scope other admins.
    MayManageAdmins,
    /// The founding root user's name (a new server CA's superuser).
    RootAdminName,
    /// Re-entry of a new admin password, to confirm it matches.
    AdminPasswordConfirm,
    /// Path to a CA certificate an external PKI signed (external-CA install).
    SignedCert,
    /// Path to an external PKI's root certificate (external-CA install).
    ExternalRoot,
    /// New directory the local controller creates for a consistent backup.
    BackupTarget,
    /// Existing installation bundle selected for restore.
    RestoreSource,
    /// Explicit split-brain fence attestation before controller restore.
    FenceOldController,
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
                       (certificates issued by this cluster's CA).",
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
                help: "Where the resolver server accepts connections — an IP \
                       (you'll be asked for the port next) or a full host:port.",
            },
            Bind => FieldInfo {
                flag: "--bind",
                label: "bind address override",
                help: "Override the address advertised to clients when it \
                       differs from the listen address (NAT / cloud). Usually \
                       left unset.",
            },
            PublisherBind => FieldInfo {
                flag: "--bind",
                label: "publisher network bind",
                help: "The network this publisher should bind and advertise on, \
                       for example 10.0.0.0/24, an exact host as 10.0.0.5/32, \
                       or local. The detected interface subnet is usually the \
                       right choice.",
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
                help: "This node's private key is stored on disk. How would you \
                       like to protect it? seal (bound to this machine's TPM, \
                       unlocked automatically at startup), password (you enter it \
                       every time the service starts), or none (stored \
                       unencrypted).",
            },
            KeyPassword => FieldInfo {
                flag: "--key-password-file",
                label: "private-key password",
                help: "A password to encrypt this private key at rest. You will \
                       need to enter it every time the service starts (from the \
                       system keychain or an askpass helper). Supplied from a file \
                       or stdin for scripts; never echoed.",
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
            ClusterMode => FieldInfo {
                flag: "--server",
                label: "choose an administrative network",
                help: "Create a new administrative network and certificate \
                       authority on this machine, or enroll under an existing \
                       controller / CA. Choose the existing controller when \
                       adding the first resolver below a dedicated CA host.",
            },
            Membership => FieldInfo {
                flag: "--server",
                label: "stand-alone or join a cluster",
                help: "Join an existing netidx cluster on your network, or set \
                       up this machine on its own.",
            },
            SelectNetwork => FieldInfo {
                flag: "--admin-server",
                label: "connect to a cluster",
                help: "Choose a discovered netidx cluster by its glyph, or enter \
                       an admin-server address manually.",
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
            InsecureNoTpm => FieldInfo {
                flag: "--insecure-no-tpm",
                label: "proceed without a TPM (insecure)?",
                help: "This host has no usable TPM, so the CA's autorenew \
                       credential would be written UNSEALED — in plaintext in \
                       every backup, equivalent to backing up the CA key. Only \
                       accept for TEST CAs; a real CA belongs on hardware with a \
                       TPM 2.0 / Secure Enclave.",
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
                label: "cluster domain",
                help: "The domain this cluster is grouped under in discovery, \
                       e.g. ryu-oh.org.",
            },
            Domain => FieldInfo {
                flag: "--domain",
                label: "cluster domain",
                help: "The domain this cluster's CA is named for, e.g. \
                       example.com.",
            },
            ResolverAddr => FieldInfo {
                flag: "--addr",
                label: "resolver address",
                help: "The address of a resolver server this publisher \
                       registers with.",
            },
            Socket => FieldInfo {
                flag: "--socket",
                label: "local-auth socket path",
                help: "The resolver's local-auth unix socket a publisher \
                       connects through (used only with auth local).",
            },
            ResolverName => FieldInfo {
                flag: "--tls-name",
                label: "resolver name",
                help: "The leftmost label of this resolver's certificate name. It \
                       is joined to the TLS domain to form the full certificate \
                       name (SAN) that subscribers verify — e.g. 'resolver' plus \
                       'example.com' becomes resolver.example.com. Defaults to \
                       'resolver'; a full --tls-name sets both at once.",
            },
            SetupAdminServer => FieldInfo {
                flag: "--with-admin-server",
                label: "set up admin server?",
                help: "Set up an admin server for this cluster — a small CA that \
                       secures the admin plane (discovery, enrollment, certificate \
                       renewal). Data-plane auth is unaffected. On an anonymous \
                       data plane it is optional, so strict mode needs an explicit \
                       --with-admin-server (yes) or --no-admin-server (no); \
                       interactive defaults to yes.",
            },
            ExternalSign => FieldInfo {
                flag: "--external-sign",
                label: "use an external root CA?",
                help: "Generate the controller CA key and a subordinate-CA CSR, then \
                       wait for your external PKI or hardware root to sign it. The \
                       controller starts after the signed certificate is installed.",
            },
            AdminServerListenIp => FieldInfo {
                flag: "--listen",
                label: "admin server listen IP",
                help: "The IP the admin server on this host listens on for \
                       discovery, enrollment, and CSR signing; usually this \
                       machine's advertised IP (the resolver co-located here \
                       will share it).",
            },
            AdminServerListenPort => FieldInfo {
                flag: "--listen",
                label: "admin server listen port",
                help: "The port the admin server on this host listens on \
                       (conventionally 4565).",
            },
            ParentAddr => FieldInfo {
                flag: "--parent-addr",
                label: "parent resolver address",
                help: "The parent resolver this node refers up to for paths \
                       outside its own subtree — an IP (you'll be asked for the \
                       port) or a full host:port.",
            },
            ParentPath => FieldInfo {
                flag: "--parent-path",
                label: "referral subtree",
                help: "The subtree served under the parent referral.",
            },
            ParentAuth => FieldInfo {
                flag: "--parent-auth",
                label: "parent auth scheme",
                help: "How this node authenticates to its parent resolver: \
                       anonymous, local, krb5, or tls.",
            },
            ParentSocket => FieldInfo {
                flag: "--parent-socket",
                label: "parent local-auth socket",
                help: "The parent resolver's local-auth unix socket path (used \
                       only with parent-auth local).",
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
                help: "Leave blank to install this resolver as a peer of the \
                       base cluster, or name a subtree (e.g. /eu) to request the \
                       network delegate it to this resolver — the parent's admin \
                       must approve the delegation.",
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
            RecoveryPassword => FieldInfo {
                flag: "--recovery-password-file",
                label: "CA recovery password",
                help: "The off-box CA recovery password, printed once at CA init \
                       and locked in a safe. Unlocks the CA key to sign offline or \
                       rotate the box credential; supplied from a file/stdin for \
                       scripts, never echoed.",
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
            Uid => FieldInfo {
                flag: "--uid",
                label: "unix uid",
                help: "The unix user id a newly signed identity maps to in the \
                       resolver's id-map.",
            },
            AcceptCsrSan => FieldInfo {
                flag: "--accept-csr-san",
                label: "accept the CSR's SAN?",
                help: "Embed the SubjectAltName the CSR already carries in the \
                       signed cert. The CA is authoritative — pass --san \
                       <kind>:<value> instead to override what the CSR requested.",
            },
            MayManageAdmins => FieldInfo {
                flag: "--may-manage-admins",
                label: "may manage admins?",
                help: "Whether this admin may mint and scope other admins — the \
                       superuser capability. The server still forbids privilege \
                       escalation.",
            },
            RootAdminName => FieldInfo {
                flag: "--admin",
                label: "root user name",
                help: "Create a root user for this cluster's certificate authority. \
                       The root user can sign certificates and perform any other \
                       administrative function, including creating other users. \
                       Defaults to your current login name.",
            },
            AdminPasswordConfirm => FieldInfo {
                flag: "--password",
                label: "confirm password",
                help: "Re-enter the password to confirm it — the two entries must \
                       match.",
            },
            SignedCert => FieldInfo {
                flag: "--signed-cert",
                label: "signed certificate path",
                help: "Path to the CA certificate your external PKI signed from the \
                       emitted CSR.",
            },
            ExternalRoot => FieldInfo {
                flag: "--root",
                label: "external root certificate path",
                help: "Path to your external PKI's root certificate, if the signed \
                       certificate does not already carry the chain. Leave blank to \
                       use the chain in the signed certificate.",
            },
            BackupTarget => FieldInfo {
                flag: "--target",
                label: "backup target directory",
                help: "A new directory on this controller for the recovery bundle. \
                       Existing paths are never overwritten.",
            },
            RestoreSource => FieldInfo {
                flag: "--bundle",
                label: "backup bundle directory",
                help: "A bundle created by netidx admin backup. It is completely \
                       verified before restore writes anything.",
            },
            FenceOldController => FieldInfo {
                flag: "--old-controller-fenced",
                label: "old controller is fenced",
                help: "Confirm that the old controller cannot run. Two machines using \
                       the same controller identity would violate the admin plane's \
                       single-writer security boundary.",
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
    /// The expected length of this step, when known — a frontend can drive a
    /// determinate progress bar over it. `None` ⇒ open-ended (a frontend shows
    /// an indeterminate/marquee indicator).
    pub duration: Option<Duration>,
}

impl Progress {
    /// Build an open-ended progress note (indeterminate).
    pub fn new(stage: Stage, message: impl Into<CompactString>) -> Self {
        Progress { stage, message: message.into(), duration: None }
    }

    /// Build a progress note for a step of known length, so a frontend can
    /// drive a determinate bar over `duration`.
    pub fn timed(
        stage: Stage,
        message: impl Into<CompactString>,
        duration: Duration,
    ) -> Self {
        Progress { stage, message: message.into(), duration: Some(duration) }
    }
}

/// A netidx cluster discovered on the local network, offered to the operator by
/// [`Answerer::select_network`]: the TLS domain that groups it in discovery and
/// the CA identity (glyph + fingerprint) fetched from one of its reachable admin
/// servers.
#[derive(Clone)]
pub struct NetworkOption {
    pub domain: String,
    pub identity: CaIdentity,
}

/// The operator's pick from [`Answerer::select_network`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkChoice {
    /// The discovered network at this index in the offered slice.
    Discovered(usize),
    /// None of the above — enter an admin-server address manually instead.
    Manual,
    /// Browse again and add any newly-discovered clusters to the list.
    PollMore,
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

    /// Present the netidx clusters discovered on the local network — each shown
    /// with its CA glyph and fingerprint — plus a trailing "enter an address
    /// manually" option, and return which the operator picked. Interactive
    /// only: the strict answerer never discovers, so it errors.
    async fn select_network(
        &mut self,
        networks: &[NetworkOption],
    ) -> Result<NetworkChoice>;

    /// Ask a yes/no question with the given default.
    async fn confirm(
        &mut self,
        field: Field,
        provided: Option<bool>,
        default: bool,
    ) -> Result<bool>;

    /// Ask for a secret (password). Never echoed, never defaulted.
    async fn secret(&mut self, field: Field, provided: Option<Secret>) -> Result<Secret>;

    /// Whether this frontend already holds an explicitly supplied secret for
    /// `field` (for example `--password-file`). This lets reusable-session
    /// callers honor the rule that an explicit password takes precedence over
    /// a cache without consuming or exposing the secret before endpoint
    /// verification. Interactive prompts return false.
    fn has_explicit_secret(&self, _field: Field) -> bool {
        false
    }

    /// Announce a new section or component of the flow — a short informational
    /// dialog the operator acknowledges before the next questions (e.g. "Now
    /// setting up the resolver server"). Blocks until acknowledged by an
    /// interactive frontend; a non-interactive one returns immediately.
    async fn announce(&mut self, title: &str, body: &str) -> Result<()>;

    /// Like [`announce`], but the dialog also shows a fingerprint's identicon —
    /// for presenting a freshly-created CA's identity (the glyph joiners will
    /// verify) inline, rather than as loose text. Blocks until acknowledged; a
    /// non-interactive frontend prints the code and returns.
    async fn announce_identity(&mut self, body: &str, code: &Fingerprint) -> Result<()>;

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

    /// Dismiss the verification code after its queued request settles.
    fn clear_verification_code(&mut self);

    /// Report progress on a long or asynchronous step (discovery, waiting for
    /// approval). Non-blocking.
    fn progress(&mut self, progress: Progress);

    /// An informational note — something the engine did or found that the
    /// operator should see but needn't act on. A CLI prints it; a TUI logs it.
    fn note(&mut self, message: &str);

    /// A non-fatal warning — a degraded outcome the operator should know
    /// about (sealing unavailable, keychain save failed, enrollment denied).
    fn warn(&mut self, message: &str);

    /// Present the CA recovery password — generated once at CA init, shown
    /// once, and **never persisted**. This is the off-box break-glass secret
    /// the operator must copy into a safe now; there is no second chance to
    /// read it. Distinct from [`note`] because it must be impossible to miss:
    /// a CLI prints a boxed banner, a TUI renders a modal that forces
    /// acknowledgment before continuing.
    async fn show_recovery_password(&mut self, password: &str) -> Result<()>;
}
