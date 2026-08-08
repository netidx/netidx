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

use crate::{admin_proto::Secret, fingerprint::Fingerprint, transport::CaIdentity};
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
    // -- install: identity & admin domain -----------------------------------------
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
    /// Whether to found a new admin domain here or connect to an existing one
    /// (resolver install — the role that can found an admin domain).
    AdminDomainMode,
    /// Whether to join an existing admin domain or run this machine stand-alone,
    /// for a role that can't found an admin domain (workstation, publisher).
    Membership,
    /// Which discovered admin domain to connect to (or enter an address manually).
    SelectAdminDomain,
    /// id-map source for a resolver (`platform` / `netidx` / `none`).
    IdMapMode,
    /// Whether to register netidx as an OS service now.
    Service,
    /// The CA's common name.
    CaCommonName,
    /// The SAN glob an admin may issue certificates for.
    AllowSan,
    InsecureNoTpm,
    /// The resolver-server port.
    ResolverPort,
    /// The TLS domain a resolver's certificate name is under.
    TlsDomain,
    /// The admin domain's domain name (groups admin domains in discovery).
    AdminDomainName,
    /// A publisher's resolver-server address(es).
    ResolverAddr,
    /// A publisher's local-auth unix socket path (auth `local`).
    Socket,
    /// The leftmost label of a resolver's certificate name.
    ResolverName,
    /// Whether to set up an admin server (admin-plane CA) for this admin domain.
    SetupAdminServer,
    /// Whether the CA certificate is signed by an external PKI.
    ExternalSign,
    /// The IP the admin server on this host listens on.
    AdminServerListenIp,
    /// The port the admin server on this host listens on.
    AdminServerListenPort,

    // -- install: parent referral / delegation -------------------------------
    /// Parent resolver address for a referral.
    ParentAddr,
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
    /// The replacement an admin is choosing for its own password. Distinct
    /// from [`Field::AdminPassword`] because change-password asks for both in
    /// one flow, and "Administrator password" twice with different meanings
    /// is how someone types the old one into the new one.
    NewAdminPassword,
    /// The off-box CA recovery password (unlocks the CA key offline).
    RecoveryPassword,
    /// The human-readable reason recorded for a revocation.
    RevokeReason,
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
    /// New directory the local CA creates for a consistent backup.
    BackupTarget,
    /// Existing installation bundle selected for restore.
    RestoreSource,
    /// Replacement CA admin address during restore.
    RestoreAdminListen,
    /// Replacement advertised endpoint for a co-located resolver.
    RestoreResolverListen,
    /// Replacement local bind IP for a co-located resolver.
    RestoreResolverBind,
    /// Explicit split-brain fence attestation before CA restore.
    FenceOldCa,
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
                       (certificates issued by this admin domain's CA).",
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
            AdminDomainMode => FieldInfo {
                flag: "--server",
                label: "choose an admin domain",
                help: "Create a new admin domain and certificate authority on \
                       this machine, or enroll under an existing CA. Choose the \
                       existing CA when adding the first resolver below a \
                       dedicated CA host.",
            },
            Membership => FieldInfo {
                flag: "--server",
                label: "stand-alone or join an admin domain",
                help: "Join an existing netidx admin domain on your network, or set \
                       up this machine on its own.",
            },
            SelectAdminDomain => FieldInfo {
                flag: "--admin-server",
                label: "connect to an admin domain",
                help: "Choose a discovered netidx admin domain by its glyph, or enter \
                       an admin-server address manually.",
            },
            IdMapMode => FieldInfo {
                flag: "--id-map",
                label: "user/group id-map source",
                help: "Where the resolver maps users and groups from: platform \
                       (the OS), netidx (an identity map published in netidx), \
                       or none.",
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
            AdminDomainName => FieldInfo {
                flag: "--domain",
                label: "admin domain name",
                help: "The domain name this admin domain is discovered under, \
                       e.g. ryu-oh.org.",
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
                help: "Set up an admin server for this admin domain — a small CA that \
                       secures the admin plane (discovery, enrollment, certificate \
                       renewal). Data-plane auth is unaffected. On an anonymous \
                       data plane it is optional, so strict mode needs an explicit \
                       --with-admin-server (yes) or --no-admin-server (no); \
                       interactive defaults to yes.",
            },
            ExternalSign => FieldInfo {
                flag: "--external-sign",
                label: "use an external root CA?",
                help: "Generate the CA key and a subordinate-CA CSR, then \
                       wait for your external PKI or hardware root to sign it. The \
                       CA starts after the signed certificate is installed.",
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
                help: "Leave blank to install this resolver as a member of the \
                       base resolver cluster, or name a subtree (e.g. /eu) to \
                       request that it be delegated to this resolver as a \
                       cluster of its own — the parent's admin must approve.",
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
            NewAdminPassword => FieldInfo {
                flag: "--new-password-file",
                label: "new password",
                help: "The password to replace your current one with. Supplied \
                       from a file for scripts so it never appears on a command \
                       line; never echoed or stored.",
            },
            RecoveryPassword => FieldInfo {
                flag: "--recovery-password-file",
                label: "CA recovery password",
                help: "The off-box CA recovery password, printed once at CA init \
                       and locked in a safe. Unlocks the CA key to sign offline or \
                       rotate the box credential; supplied from a file/stdin for \
                       scripts, never echoed.",
            },
            RevokeReason => FieldInfo {
                flag: "--reason",
                label: "revocation reason",
                help: "A short human-readable reason recorded with the \
                       revocation.",
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
                help: "Create a root user for this admin domain's certificate authority. \
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
                // The only strict command that can reach this is `admin
                // restore`; `ca external install` takes the certificate as a
                // positional and never asks.
                flag: "--external-cert",
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
                help: "A new directory on this ca for the recovery bundle. \
                       Existing paths are never overwritten.",
            },
            RestoreSource => FieldInfo {
                flag: "--bundle",
                label: "backup bundle directory",
                help: "A bundle created by netidx admin backup. It is completely \
                       verified before restore writes anything.",
            },
            RestoreAdminListen => FieldInfo {
                flag: "--listen",
                label: "restored CA address",
                help: "The routable address and port this replacement ca \
                       will use. The IP defaults from this host's interfaces and the \
                       port comes from the backup.",
            },
            RestoreResolverListen => FieldInfo {
                flag: "--resolver-listen",
                label: "restored resolver address",
                help: "The advertised address and port of the resolver co-located \
                       with this ca. The IP defaults from this host's interfaces \
                       and the port comes from the backup. This updates the resolver \
                       config, local client config, and CA-owned admin domain map together.",
            },
            RestoreResolverBind => FieldInfo {
                flag: "--resolver-bind",
                label: "restored resolver bind IP",
                help: "The local interface IP the restored resolver binds. It usually \
                       matches the advertised resolver IP; use a private interface IP \
                       here when the advertised address is behind NAT.",
            },
            FenceOldCa => FieldInfo {
                flag: "--old-ca-fenced",
                label: "old CA is fenced",
                help: "Confirm that the old CA cannot run. Two machines using \
                       the same CA identity would violate the admin plane's \
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

/// A netidx admin domain discovered on the local network, offered to the operator by
/// [`Answerer::select_admin_domain`]: the TLS domain that groups it in discovery and
/// the CA identity (glyph + fingerprint) fetched from one of its reachable admin
/// servers.
#[derive(Clone)]
pub struct AdminDomainOption {
    pub domain: String,
    pub identity: CaIdentity,
}

/// The operator's pick from [`Answerer::select_admin_domain`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdminDomainChoice {
    /// The discovered admin domain at this index in the offered slice.
    Discovered(usize),
    /// None of the above — enter an admin-server address manually instead.
    Manual,
    /// Browse again and add any newly-discovered admin domains to the list.
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
    /// interactive-only step (admin domain discovery, glyph confirm with no
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

    /// Present the netidx admin domains discovered on the local network — each shown
    /// with its CA glyph and fingerprint — plus a trailing "enter an address
    /// manually" option, and return which the operator picked. Interactive
    /// only: the strict answerer never discovers, so it errors.
    async fn select_admin_domain(
        &mut self,
        domains: &[AdminDomainOption],
    ) -> Result<AdminDomainChoice>;

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

    /// Present a generated password that is shown once and **never
    /// persisted** — there is no second chance to read it. Distinct from
    /// [`note`](Self::note) because it must be impossible to miss: a CLI
    /// prints a boxed banner, a TUI renders a modal that forces
    /// acknowledgment before continuing.
    ///
    /// `secret` says which one it is; the frontend supplies the wording,
    /// since what the operator must *do* with it differs (lock it in a safe
    /// versus hand it to a colleague) and phrasing is the frontend's job.
    async fn show_one_time_secret(
        &mut self,
        secret: OneTimeSecret,
        password: &str,
    ) -> Result<()>;
}

/// Which shown-once secret [`Answerer::show_one_time_secret`] is presenting.
///
/// Owns its name rather than borrowing: a frontend may have to hand this to
/// its UI thread before rendering (the TUI does), and a lifetime here would
/// buy an owned mirror of the same two cases on the other side of that
/// channel. One allocation, on a path that stops to talk to a human anyway.
#[derive(Debug, Clone)]
pub enum OneTimeSecret {
    /// The off-box CA break-glass credential, generated at init or rotate.
    /// The only credential that can unlock the CA key away from the box, so
    /// losing it and the machine loses the CA.
    CaRecovery,
    /// A one-time key for `admin`, from a reset or a freshly minted role
    /// admin. Authorizes only its own replacement, so the operator's job is
    /// to convey it, not to guard it indefinitely.
    AdminPassword { admin: String },
}

/// A scripted [`Answerer`] for engine tests: it hands back queued answers and
/// records which [`Field`]s were asked, so a test can assert both the value a
/// rule produced *and* that the rule asked only what it should have. Any
/// question it has no script for is a test failure, not a silent default.
#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use std::collections::VecDeque;

    pub(crate) struct Scripted {
        interactive: bool,
        text: VecDeque<Option<String>>,
        confirm: VecDeque<bool>,
        /// Every field asked, in order.
        pub(crate) asked: Vec<Field>,
    }

    impl Scripted {
        /// A frontend that prompts, answering `text` then `confirm` in order.
        pub(crate) fn interactive(
            text: impl IntoIterator<Item = Option<&'static str>>,
            confirm: impl IntoIterator<Item = bool>,
        ) -> Self {
            Self {
                interactive: true,
                text: text.into_iter().map(|t| t.map(str::to_string)).collect(),
                confirm: confirm.into_iter().collect(),
                asked: Vec::new(),
            }
        }

        /// A strict frontend: every question is an error, as the CLI's is.
        pub(crate) fn strict() -> Self {
            Self {
                interactive: false,
                text: VecDeque::new(),
                confirm: VecDeque::new(),
                asked: Vec::new(),
            }
        }
    }

    #[async_trait::async_trait]
    impl Answerer for Scripted {
        fn interactive(&self) -> bool {
            self.interactive
        }

        async fn text(
            &mut self,
            field: Field,
            provided: Option<String>,
            _default: Option<&str>,
            _required: bool,
        ) -> Result<Option<String>> {
            if let Some(provided) = provided {
                return Ok(Some(provided));
            }
            self.asked.push(field);
            self.text
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("unscripted question {field:?}"))
        }

        async fn choice(
            &mut self,
            field: Field,
            _provided: Option<String>,
            _choices: &[&str],
            _default: Option<&str>,
        ) -> Result<String> {
            anyhow::bail!("unscripted choice {field:?}")
        }

        async fn select_admin_domain(
            &mut self,
            _domains: &[AdminDomainOption],
        ) -> Result<AdminDomainChoice> {
            anyhow::bail!("unscripted admin domain selection")
        }

        async fn confirm(
            &mut self,
            field: Field,
            provided: Option<bool>,
            _default: bool,
        ) -> Result<bool> {
            if let Some(provided) = provided {
                return Ok(provided);
            }
            self.asked.push(field);
            self.confirm
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("unscripted confirmation {field:?}"))
        }

        async fn secret(
            &mut self,
            field: Field,
            _provided: Option<Secret>,
        ) -> Result<Secret> {
            anyhow::bail!("unscripted secret {field:?}")
        }

        async fn announce(&mut self, _title: &str, _body: &str) -> Result<()> {
            Ok(())
        }

        async fn announce_identity(
            &mut self,
            _body: &str,
            _code: &Fingerprint,
        ) -> Result<()> {
            Ok(())
        }

        async fn confirm_identity(&mut self, _identity: &CaIdentity) -> Result<bool> {
            anyhow::bail!("unscripted identity confirmation")
        }

        fn show_verification_code(&mut self, _purpose: &str, _code: &Fingerprint) {}

        fn clear_verification_code(&mut self) {}

        fn progress(&mut self, _progress: Progress) {}

        fn note(&mut self, _message: &str) {}

        fn warn(&mut self, _message: &str) {}

        async fn show_one_time_secret(
            &mut self,
            _secret: OneTimeSecret,
            _password: &str,
        ) -> Result<()> {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    fn sources(dir: &Path, out: &mut Vec<(String, String)>) {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                sources(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                let name = path.file_name().unwrap().to_string_lossy().into_owned();
                out.push((name, fs::read_to_string(&path).unwrap()));
            }
        }
    }

    /// The variants declared in `pub enum Field`, read from this file rather
    /// than a list someone has to remember to extend.
    fn declared_variants(source: &str) -> Vec<String> {
        let body = source
            .split_once("pub enum Field {")
            .expect("the Field enum")
            .1
            .split_once("\n}\n")
            .expect("the end of the Field enum")
            .0;
        body.lines()
            .filter_map(|line| line.strip_suffix(','))
            .filter(|line| line.starts_with("    ") && !line.starts_with("     "))
            .map(|line| line.trim().to_string())
            .filter(|name| {
                name.chars().next().is_some_and(char::is_uppercase)
                    && name.chars().all(|c| c.is_ascii_alphanumeric())
            })
            .collect()
    }

    fn mentions(source: &str, variant: &str) -> bool {
        let needle = format!("Field::{variant}");
        let mut from = 0;
        while let Some(at) = source[from..].find(&needle) {
            let end = from + at + needle.len();
            // `Field::Admin` must not match `Field::AdminName`.
            let boundary = source[end..]
                .chars()
                .next()
                .is_none_or(|c| !c.is_ascii_alphanumeric() && c != '_');
            if boundary {
                return true;
            }
            from = end;
        }
        false
    }

    /// Every question the engine can ask must be asked *by* the engine.
    ///
    /// A `Field` only a frontend ever passes means the library declared a
    /// decision — with a flag name and help text — and then let someone else
    /// own the ceremony around it privately. That is exactly how the eight
    /// backup/restore variants came to be answered only by the TUI while the
    /// strict CLI grew its own parallel implementation. The compiler cannot
    /// see it, because a `pub enum` variant nobody constructs is not dead
    /// code, so this reads the source instead.
    #[test]
    fn every_field_the_engine_declares_is_a_question_the_engine_asks() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        sources(&root, &mut files);
        let declaration = files
            .iter()
            .find(|(name, _)| name == "answer.rs")
            .expect("answer.rs")
            .1
            .clone();
        let variants = declared_variants(&declaration);
        assert!(variants.len() > 30, "parsed only {:?}", variants);
        // `select_admin_domain` is a trait method of its own, so the engine
        // asks it without ever naming the variant; the variant exists to give
        // the strict answerer a flag to name when it refuses.
        const BY_TRAIT_METHOD: [&str; 1] = ["SelectAdminDomain"];
        let orphans: Vec<_> = variants
            .iter()
            .filter(|variant| !BY_TRAIT_METHOD.contains(&variant.as_str()))
            .filter(|variant| {
                !files
                    .iter()
                    .any(|(name, src)| name != "answer.rs" && mentions(src, variant))
            })
            .collect();
        assert!(
            orphans.is_empty(),
            "these Fields are declared here but asked only by a frontend: {orphans:?}\n\
             Either the engine should own the ceremony that asks them, or the \
             variant does not belong in this enum."
        );
    }
}
