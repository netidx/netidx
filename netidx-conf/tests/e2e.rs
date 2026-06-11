//! End-to-end template tests.
//!
//! Each test:
//!
//! 1. Picks a free TCP port and builds template params pointing at a
//!    tempdir.
//! 2. Renders the template and calls `apply()` to write the configs.
//! 3. Loads the just-written configs back from disk via the same
//!    `netidx::config::Config::load` / `netidx::resolver_server::config::Config::load`
//!    helpers a real binary would call at start.
//! 4. Spins up `netidx::resolver_server::Server` + a Publisher +
//!    a Subscriber all in this tokio runtime.
//! 5. Publishes a value and asserts the subscriber sees it.
//!
//! This catches every "the config we just generated wouldn't actually
//! work in netidx" bug — TLS path mismatches, addr/auth scheme
//! disagreements, `default_bind_config` defaults landing the
//! publisher on the wrong interface, etc. — that pure render-time
//! tests can't see.
//!
//! What this layer does NOT cover (by design — see the discussion at
//! the top of `netidx-tools/src/conf/init.rs`):
//!
//! - systemd / launchd install (no service supervisor in-process).
//! - daemonize (process fork; can't happen inside a tokio test).
//! - Kerberos (the KDC is external).
//! - multi-host topology (parent referrals across machines).
//!
//! Those live in the container/VM test layer that will (eventually)
//! sit alongside this file.

use anyhow::Result;
use arcstr::ArcStr;
use netidx::{
    config as cfg_client,
    path::Path,
    publisher::{PublisherBuilder, Value},
    resolver_server::{self, config as cfg_resolver},
    subscriber::{Event, SubscriberBuilder},
};
// `ca` + `tls_install` (TLS test only) depend on openssl → unix-only.
// `id_map_engine` + the id-map daemon are used only by the TLS test
// today, so gate them too rather than warn about unused imports on
// Windows.
#[cfg(unix)]
use netidx_conf::{ca, id_map as id_map_engine, tls as tls_install};
// `WorkstationParams` is referenced only by the Local-auth workstation
// test (also unix-only).
#[cfg(unix)]
use netidx_conf::template::workstation::WorkstationParams;
use netidx_conf::template::{
    self, AuthChoice, ReferralAuth,
    publisher::PublisherParams,
    resolver::ResolverParams,
};
#[cfg(unix)]
use netidx_id_map::runtime::{Server as IdMapServer, ServerParams as IdMapParams};
use std::{path::PathBuf, sync::OnceLock, time::Duration};
use tempfile::TempDir;

/// One-time setup: redirect `XDG_CONFIG_HOME` (and on macOS,
/// `$HOME/Library/...` via `HOME`) at a per-process tempdir so that
/// any template path that resolves through `dirs::config_dir()` —
/// notably `tls::identity_dir(...)`, which several templates install
/// certs into and the auto-generated client config references —
/// doesn't pollute the developer's real `~/.config/netidx`. Without
/// this, running `cargo test` overwrites `~/.config/netidx/tls/resolver/`
/// with the test CA's cert, which is rude.
///
/// We deliberately hold the `TempDir` in a `OnceLock` (not just leak
/// the path) so the directory is cleaned up at process exit on the
/// happy path. Tests use their *own* tempdirs for explicit paths
/// (resolver.json, client.json, ...) — this only covers the
/// "user config" implicit defaults.
fn ensure_xdg_redirect() {
    static GUARD: OnceLock<TempDir> = OnceLock::new();
    GUARD.get_or_init(|| {
        let td = TempDir::new().expect("test xdg tempdir");
        // SAFETY: env mutation is technically unsound in a
        // multi-threaded program, but `OnceLock::get_or_init`
        // serialises the first call across all threads, so only one
        // thread mutates the env, and it does so *before* any test
        // can read XDG_CONFIG_HOME via dirs::config_dir(). Once the
        // OnceLock is initialised, future calls are no-ops and the
        // env var is stable.
        unsafe {
            std::env::set_var("XDG_CONFIG_HOME", td.path());
        }
        td
    });
}

/// Pick a likely-free local TCP port by binding to `127.0.0.1:0` and
/// immediately releasing. There's a race between the release and the
/// caller's bind, but for a single-process test runner with low
/// concurrency it's fine — and beats hard-coding a port that would
/// clash with concurrent test runs (or, e.g., a real netidx daemon on
/// the dev machine).
fn pick_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Subscribe at `path`, wait for the subscription to come live, then
/// read the current value. Bounded by `timeout` so a wiring bug
/// surfaces as a clear test failure rather than a hang.
async fn await_subscription_value(
    sub: &netidx::subscriber::Subscriber,
    path: Path,
    timeout: Duration,
) -> Result<Value> {
    let dval = sub.subscribe(path);
    tokio::time::timeout(timeout, dval.wait_subscribed()).await??;
    match dval.last() {
        Event::Update(v) => Ok(v),
        Event::Unsubscribed => {
            anyhow::bail!("subscription died between wait_subscribed and last")
        }
    }
}

/// Spin up a publisher + subscriber from the supplied (already
/// loaded + validated) client config, publish one value, assert the
/// subscriber observes it. The supplied path is used as the netidx
/// name; pick something unique per test to avoid cross-test
/// interference if/when this harness adds parallel cases.
async fn round_trip(
    client_cfg: cfg_client::Config,
    path: &str,
    value: Value,
) -> Result<()> {
    let path = Path::from(ArcStr::from(path));
    let publisher = PublisherBuilder::new(client_cfg.clone()).build().await?;
    let _val = publisher.publish(path.clone(), value.clone())?;
    // flushed() awaits any pending publish-side work — gives the
    // resolver a chance to register our publish before the subscriber
    // tries to resolve it.
    publisher.flushed().await;

    let subscriber = SubscriberBuilder::new(client_cfg).build()?;
    let observed =
        await_subscription_value(&subscriber, path.clone(), Duration::from_secs(5))
            .await?;
    assert_eq!(
        observed, value,
        "round-trip value mismatch at {path}: expected {value:?}, got {observed:?}",
    );
    Ok(())
}

/// Build a `ResolverParams` for an anonymous-auth resolver bound to
/// 127.0.0.1 on `port`, writing all artifacts into `dir`. Used by
/// every anonymous-auth test as the common case.
fn anon_params(dir: &TempDir, port: u16) -> ResolverParams {
    ResolverParams {
        auth: AuthChoice::Anonymous,
        base: ArcStr::from("/"),
        listen: format!("127.0.0.1:{port}").parse().unwrap(),
        bind: None,
        parent: None,
        // We exercise the perms-file path in dedicated tests; keep
        // the bare round-trip simple.
        perms_seed: None,
        with_perms_file: false,
        perms_path: None,
        resolver_config_path: Some(dir.path().join("resolver.json")),
        // No service units in an in-process test — we're not booting
        // systemd. Skip to keep the tempdir clean.
        units_dir: None,
        // Unused when `units_dir` is None, but the field's required.
        // Pick something absolute so the template's validation passes
        // even on this code path (it errors on relative).
        netidx_binary: PathBuf::from("/usr/local/bin/netidx"),
        with_id_map: false,
        id_map_path: None,
        id_map_socket: None,
        with_local_client: true,
        client_config_path: Some(dir.path().join("client.json")),
        local_client_bind: None,
    }
}

// ---- the actual tests -------------------------------------------------------

/// Smoke test of the simplest possible flow: render the resolver
/// template with anonymous auth + `with_local_client`, apply it, then
/// use the auto-generated client config to push a value through. If
/// this test passes, the bare addrs / auth / `default_bind_config`
/// plumbing between resolver and client config is correct.
#[tokio::test(flavor = "multi_thread")]
async fn resolver_template_anonymous_round_trip() -> Result<()> {
    let _ = env_logger::try_init();
    ensure_xdg_redirect();
    let dir = TempDir::new()?;
    let port = pick_port();

    let rt = template::resolver::resolver(&anon_params(&dir, port))?;
    rt.apply()?;

    // Load both configs the same way a real binary does.
    let resolver_cfg = cfg_resolver::Config::load(dir.path().join("resolver.json"))?;
    let _server = resolver_server::Server::new(resolver_cfg, false, 0).await?;

    let client_cfg = cfg_client::Config::load(dir.path().join("client.json"))?;
    round_trip(client_cfg, "/e2e/anonymous", Value::U32(42)).await?;
    Ok(())
}

/// Same as the anonymous round-trip, but with the auto-seeded
/// perms file in play (the default behaviour from change set
/// "resolver template includes perms.json by default"). Verifies
/// that the auto-seeded base-anchored rules (`<base>/$[user]` →
/// `$[user]` → swlpd and `<base>` → users group → swl) are
/// loadable by the resolver, AND don't break anonymous publish at
/// any path — anonymous principals match no entity in the seed,
/// but anonymous auth bypasses perms checks entirely, so the
/// round-trip must still succeed.
#[tokio::test(flavor = "multi_thread")]
async fn resolver_template_anonymous_with_default_perms_round_trip() -> Result<()> {
    let _ = env_logger::try_init();
    ensure_xdg_redirect();
    let dir = TempDir::new()?;
    let port = pick_port();
    let mut params = anon_params(&dir, port);
    params.with_perms_file = true;
    params.perms_path = Some(dir.path().join("perms.json"));

    let rt = template::resolver::resolver(&params)?;
    rt.apply()?;

    let resolver_cfg = cfg_resolver::Config::load(dir.path().join("resolver.json"))?;
    let _server = resolver_server::Server::new(resolver_cfg, false, 0).await?;

    let client_cfg = cfg_client::Config::load(dir.path().join("client.json"))?;
    round_trip(client_cfg, "/e2e/anonymous-with-perms", Value::U32(7)).await?;
    Ok(())
}

/// Workstation template end-to-end: emits a *loopback* resolver
/// bound to a unix socket for `Local` auth, plus a client config
/// pointing at it. Exercises the same `Local`-over-unix-socket auth
/// path the real installed workstation uses, but with every path
/// scoped to the tempdir so concurrent test runs don't collide.
///
/// Uses the current Unix username as the `owner`, so the auto-seeded
/// perms file grants the test process full rights at `/local`.
/// (Without that grant, the resolver would `Deny` every publish /
/// resolve from the in-process publisher, which was the failure mode
/// the previous iteration of this test surfaced.)
///
/// **Unix-only**: the workstation template on non-unix uses
/// Anonymous auth (Local auth requires unix socket peer creds,
/// which Windows doesn't have). A separate test would cover that
/// path; for now we leave the gap and let the unix test catch the
/// Local-auth-specific wiring.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn workstation_template_local_round_trip() -> Result<()> {
    let _ = env_logger::try_init();
    ensure_xdg_redirect();
    let dir = TempDir::new()?;
    let port = pick_port();
    // The test process IS the Local-auth principal — peer creds give
    // the resolver our actual uid, which it then maps to a username
    // via /bin/id. Seeding perms with that same username is the only
    // way an in-process pub/sub round-trip can succeed without us
    // overriding the resolver's id-map.
    let current_user = nix::unistd::User::from_uid(nix::unistd::Uid::current())?
        .ok_or_else(|| anyhow::anyhow!("could not determine current user"))?
        .name;
    let params = WorkstationParams {
        parent: None,
        tls_identities: vec![],
        default_auth: None,
        base: ArcStr::from("/local"),
        listen_port: Some(port),
        local_socket: Some(dir.path().join("auth.sock")),
        client_config_path: Some(dir.path().join("client.json")),
        resolver_config_path: Some(dir.path().join("resolver.json")),
        units_dir: None,
        netidx_binary: PathBuf::from("/usr/local/bin/netidx"),
        with_container: false,
        owner: Some(ArcStr::from(current_user.as_str())),
        perms_seed: None,
        with_perms_file: true,
        perms_path: Some(dir.path().join("perms.json")),
    };
    let rt = template::workstation::workstation(&params)?;
    rt.apply()?;

    let resolver_cfg = cfg_resolver::Config::load(dir.path().join("resolver.json"))?;
    let _server = resolver_server::Server::new(resolver_cfg, false, 0).await?;

    let client_cfg = cfg_client::Config::load(dir.path().join("client.json"))?;
    round_trip(client_cfg, "/local/e2e/workstation", Value::String("hi".into()))
        .await?;
    Ok(())
}

/// Resolver template with TLS auth + id-map, end-to-end. The most
/// demanding case: a local CA issues a cert for the resolver; with
/// base="/" the auto-seeded perms file grants `/$[user]` →
/// `$[user]` → `swlpd` *and* `/` → `<cert-SAN>` → `swlpd` (the
/// resolver-cert seed); the in-process id-map daemon maps the TLS
/// SAN `resolver` to uid 1000 + group `users`; pub/sub at
/// `/resolver/foo` succeeds because the resolver sees the TLS
/// principal, resolves their uid via the id-map, and the perm
/// check finds the dynamic `$[user]` entry (and the TLS-cert
/// entry) match.
///
/// What this catches end-to-end that pure render tests can't:
/// - TLS path mismatches (resolver's cert dir vs client's cert dir).
/// - The cert SAN ↔ id-map identity name ↔ perms `$[user]`
///   expansion all agreeing on the same string.
/// - The activation-unit-driven id-map daemon flow (which we run
///   in-process here via `IdMapServer::start`, since there's no
///   activation supervisor in a tokio test).
///
/// **Unix-only**: the test issues certs via `netidx_conf::ca`,
/// which depends on openssl (unix-only — we don't ship openssl to
/// Windows). On Windows a TLS workstation install uses pre-issued
/// certs supplied via explicit flags.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn resolver_template_tls_round_trip() -> Result<()> {
    let _ = env_logger::try_init();
    ensure_xdg_redirect();
    let dir = TempDir::new()?;
    let port = pick_port();

    let ca_dir = dir.path().join("ca");
    let ca = ca::Ca::init(
        &ca::CaParams {
            directory: ca_dir.clone(),
            subject: ca::Subject::cn("e2e-test-ca"),
            san: vec![],
            key_bits: 2048,
            validity_days: 30,
        },
        None,
    )?;
    // Resolver cert: SAN = "resolver.example.com" — the multi-label
    // form lets the template derive the identity domain
    // (`example.com`) for the client-side `tls.identities` key while
    // keeping `resolver.example.com` as the wire identity / install
    // dir name. Single-label SANs are rejected by
    // `tls::domain_from_san`.
    let resolver_id_src = dir.path().join("resolver-id-src");
    let resolver_issued = ca.issue(&ca::IssueParams {
        subject: ca::Subject::cn("resolver.example.com"),
        san: vec![ca::SanEntry::Dns("resolver.example.com".into())],
        key_bits: 2048,
        validity_days: 30,
        out_dir: resolver_id_src.clone(),
        password: None,
    })?;

    // Render the resolver template with TLS auth + auto-seed perms +
    // the id-map daemon enabled. With base="/", the auto-seeded perms
    // grant `/users/$[user]` to `$[user]` and (for TLS) `/` to the
    // cert SAN — so a publish under either `/users/<SAN>/...` or
    // anywhere at `/` succeeds once the id-map maps the SAN to a uid.
    let id_map_sock = dir.path().join("id-map.sock");
    let id_map_json = dir.path().join("id-map.json");
    let mut params = anon_params(&dir, port);
    params.auth = AuthChoice::Tls {
        name: ArcStr::from("resolver.example.com"),
        certificate: resolver_issued.certificate.clone(),
        private_key: resolver_issued.private_key.clone(),
        trusted: ca_dir.join("certificate.pem"),
        askpass: None,
    };
    params.with_perms_file = true;
    params.perms_path = Some(dir.path().join("perms.json"));
    params.with_id_map = true;
    params.id_map_socket = Some(id_map_sock.clone());
    params.id_map_path = Some(id_map_json.clone());
    let rt = template::resolver::resolver(&params)?;
    rt.apply()?;

    // Sanity check: the install actually placed the resolver's cert
    // at the canonical (XDG-redirected) location.
    let resolver_tls_dir = tls_install::identity_dir("resolver.example.com")?;
    assert!(
        resolver_tls_dir.join("certificate.pem").exists(),
        "resolver identity not installed at canonical location ({})",
        resolver_tls_dir.display(),
    );

    // Seed the id-map with an entry for the TLS principal `resolver`.
    // Overwrite the empty starter the template's apply() dropped —
    // `id_map_engine::save` validates structurally before writing.
    let mut map = id_map_engine::empty();
    id_map_engine::upsert_identity(&mut map, "resolver.example.com", 1000, "users", &[])?;
    id_map_engine::save(&id_map_json, &map)?;

    // Start the id-map daemon in-process. The resolver's auth check
    // will connect to this socket every time it needs to resolve a
    // TLS SAN → unix uid + group.
    let _id_map_daemon = IdMapServer::start(IdMapParams::new(id_map_sock, id_map_json))
        .await?;

    let resolver_cfg = cfg_resolver::Config::load(dir.path().join("resolver.json"))?;
    let server = resolver_server::Server::new(resolver_cfg, false, 0).await?;

    // Probe the running resolver for its served TLS name — the discovery
    // the setup flow uses to prefill the "resolver TLS name" prompt. This
    // exercises the whole probe path against a real *mutual*-TLS resolver:
    // the handshake is rejected (the probe sends no client cert), so the
    // result depends on the TOFU verifier capturing the leaf before that
    // rejection and reading its DNS SAN.
    let probed =
        netidx_conf::resolver_probe::probe_resolver_tls_name(*server.local_addr())
            .await?;
    assert_eq!(probed.as_deref(), Some("resolver.example.com"));

    // Path under `/users/resolver.example.com/` matches the auto-seed
    // `/users/$[user]` dynamic entry with $[user]=resolver.example.com
    // (the TLS-cert entry at base "/" would also cover it).
    let client_cfg = cfg_client::Config::load(dir.path().join("client.json"))?;
    round_trip(client_cfg, "/users/resolver.example.com/e2e", Value::I64(99))
        .await?;
    Ok(())
}

/// Revocation, end-to-end at the enforcement choke point: a client
/// whose certificate lands on the CRL is refused by the resolver — and
/// the CRL takes effect on a *running* resolver (the CRL-watching
/// acceptor reloads when `crl.pem` appears beside the trust bundle; no
/// restart).
///
/// Shape: same TLS install as `resolver_template_tls_round_trip`, then
/// (1) baseline round-trip succeeds; (2) the client's serial is revoked
/// in the CA index, the CRL is signed and dropped beside the resolver's
/// trusted bundle; (3) a fresh publisher/subscriber pair — forced into
/// new TLS handshakes — can no longer get anything registered, observed
/// as a bounded timeout where the baseline succeeded in milliseconds.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn revoked_certificate_is_refused_by_a_running_resolver() -> Result<()> {
    use netidx_conf::ca_index;
    let _ = env_logger::try_init();
    ensure_xdg_redirect();
    let dir = TempDir::new()?;
    let port = pick_port();

    let ca_dir = dir.path().join("ca");
    let ca = ca::Ca::init(
        &ca::CaParams {
            directory: ca_dir.clone(),
            subject: ca::Subject::cn("e2e-revocation-ca"),
            san: vec![],
            key_bits: 2048,
            validity_days: 30,
        },
        None,
    )?;
    // A SAN distinct from every other test's: identities install at the
    // canonical (shared, XDG-redirected) tls dir keyed by SAN, and the
    // TLS tests run concurrently — two tests writing the same identity
    // dir from different CAs race each other into handshake failures.
    let resolver_id_src = dir.path().join("resolver-id-src");
    let resolver_issued = ca.issue(&ca::IssueParams {
        subject: ca::Subject::cn("resolver.revoked.example"),
        san: vec![ca::SanEntry::Dns("resolver.revoked.example".into())],
        key_bits: 2048,
        validity_days: 30,
        out_dir: resolver_id_src.clone(),
        password: None,
    })?;

    let id_map_sock = dir.path().join("id-map.sock");
    let id_map_json = dir.path().join("id-map.json");
    let mut params = anon_params(&dir, port);
    params.auth = AuthChoice::Tls {
        name: ArcStr::from("resolver.revoked.example"),
        certificate: resolver_issued.certificate.clone(),
        private_key: resolver_issued.private_key.clone(),
        trusted: ca_dir.join("certificate.pem"),
        askpass: None,
    };
    params.with_perms_file = true;
    params.perms_path = Some(dir.path().join("perms.json"));
    params.with_id_map = true;
    params.id_map_socket = Some(id_map_sock.clone());
    params.id_map_path = Some(id_map_json.clone());
    let rt = template::resolver::resolver(&params)?;
    rt.apply()?;

    let mut map = id_map_engine::empty();
    id_map_engine::upsert_identity(&mut map, "resolver.revoked.example", 1000, "users", &[])?;
    id_map_engine::save(&id_map_json, &map)?;
    let _id_map_daemon =
        IdMapServer::start(IdMapParams::new(id_map_sock, id_map_json)).await?;

    let resolver_cfg = cfg_resolver::Config::load(dir.path().join("resolver.json"))?;
    let _server = resolver_server::Server::new(resolver_cfg, false, 0).await?;

    // 1. Baseline: the certificate works.
    let client_cfg = cfg_client::Config::load(dir.path().join("client.json"))?;
    round_trip(client_cfg.clone(), "/users/resolver.revoked.example/pre", Value::I64(1))
        .await?;

    // 2. Revoke the client identity's serial (the index recorded it at
    //    issuance) and sign + install the CRL beside the resolver's
    //    trusted bundle — the convention the acceptor watches. The
    //    resolver keeps running throughout.
    let live = ca_index::live_for_name(&ca_dir, "resolver.revoked.example")?;
    assert_eq!(live.len(), 1, "issuance index should hold the one client cert");
    ca_index::append(
        &ca_dir,
        &ca_index::Event::Revoked(ca_index::Revocation {
            serial: live[0].cert.serial,
            revoked_unix: ca_index::now_unix(),
            reason: "e2e test".into(),
        }),
    )?;
    let ca_key = std::fs::read(ca_dir.join("private.key"))?;
    ca_index::write_crl(&ca_dir, &ca_key)?;
    let rcfg = netidx_conf::resolver::ResolverConfig::load(dir.path().join("resolver.json"))?;
    let mut installed = false;
    for member in &rcfg.0.member_servers {
        if let cfg_resolver::file::Auth::Tls { trusted, .. } = &member.auth {
            let dest = std::path::Path::new(trusted.as_str()).with_file_name("crl.pem");
            std::fs::copy(ca_index::crl_path(&ca_dir), &dest)?;
            installed = true;
        }
    }
    assert!(installed, "resolver config should carry a TLS trusted path");

    // 3. A fresh publisher must fail to register: its TLS handshake is
    //    now refused at the resolver. The publisher layer retries
    //    forever by design, so refusal manifests as a bounded timeout
    //    where the baseline took milliseconds.
    let denied = tokio::time::timeout(
        Duration::from_secs(15),
        round_trip(client_cfg, "/users/resolver.revoked.example/post", Value::I64(2)),
    )
    .await;
    match denied {
        Err(_elapsed) => (), // timed out: never registered — revoked
        Ok(Err(_)) => (),    // or failed outright — also revoked
        Ok(Ok(())) => panic!("revoked certificate completed a round trip"),
    }
    Ok(())
}

/// Publisher-template end-to-end: stand up an in-process anonymous
/// resolver (built directly, NOT via the resolver template — we
/// want to isolate this test to the *publisher* template's output),
/// then render the publisher template pointing at that resolver's
/// addr. Load the resulting client.json and round-trip a value.
///
/// What this catches that pure render tests can't:
/// - `default_bind_config` defaults — when the publisher's bind
///   lands on the wrong interface (e.g. NIC subnet vs loopback) the
///   resolver registration would fail with EADDRNOTAVAIL or a
///   loopback-mixing rejection.
/// - addr-from-CLI flowing through ReferralAuth correctly.
/// - `default_auth` selection across the CLI-default path.
#[tokio::test(flavor = "multi_thread")]
async fn publisher_template_anonymous_round_trip() -> Result<()> {
    let _ = env_logger::try_init();
    ensure_xdg_redirect();
    let dir = TempDir::new()?;

    // Bare anonymous resolver bound to 127.0.0.1:0 (OS picks the
    // port; we read it back via `local_addr`). Same pattern netidx's
    // own `slow_consumer` test uses — gives us a free, working
    // resolver to point the publisher template at.
    let bare_resolver = {
        let cfg = cfg_resolver::file::ConfigBuilder::default()
            .member_servers(vec![cfg_resolver::file::MemberServerBuilder::default()
                .auth(cfg_resolver::file::Auth::Anonymous)
                .addr("127.0.0.1:0".parse()?)
                .bind_addr("127.0.0.1".parse()?)
                .build()?])
            .build()?;
        let cfg = cfg_resolver::Config::from_file(cfg)?;
        resolver_server::Server::new(cfg, false, 0).await?
    };
    let resolver_addr = *bare_resolver.local_addr();

    let params = PublisherParams {
        addrs: vec![(resolver_addr, ReferralAuth::Anonymous)],
        default_auth: None,
        tls_identities: vec![],
        base: ArcStr::from("/"),
        config_path: Some(dir.path().join("client.json")),
        // The publisher template's CLI fills this from
        // `default_advertised_ip()/<prefix>`; for an in-process test
        // pointing at a loopback resolver, the right value is
        // explicitly `local` (BindCfg::Local = 127.0.0.1). Pinning
        // it here mirrors what the CLI would derive when the
        // operator passes `--bind local`.
        default_bind_config: Some("local".to_string()),
    };
    let rt = template::publisher::publisher(&params)?;
    rt.apply()?;

    let client_cfg = cfg_client::Config::load(dir.path().join("client.json"))?;
    round_trip(client_cfg, "/e2e/publisher-anon", Value::U64(123)).await?;
    Ok(())
}
