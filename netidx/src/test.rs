#[cfg(unix)]
mod kdc;

mod resolver {
    use crate::{
        config::Config as ClientConfig,
        path::Path,
        protocol::glob::{Glob, GlobSet},
        publisher::PublishFlags,
        resolver_client::{
            ChangeTracker, DesiredAuth, PublisherKey, ResolverRead, ResolverWrite,
        },
        resolver_server::{Server, config::Config as ServerConfig},
    };
    use arcstr::literal;
    use netidx_netproto::resolver::{PublisherPriority, TargetAuth};
    use rand::{RngExt, rng};
    use std::{iter, net::SocketAddr, time::Duration};
    use tokio::time;

    fn p(p: &'static str) -> Path {
        Path::from(p)
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn duplicate_local_server_does_not_replace_live_auth_socket() {
        use crate::{os::local_auth::AuthClient, resolver_server::config::Config};
        use std::net::TcpListener;

        let dir = tempfile::tempdir().unwrap();
        let auth_socket = dir.path().join("auth.sock");
        // The server has to be started twice on one address, so it can't bind
        // port 0. Reserving a port by binding and dropping it races with
        // everything else in the suite doing the same, so take the first one
        // the server can actually claim. The bind happens before the auth
        // socket is created, so a lost race leaves nothing behind.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let (cfg, server) = loop {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            drop(listener);
            let cfg = Config::parse(&format!(
                r#"{{
                "parent": null,
                "children": [],
                "member_servers": [{{
                    "addr": "{addr}",
                    "bind_addr": "127.0.0.1",
                    "auth": {{"Local": "{}"}}
                }}],
                "perms": {{}}
            }}"#,
                auth_socket.display()
            ))
            .unwrap();
            match Server::new(cfg.clone(), false, 0).await {
                Ok(server) => break (cfg, server),
                Err(e) => assert!(
                    std::time::Instant::now() < deadline,
                    "could not start the first server: {e}"
                ),
            }
        };

        AuthClient::token(auth_socket.to_str().unwrap()).await.unwrap();
        assert!(Server::new(cfg, false, 0).await.is_err());
        AuthClient::token(auth_socket.to_str().unwrap()).await.unwrap();

        drop(server);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn publish_resolve_simple() {
        let _ = env_logger::try_init();
        let server_cfg = ServerConfig::load("../cfg/simple-server.json")
            .expect("load simple server config");
        let mut client_cfg = ClientConfig::load("../cfg/simple-client.json")
            .expect("load simple client config");
        let server = Server::new(server_cfg, false, 0).await.expect("start server");
        client_cfg.addrs[0].0 = *server.local_addr();
        client_cfg.detach();
        let paddr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let w = ResolverWrite::new(
            client_cfg.clone(),
            DesiredAuth::Anonymous,
            paddr,
            PublisherPriority::Normal,
        )
        .unwrap();
        let r = ResolverRead::new(client_cfg, DesiredAuth::Anonymous);
        let paths = vec![p("/foo/bar"), p("/foo/baz"), p("/app/v0"), p("/app/v1")];
        let flags = Some(PublishFlags::USE_EXISTING.bits());
        w.publish_with_flags(paths.iter().map(|p| (p.clone(), flags))).await.unwrap();
        let (publishers, mut resolved) = r.resolve(paths.clone()).await.unwrap();
        for r in resolved.drain(..) {
            let r = r.unwrap();
            assert_eq!(r.publishers.len(), 1);
            let key = PublisherKey::new(r.resolver, r.publishers[0].id);
            let pb = publishers.get(&key).unwrap();
            assert_eq!(pb.addr, paddr);
        }
        let mut l = r.list(p("/")).await.unwrap();
        l.sort();
        assert_eq!(&**l, &[p("/app"), p("/foo")]);
        let mut l = r.list(p("/foo")).await.unwrap();
        l.sort();
        assert_eq!(&**l, &[p("/foo/bar"), p("/foo/baz")]);
        let mut l = r.list(p("/app")).await.unwrap();
        l.sort();
        assert_eq!(&**l, &[p("/app/v0"), p("/app/v1")]);
        drop(server)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn publish_default() {
        let _ = env_logger::try_init();
        let server_cfg = ServerConfig::load("../cfg/simple-server.json")
            .expect("load simple server config");
        let mut client_cfg = ClientConfig::load("../cfg/simple-client.json")
            .expect("load simple client config");
        let server = Server::new(server_cfg, false, 0).await.expect("start server");
        client_cfg.addrs[0].0 = *server.local_addr();
        client_cfg.detach();
        let paddr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let w = ResolverWrite::new(
            client_cfg.clone(),
            DesiredAuth::Anonymous,
            paddr,
            PublisherPriority::Normal,
        )
        .unwrap();
        let r = ResolverRead::new(client_cfg, DesiredAuth::Anonymous);
        let defaults = [
            p("/default"),
            p("/default0"),
            p("/default1"),
            p("/default2"),
            p("/default3"),
            p("/default4"),
            p("/default5"),
            p("/default6"),
            p("/default7"),
            p("/default8"),
        ];
        w.publish_default(defaults.iter().cloned()).await.unwrap();
        let paths = vec![p("/default/foo/bar"), p("/default/foo/baz")];
        let (publishers, mut resolved) = r.resolve(paths.clone()).await.unwrap();
        for r in resolved.drain(..) {
            let r = r.unwrap();
            assert_eq!(r.publishers.len(), 1);
            let key = PublisherKey::new(r.resolver, r.publishers[0].id);
            let pb = publishers.get(&key).unwrap();
            assert_eq!(pb.addr, paddr);
        }
        let l = r.list(p("/")).await.unwrap();
        assert_eq!(&**l, &defaults);
        w.clear().await.unwrap();
        let (_, mut resolved) = r.resolve(paths.clone()).await.unwrap();
        for r in resolved.drain(..) {
            assert_eq!(r.unwrap().publishers.len(), 0);
        }
        let l = r.list(p("/")).await.unwrap();
        assert_eq!(&**l, &[]);
        drop(server)
    }

    struct Ctx {
        _local: Server,
        _root: (Server, Server),
        _huge0: (Server, Server),
        _huge1: (Server, Server),
        _huge1_sub: (Server, Server),
        cfg_local: ClientConfig,
        cfg_root: ClientConfig,
        cfg_huge0: ClientConfig,
        cfg_huge1: ClientConfig,
        cfg_huge1_sub: ClientConfig,
    }

    impl Ctx {
        fn random_server(&self) -> ClientConfig {
            match rng().random_range(0. ..=1.) {
                n if n >= 0. && n <= 0.20 => self.cfg_root.clone(),
                n if n > 0.20 && n <= 0.40 => self.cfg_huge0.clone(),
                n if n > 0.40 && n <= 0.60 => self.cfg_huge1.clone(),
                n if n > 0.60 && n <= 0.80 => self.cfg_huge1_sub.clone(),
                _ => self.cfg_local.clone(),
            }
        }

        async fn new() -> Ctx {
            let server_cfg_local = ServerConfig::load("../cfg/complex-local-server.json")
                .expect("local server config");
            let cfg_local = ClientConfig::load("../cfg/complex-local-client.json")
                .expect("local client config");
            let server_cfg_root = ServerConfig::load("../cfg/complex-root-server.json")
                .expect("root server config");
            let cfg_root = ClientConfig::load("../cfg/complex-root-client.json")
                .expect("root client config");
            let server_cfg_huge0 = ServerConfig::load("../cfg/complex-huge0-server.json")
                .expect("huge0 server config");
            let cfg_huge0 = ClientConfig::load("../cfg/complex-huge0-client.json")
                .expect("huge0 client config");
            let server_cfg_huge1 = ServerConfig::load("../cfg/complex-huge1-server.json")
                .expect("huge1 server config");
            let cfg_huge1 = ClientConfig::load("../cfg/complex-huge1-client.json")
                .expect("huge1 client config");
            let server_cfg_huge1_sub =
                ServerConfig::load("../cfg/complex-huge1-sub-server.json")
                    .expect("huge1 sub server config");
            let cfg_huge1_sub =
                ClientConfig::load("../cfg/complex-huge1-sub-client.json")
                    .expect("huge1 sub client config");
            let server_local =
                Server::new(server_cfg_local, false, 0).await.expect("local server");
            let server0_root = Server::new(server_cfg_root.clone(), false, 0)
                .await
                .expect("root server 0");
            let server1_root =
                Server::new(server_cfg_root, false, 1).await.expect("root server 1");
            let server0_huge0 = Server::new(server_cfg_huge0.clone(), false, 0)
                .await
                .expect("huge0 server0");
            let server1_huge0 =
                Server::new(server_cfg_huge0, false, 1).await.expect("huge0 server1");
            let server0_huge1 = Server::new(server_cfg_huge1.clone(), false, 0)
                .await
                .expect("huge1 server0");
            let server1_huge1 =
                Server::new(server_cfg_huge1, false, 1).await.expect("huge1 server0");
            let server0_huge1_sub = Server::new(server_cfg_huge1_sub.clone(), false, 0)
                .await
                .expect("huge1 sub server0");
            let server1_huge1_sub = Server::new(server_cfg_huge1_sub, false, 1)
                .await
                .expect("huge1 sub server0");
            Ctx {
                _local: server_local,
                _root: (server0_root, server1_root),
                _huge0: (server0_huge0, server1_huge0),
                _huge1: (server0_huge1, server1_huge1),
                _huge1_sub: (server0_huge1_sub, server1_huge1_sub),
                cfg_local,
                cfg_root,
                cfg_huge0,
                cfg_huge1,
                cfg_huge1_sub,
            }
        }
    }

    async fn check_list(local: bool, r: &ResolverRead) {
        let mut l = r.list(p("/")).await.unwrap();
        l.sort();
        if local {
            assert_eq!(&*l, &[p("/app"), p("/local"), p("/tmp")]);
        } else {
            assert_eq!(&*l, &[p("/app"), p("/tmp")]);
        }
        if local {
            let mut l = r.list(p("/local")).await.unwrap();
            l.sort();
            assert_eq!(&*l, &[p("/local/bar"), p("/local/foo")])
        }
        let mut l = r.list(p("/tmp")).await.unwrap();
        l.sort();
        assert_eq!(&*l, &[p("/tmp/x"), p("/tmp/y"), p("/tmp/z")]);
        let mut l = r.list(p("/app")).await.unwrap();
        l.sort();
        assert_eq!(&*l, &[p("/app/huge0"), p("/app/huge1")]);
        let mut l = r.list(p("/app/huge0")).await.unwrap();
        l.sort();
        assert_eq!(&*l, &[p("/app/huge0/x"), p("/app/huge0/y"), p("/app/huge0/z")]);
        let mut l = r.list(p("/app/huge1")).await.unwrap();
        l.sort();
        assert_eq!(
            &*l,
            &[
                p("/app/huge1/sub"),
                p("/app/huge1/x"),
                p("/app/huge1/y"),
                p("/app/huge1/z")
            ]
        );
        let mut l = r.list(p("/app/huge1/sub")).await.unwrap();
        l.sort();
        assert_eq!(
            &*l,
            &[p("/app/huge1/sub/x"), p("/app/huge1/sub/y"), p("/app/huge1/sub/z")]
        );
        let pat = Glob::new(literal!("/app/huge*/*")).unwrap();
        let pset = GlobSet::new(true, iter::once(pat)).unwrap();
        let mut l = Vec::new();
        for mut b in r.list_matching(&pset).await.unwrap().drain(..) {
            l.extend(b.drain(..));
        }
        l.sort();
        assert_eq!(
            &*l,
            &[
                p("/app/huge0/x"),
                p("/app/huge0/y"),
                p("/app/huge0/z"),
                p("/app/huge1/x"),
                p("/app/huge1/y"),
                p("/app/huge1/z")
            ]
        );
    }

    async fn check_resolve(
        ctx: &Ctx,
        r: &ResolverRead,
        paths: &[Path],
        addrs: &[SocketAddr],
    ) {
        let (publishers, mut answer) = r.resolve(paths.iter().cloned()).await.unwrap();
        let mut i = 0;
        for (p, r) in paths.iter().zip(answer.drain(..)) {
            let r = r.unwrap();
            let mut r_addrs = r
                .publishers
                .iter()
                .map(|pr| publishers[&PublisherKey::new(r.resolver, pr.id)].addr)
                .collect::<Vec<_>>();
            r_addrs.sort();
            assert_eq!(r_addrs.len(), addrs.len());
            assert_eq!(r_addrs, addrs);
            assert!(publishers.values().all(|p| p.target_auth == TargetAuth::Anonymous));
            match p.as_ref() {
                "/tmp/x" | "/tmp/y" | "/tmp/z" => assert!(
                    r.resolver == ctx.cfg_root.addrs[0].0
                        || r.resolver == ctx.cfg_root.addrs[1].0
                ),
                "/app/huge0/x" | "/app/huge0/y" | "/app/huge0/z" => assert!(
                    r.resolver == ctx.cfg_huge0.addrs[0].0
                        || r.resolver == ctx.cfg_huge0.addrs[1].0
                ),
                "/app/huge1/x" | "/app/huge1/y" | "/app/huge1/z" => assert!(
                    r.resolver == ctx.cfg_huge1.addrs[0].0
                        || r.resolver == ctx.cfg_huge1.addrs[1].0
                ),
                "/app/huge1/sub/x" | "/app/huge1/sub/y" | "/app/huge1/sub/z" => assert!(
                    r.resolver == ctx.cfg_huge1_sub.addrs[0].0
                        || r.resolver == ctx.cfg_huge1_sub.addrs[1].0
                ),
                "/local/foo" | "/local/bar" => {
                    assert_eq!(r.resolver, ctx.cfg_local.addrs[0].0)
                }
                p => unreachable!("unexpected path {}", p),
            }
            i += 1
        }
        assert_eq!(i, paths.len());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn publish_resolve_complex() {
        let _ = env_logger::try_init();
        let ctx = Ctx::new().await;
        let waddrs =
            vec!["127.0.0.1:5543".parse().unwrap(), "127.0.0.1:5544".parse().unwrap()];
        let paths = [
            "/tmp/x",
            "/tmp/y",
            "/tmp/z",
            "/app/huge0/x",
            "/app/huge0/y",
            "/app/huge0/z",
            "/app/huge1/x",
            "/app/huge1/y",
            "/app/huge1/z",
            "/app/huge1/sub/x",
            "/app/huge1/sub/y",
            "/app/huge1/sub/z",
        ]
        .iter()
        .map(|r| Path::from(*r))
        .collect::<Vec<_>>();
        let local_paths = ["/local/foo", "/local/bar"]
            .iter()
            .map(|r| Path::from(*r))
            .collect::<Vec<_>>();
        let mut ct_root = ChangeTracker::new(Path::from("/"));
        let mut ct_app = ChangeTracker::new(Path::from("/app"));
        let r_root = ResolverRead::new(ctx.cfg_root.clone(), DesiredAuth::Anonymous);
        assert!(r_root.check_changed(&mut ct_root).await.unwrap());
        assert!(r_root.check_changed(&mut ct_app).await.unwrap());
        let w0 = ResolverWrite::new(
            ctx.random_server(),
            DesiredAuth::Anonymous,
            waddrs[0],
            PublisherPriority::Normal,
        )
        .unwrap();
        let w1 = ResolverWrite::new(
            ctx.random_server(),
            DesiredAuth::Anonymous,
            waddrs[1],
            PublisherPriority::Normal,
        )
        .unwrap();
        w0.publish(paths.iter().cloned()).await.unwrap();
        let wl = ResolverWrite::new(
            ctx.cfg_local.clone(),
            DesiredAuth::Anonymous,
            waddrs[0],
            PublisherPriority::Normal,
        )
        .unwrap();
        wl.publish(local_paths.iter().cloned()).await.unwrap();
        time::sleep(Duration::from_millis(1000)).await;
        assert!(r_root.check_changed(&mut ct_root).await.unwrap());
        assert!(!r_root.check_changed(&mut ct_root).await.unwrap());
        assert!(r_root.check_changed(&mut ct_app).await.unwrap());
        assert!(!r_root.check_changed(&mut ct_app).await.unwrap());
        check_list(false, &r_root).await;
        check_resolve(&ctx, &r_root, &paths, &[waddrs[0]][..]).await;
        w1.publish(paths.iter().cloned()).await.unwrap();
        time::sleep(Duration::from_millis(1000)).await;
        assert!(r_root.check_changed(&mut ct_root).await.unwrap());
        assert!(!r_root.check_changed(&mut ct_root).await.unwrap());
        assert!(r_root.check_changed(&mut ct_app).await.unwrap());
        assert!(!r_root.check_changed(&mut ct_app).await.unwrap());
        // CR estokes: it is not strictly guaranteed that both servers
        // in the cluster will have finished publishing all the paths
        // when this method returns, as such this test could fail
        // spuriously.
        check_list(false, &r_root).await;
        check_resolve(&ctx, &r_root, &paths, &waddrs).await;
        let r_local = ResolverRead::new(ctx.cfg_local.clone(), DesiredAuth::Anonymous);
        check_list(true, &r_local).await;
        check_resolve(&ctx, &r_local, &paths, &waddrs).await;
        check_resolve(&ctx, &r_local, &local_paths, &[waddrs[0]]).await;
        let r_huge0 = ResolverRead::new(ctx.cfg_huge0.clone(), DesiredAuth::Anonymous);
        check_list(false, &r_huge0).await;
        check_resolve(&ctx, &r_huge0, &paths, &waddrs).await;
        let r_huge1 = ResolverRead::new(ctx.cfg_huge1.clone(), DesiredAuth::Anonymous);
        check_list(false, &r_huge1).await;
        check_resolve(&ctx, &r_huge1, &paths, &waddrs).await;
        let r_huge1_sub =
            ResolverRead::new(ctx.cfg_huge1.clone(), DesiredAuth::Anonymous);
        check_list(false, &r_huge1_sub).await;
        check_resolve(&ctx, &r_huge1_sub, &paths, &waddrs).await;
        assert!(!r_root.check_changed(&mut ct_root).await.unwrap());
        assert!(!r_root.check_changed(&mut ct_app).await.unwrap());
        w0.unpublish(paths.iter().cloned()).await.unwrap();
        assert!(r_root.check_changed(&mut ct_root).await.unwrap());
        assert!(!r_root.check_changed(&mut ct_root).await.unwrap());
        assert!(r_root.check_changed(&mut ct_app).await.unwrap());
        assert!(!r_root.check_changed(&mut ct_app).await.unwrap());
        check_list(false, &r_root).await;
        check_resolve(&ctx, &r_root, &paths, &[waddrs[1]][..]).await;
        check_list(false, &r_huge1_sub).await;
        check_resolve(&ctx, &r_huge1_sub, &paths, &[waddrs[1]][..]).await;
        check_list(false, &r_huge1).await;
        check_resolve(&ctx, &r_huge1, &paths, &[waddrs[1]][..]).await;
        check_list(false, &r_huge0).await;
        check_resolve(&ctx, &r_huge0, &paths, &[waddrs[1]][..]).await;
    }
}

/// What a publisher owes each member of its resolver cluster, and what it
/// must do to make good on that after one of them has been out of touch.
///
/// A resolver holds a publisher's records only as long as the publisher keeps
/// talking to it, and it is the *publisher* that knows the truth. So every
/// write connection has to be able to reconstruct the whole picture on
/// reconnect. These tests pin that contract.
mod republish {
    use crate::{
        config::{
            Config as ClientConfig, DefaultAuthMech, file as cfile, watch::POLL_INTERVAL,
        },
        path::Path,
        resolver_client::{DesiredAuth, ResolverRead, ResolverWrite},
        resolver_server::{
            Server,
            config::{Config as ServerConfig, ReadGate, file as sfile},
        },
    };
    use netidx_netproto::resolver::PublisherPriority;
    use std::{
        collections::BTreeSet,
        iter,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
        time::Duration,
    };
    use tokio::{
        net::{TcpListener, TcpStream},
        sync::watch,
        task, time,
    };

    fn p(s: &str) -> Path {
        Path::from(String::from(s))
    }

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    /// Take a port the OS says is free and immediately hand it back. Racy in
    /// principle, but a member that restarts has to come back on the address
    /// its clients already know, so binding port 0 is not an option.
    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }

    fn members(ports: &[u16], writer_ttl: u64) -> Vec<sfile::MemberServer> {
        ports
            .iter()
            .map(|port| {
                sfile::MemberServerBuilder::default()
                    .addr(addr(*port))
                    .bind_addr(IpAddr::V4(Ipv4Addr::LOCALHOST))
                    .auth(sfile::Auth::Anonymous)
                    .writer_ttl(writer_ttl)
                    .build()
                    .unwrap()
            })
            .collect()
    }

    fn server_cfg_file(ports: &[u16], writer_ttl: u64) -> sfile::Config {
        sfile::ConfigBuilder::default()
            .member_servers(members(ports, writer_ttl))
            .build()
            .unwrap()
    }

    /// The same cluster, with the *second* member's read gate set.
    fn gated_cfg_file(ports: &[u16], gate: ReadGate) -> sfile::Config {
        let mut members = members(ports, 120);
        members[1].read_gated = gate;
        sfile::ConfigBuilder::default().member_servers(members).build().unwrap()
    }

    /// A resolver at `/` that refers everything under `/eu` to `eu`. The ttl
    /// is what makes an edit reach a client that has already been told where
    /// `/eu` lives — without one, referrals are cached forever.
    fn root_cfg_file(root: u16, eu: &[u16]) -> sfile::Config {
        sfile::ConfigBuilder::default()
            .member_servers(members(&[root], 120))
            .children(vec![sfile::Referral {
                path: arcstr::ArcStr::from("/eu"),
                ttl: Some(1),
                addrs: eu
                    .iter()
                    .map(|port| (addr(*port), sfile::RefAuth::Anonymous))
                    .collect(),
            }])
            .build()
            .unwrap()
    }

    fn client_cfg_file(addrs: &[SocketAddr]) -> cfile::Config {
        cfile::ConfigBuilder::default()
            .addrs(addrs.iter().map(|a| (*a, cfile::Auth::Anonymous)).collect::<Vec<_>>())
            .default_auth(DefaultAuthMech::Anonymous)
            .build()
            .unwrap()
    }

    fn client_cfg(addrs: &[SocketAddr]) -> ClientConfig {
        ClientConfig::from_file(client_cfg_file(addrs)).unwrap()
    }

    /// Write a client config the way the admin agent does — to a temporary
    /// file, then renamed into place — so a reader can never see a partial
    /// one.
    fn write_client_cfg(path: &std::path::Path, addrs: &[SocketAddr]) {
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, serde_json::to_string(&client_cfg_file(addrs)).unwrap())
            .unwrap();
        std::fs::rename(&tmp, path).unwrap();
    }

    /// Start member `id`, retrying while a previous incarnation's listener is
    /// still winding down — `Server`'s stop is signalled from `Drop` and
    /// completes on another task — or while another test is holding the port
    /// we reserved.
    async fn start(cfg: sfile::Config, id: usize) -> Server {
        let cfg = ServerConfig::from_file(cfg).unwrap();
        let deadline = time::Instant::now() + Duration::from_secs(10);
        loop {
            match Server::new(cfg.clone(), false, id).await {
                Ok(s) => break s,
                Err(e) => {
                    if time::Instant::now() >= deadline {
                        panic!("member {id} would not start: {e}")
                    }
                    time::sleep(Duration::from_millis(50)).await
                }
            }
        }
    }

    async fn start_member(ports: &[u16], writer_ttl: u64, id: usize) -> Server {
        start(server_cfg_file(ports, writer_ttl), id).await
    }

    fn writer(addrs: &[SocketAddr]) -> ResolverWrite {
        ResolverWrite::new(
            client_cfg(addrs),
            DesiredAuth::Anonymous,
            "127.0.0.1:1".parse().unwrap(),
            PublisherPriority::Normal,
        )
        .unwrap()
    }

    /// A reader pointed at exactly one member, so a test can ask what that
    /// member individually believes rather than what the cluster answers.
    fn reader(addr: SocketAddr) -> ResolverRead {
        ResolverRead::new(client_cfg(&[addr]), DesiredAuth::Anonymous)
    }

    async fn published(r: &ResolverRead, paths: &[Path]) -> BTreeSet<Path> {
        match r.resolve(paths.iter().cloned()).await {
            Err(_) => BTreeSet::new(),
            Ok((_, mut res)) => paths
                .iter()
                .cloned()
                .zip(res.drain(..))
                .filter(|(_, r)| {
                    r.as_ref().map(|r| r.publishers.len() > 0).unwrap_or(false)
                })
                .map(|(p, _)| p)
                .collect(),
        }
    }

    /// Wait until `r` reports exactly `want` among `probe`, then return. The
    /// set is compared exactly in both directions so a stale record left
    /// behind fails as loudly as a missing one.
    async fn converges_to(who: &str, r: &ResolverRead, probe: &[Path], want: &[Path]) {
        let want = want.iter().cloned().collect::<BTreeSet<Path>>();
        let deadline = time::Instant::now() + Duration::from_secs(20);
        loop {
            let have = published(r, probe).await;
            if have == want {
                return;
            }
            if time::Instant::now() >= deadline {
                let missing = want.difference(&have).collect::<Vec<_>>();
                let stale = have.difference(&want).collect::<Vec<_>>();
                panic!("{who} never converged; missing {missing:?}, stale {stale:?}")
            }
            time::sleep(Duration::from_millis(100)).await
        }
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Link {
        Up,
        /// New connections are refused and live ones are killed, but the
        /// resolver behind the proxy keeps everything it knows about us.
        Cut,
        /// Connections are accepted and then ignored, so the client stalls in
        /// its handshake for a hello timeout instead of failing fast — long
        /// enough to fall behind everything else the publisher is doing.
        BlackHole,
    }

    /// A pass-through TCP proxy in front of one member. Dropping a `Server`
    /// also drops everything it knows; this cuts the *link* instead, which is
    /// the only way to reach the reconnect paths that have to reconcile
    /// against a resolver still holding our old records.
    struct Proxy {
        addr: SocketAddr,
        link: watch::Sender<Link>,
        /// Held by every forwarding task, so `set` can tell when they are all
        /// gone. Without this a test that cuts the link and immediately sends
        /// something can have it arrive anyway, through a connection that has
        /// been told to die but hasn't yet.
        forwarding: Arc<()>,
    }

    impl Proxy {
        async fn new(target: SocketAddr) -> Proxy {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (tx, rx) = watch::channel(Link::Up);
            let forwarding = Arc::new(());
            // Weak, so the only strong references are the `Proxy` itself and
            // the tasks actually carrying traffic. Otherwise the accept loop's
            // own clone would keep the count above one forever.
            let token = Arc::downgrade(&forwarding);
            task::spawn(async move {
                while let Ok((client, _)) = listener.accept().await {
                    let mut link = rx.clone();
                    let state = *link.borrow_and_update();
                    match state {
                        Link::Cut => (),
                        Link::BlackHole => {
                            task::spawn(async move {
                                let _client = client;
                                let _ = link.wait_for(|l| *l != Link::BlackHole).await;
                            });
                        }
                        Link::Up => {
                            let Some(token) = token.upgrade() else { break };
                            task::spawn(async move {
                                let _token = token;
                                let Ok(server) = TcpStream::connect(target).await else {
                                    return;
                                };
                                let (mut cr, mut cw) = client.into_split();
                                let (mut sr, mut sw) = server.into_split();
                                tokio::select! {
                                    _ = tokio::io::copy(&mut cr, &mut sw) => (),
                                    _ = tokio::io::copy(&mut sr, &mut cw) => (),
                                    _ = link.wait_for(|l| *l != Link::Up) => (),
                                }
                            });
                        }
                    }
                }
            });
            Proxy { addr, link: tx, forwarding }
        }

        /// Change the link state and, when that means "stop carrying traffic",
        /// wait until it really has stopped. New connections in `Cut` are
        /// dropped and in `BlackHole` never forward, so neither can keep this
        /// waiting.
        async fn set(&self, link: Link) {
            self.link.send_replace(link);
            if link != Link::Up {
                while Arc::strong_count(&self.forwarding) > 1 {
                    time::sleep(Duration::from_millis(10)).await
                }
            }
        }
    }

    /// Everything the publisher has must end up on a member that comes back,
    /// including paths published while it was unreachable. A member that
    /// missed an update has no other way to learn about it — the publisher
    /// sends deltas and nothing replays them.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_returning_member_gets_the_whole_publish_set() {
        let _ = env_logger::try_init();
        let ports = [free_port(), free_port()];
        let _a = start_member(&ports, 120, 0).await;
        let b = start_member(&ports, 120, 1).await;
        let w = writer(&[addr(ports[0]), addr(ports[1])]);
        let rb = reader(addr(ports[1]));
        let before = vec![p("/t/one"), p("/t/two"), p("/t/three")];
        w.publish(before.iter().cloned()).await.unwrap();
        converges_to("b", &rb, &before, &before).await;
        drop(b);
        // The first of these is attempted while b's connection is still
        // nominally alive, the second after it has been marked dead. Those are
        // different paths through the write connection, and both have to end
        // with the path on b once it is back.
        let during = vec![p("/t/four"), p("/t/five")];
        for path in during.iter() {
            let _ = w.publish(iter::once(path.clone())).await;
        }
        let _b = start_member(&ports, 120, 1).await;
        // Give b's connection a reason to reconnect.
        let _ = w.publish(iter::once(p("/t/six"))).await;
        let all = before
            .iter()
            .chain(during.iter())
            .cloned()
            .chain(iter::once(p("/t/six")))
            .collect::<Vec<_>>();
        converges_to("b", &rb, &all, &all).await;
    }

    /// A connection that falls far enough behind the publisher to be dropped
    /// from the broadcast has to recover the batches it never saw. It is the
    /// same requirement as the test above, reached by a different route: one
    /// slow member must not end up permanently missing paths.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_connection_that_falls_behind_recovers_what_it_missed() {
        let _ = env_logger::try_init();
        let ports = [free_port(), free_port()];
        let _a = start_member(&ports, 120, 0).await;
        let _b = start_member(&ports, 120, 1).await;
        let proxy = Proxy::new(addr(ports[1])).await;
        let w = writer(&[addr(ports[0]), proxy.addr]);
        let rb = reader(addr(ports[1]));
        let first = vec![p("/t/first")];
        w.publish(first.iter().cloned()).await.unwrap();
        converges_to("b", &rb, &first, &first).await;
        // Stall b's connection in a handshake, then run far enough ahead of it
        // that the broadcast drops what it hasn't consumed.
        proxy.set(Link::BlackHole).await;
        let many = (0..150).map(|i| p(&format!("/t/many/{i}"))).collect::<Vec<_>>();
        for path in many.iter() {
            let _ = w.publish(iter::once(path.clone())).await;
        }
        proxy.set(Link::Up).await;
        let _ = w.publish(iter::once(p("/t/last"))).await;
        let all = first
            .iter()
            .chain(many.iter())
            .cloned()
            .chain(iter::once(p("/t/last")))
            .collect::<Vec<_>>();
        converges_to("b", &rb, &all, &all).await;
    }

    /// A publisher must start using a member added to its config, with
    /// everything it already has, and without being restarted. This is what
    /// the whole config-following change is for.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_publisher_takes_up_a_member_added_to_its_config() {
        let _ = env_logger::try_init();
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("client.json");
        let ports = [free_port(), free_port()];
        let _a = start_member(&ports, 120, 0).await;
        let _b = start_member(&ports, 120, 1).await;
        write_client_cfg(&cfg_path, &[addr(ports[0])]);
        let w = ResolverWrite::new(
            ClientConfig::load(&cfg_path).unwrap(),
            DesiredAuth::Anonymous,
            "127.0.0.1:1".parse().unwrap(),
            PublisherPriority::Normal,
        )
        .unwrap();
        let ra = reader(addr(ports[0]));
        let rb = reader(addr(ports[1]));
        let paths = vec![p("/t/one"), p("/t/two"), p("/t/three")];
        w.publish(paths.iter().cloned()).await.unwrap();
        converges_to("a", &ra, &paths, &paths).await;
        // b is in the cluster but not in this publisher's config, so it must
        // have nothing — otherwise the assertion below proves nothing.
        assert!(published(&rb, &paths).await.is_empty());
        write_client_cfg(&cfg_path, &[addr(ports[0]), addr(ports[1])]);
        converges_to("b", &rb, &paths, &paths).await;
    }

    /// A subscriber must likewise start using a member added to its config,
    /// which is what lets it survive losing the one it had.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_subscriber_takes_up_a_member_added_to_its_config() {
        let _ = env_logger::try_init();
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("client.json");
        let ports = [free_port(), free_port()];
        let a = start_member(&ports, 120, 0).await;
        let _b = start_member(&ports, 120, 1).await;
        write_client_cfg(&cfg_path, &[addr(ports[0])]);
        // Publishes to both members; only the subscriber's view is under test.
        let w = writer(&[addr(ports[0]), addr(ports[1])]);
        let paths = vec![p("/t/one"), p("/t/two")];
        w.publish(paths.iter().cloned()).await.unwrap();
        let r = ResolverRead::new(
            ClientConfig::load(&cfg_path).unwrap(),
            DesiredAuth::Anonymous,
        );
        converges_to("the subscriber", &r, &paths, &paths).await;
        write_client_cfg(&cfg_path, &[addr(ports[0]), addr(ports[1])]);
        // Wait long enough for the change to have been picked up, then take
        // away the only resolver the subscriber was started with.
        time::sleep(POLL_INTERVAL * 5).await;
        drop(a);
        converges_to("the subscriber", &r, &paths, &paths).await;
    }

    /// A resolver must pick up an edited referral from its own config. This
    /// is the other half of the same problem: the administrative plane
    /// rewrites `parent` and `children` when a neighbouring cluster changes,
    /// and until now nothing read them back.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_resolver_picks_up_an_edited_child_referral() {
        let _ = env_logger::try_init();
        let root_port = free_port();
        let eu_ports = [free_port(), free_port()];
        let root = start(root_cfg_file(root_port, &[eu_ports[0]]), 0).await;
        let _eu0 = start_member(&eu_ports, 120, 0).await;
        let _eu1 = start_member(&eu_ports, 120, 1).await;
        // Published only to the member root does *not* refer to yet.
        let w = writer(&[addr(eu_ports[1])]);
        let paths = vec![p("/eu/x")];
        w.publish(paths.iter().cloned()).await.unwrap();
        let r = ResolverRead::new(client_cfg(&[addr(root_port)]), DesiredAuth::Anonymous);
        assert!(published(&r, &paths).await.is_empty());
        let not_applied = root.reload(&root_cfg_file(root_port, &[eu_ports[1]])).await;
        assert_eq!(not_applied.unwrap(), Default::default());
        converges_to("the subscriber", &r, &paths, &paths).await;
    }

    /// Where a child attaches is fixed when the store is built, so a reload
    /// has to say so rather than appear to have worked.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_added_child_is_reported_as_not_applied() {
        let _ = env_logger::try_init();
        let root_port = free_port();
        let eu_port = free_port();
        let root = start(server_cfg_file(&[root_port], 120), 0).await;
        let not_applied =
            root.reload(&root_cfg_file(root_port, &[eu_port])).await.unwrap();
        assert_eq!(not_applied.children_added, vec![p("/eu")]);
        assert!(not_applied.children_removed.is_empty());
        // ...and the other way round.
        let with_child = start(root_cfg_file(free_port(), &[eu_port]), 0).await;
        let not_applied = with_child
            .reload(&server_cfg_file(&[*with_child.local_addr()].map(|a| a.port()), 120))
            .await
            .unwrap();
        assert_eq!(not_applied.children_removed, vec![p("/eu")]);
        assert!(not_applied.children_added.is_empty());
    }

    /// A gated member keeps taking writes and stops answering reads, and
    /// clients fail over to a member that will. This is what lets a replica
    /// be filled before anyone is pointed at it, and a decommissioned one be
    /// taken out of service without being stopped.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_gated_member_takes_writes_but_not_reads() {
        let _ = env_logger::try_init();
        let ports = [free_port(), free_port()];
        let _a = start_member(&ports, 120, 0).await;
        let gated = start(gated_cfg_file(&ports, ReadGate::Yes), 1).await;
        assert!(!gated.reads_allowed());
        // The publisher talks to both, including the gated one.
        let w = writer(&[addr(ports[0]), addr(ports[1])]);
        let paths = vec![p("/t/one"), p("/t/two")];
        w.publish(paths.iter().cloned()).await.unwrap();
        // The ungated member answers.
        converges_to("a", &reader(addr(ports[0])), &paths, &paths).await;
        // The gated one took the writes but refuses to talk about them, and a
        // subscriber that knows about both still gets its answer.
        let both = ResolverRead::new(
            client_cfg(&[addr(ports[0]), addr(ports[1])]),
            DesiredAuth::Anonymous,
        );
        converges_to("a subscriber", &both, &paths, &paths).await;
        // Open the gate and the same member starts answering — with the paths
        // it has been quietly accepting all along.
        gated.reload(&gated_cfg_file(&ports, ReadGate::No)).await.unwrap();
        assert!(gated.reads_allowed());
        converges_to("the gated member", &reader(addr(ports[1])), &paths, &paths).await;
    }

    #[test]
    fn a_gate_opens_when_its_deadline_passes() {
        use chrono::{Duration as CDuration, Utc};
        assert!(ReadGate::No.is_open());
        assert!(!ReadGate::Yes.is_open());
        assert!(!ReadGate::Until(Utc::now() + CDuration::seconds(60)).is_open());
        assert!(ReadGate::Until(Utc::now() - CDuration::seconds(1)).is_open());
    }

    /// The running server keeps its gate as the millisecond scale `opens_at`
    /// collapses it to, so the sentinels standing in for `No` and `Yes` have
    /// to stay out of the range a real deadline can reach. chrono's own limits
    /// are the ones that decide it.
    #[test]
    fn no_deadline_can_be_mistaken_for_a_constant_gate() {
        use chrono::{DateTime, Utc};
        assert!(DateTime::<Utc>::MAX_UTC.timestamp_millis() < i64::MAX);
        assert!(DateTime::<Utc>::MIN_UTC.timestamp_millis() > i64::MIN);
    }

    /// A host reports one gate but may hold several member blocks. Whichever
    /// of them refuses reads for longest is the one that can turn a subscriber
    /// away, so that is the one worth reporting.
    #[test]
    fn the_strictest_gate_is_the_one_that_refuses_longest() {
        use chrono::{Duration as CDuration, Utc};
        let soon = ReadGate::Until(Utc::now() + CDuration::seconds(60));
        let later = ReadGate::Until(Utc::now() + CDuration::seconds(600));
        for (a, b, want) in [
            (ReadGate::No, ReadGate::No, ReadGate::No),
            (ReadGate::No, ReadGate::Yes, ReadGate::Yes),
            (ReadGate::No, soon, soon),
            (soon, ReadGate::Yes, ReadGate::Yes),
            (soon, later, later),
        ] {
            assert_eq!(a.strictest(b), want, "{a:?} vs {b:?}");
            assert_eq!(b.strictest(a), want, "{b:?} vs {a:?} (must commute)");
        }
    }

    /// An unpublish that could not be delivered has to be retried, not
    /// forgotten. The member still holds the record — nothing else will ever
    /// remove it before the writer ttl expires.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_undelivered_unpublish_is_retried() {
        let _ = env_logger::try_init();
        let ports = [free_port(), free_port()];
        let _a = start_member(&ports, 120, 0).await;
        let _b = start_member(&ports, 120, 1).await;
        let proxy = Proxy::new(addr(ports[1])).await;
        let w = writer(&[addr(ports[0]), proxy.addr]);
        let rb = reader(addr(ports[1]));
        let paths = vec![p("/t/one"), p("/t/two"), p("/t/three")];
        w.publish(paths.iter().cloned()).await.unwrap();
        converges_to("b", &rb, &paths, &paths).await;
        proxy.set(Link::Cut).await;
        let _ = w.unpublish(iter::once(p("/t/two"))).await;
        // Let b's write connection notice the break, and confirm the unpublish
        // really did not land — otherwise the assertion below proves nothing.
        time::sleep(Duration::from_secs(1)).await;
        assert!(published(&rb, &paths).await.contains(&p("/t/two")));
        proxy.set(Link::Up).await;
        let _ = w.publish(iter::once(p("/t/four"))).await;
        converges_to("b", &rb, &paths, &[p("/t/one"), p("/t/three")]).await;
    }

    /// Same contract for `clear`: a member that missed it must not be left
    /// holding the paths the publisher has disowned.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_undelivered_clear_is_retried() {
        let _ = env_logger::try_init();
        let ports = [free_port(), free_port()];
        let _a = start_member(&ports, 120, 0).await;
        let _b = start_member(&ports, 120, 1).await;
        let proxy = Proxy::new(addr(ports[1])).await;
        let w = writer(&[addr(ports[0]), proxy.addr]);
        let rb = reader(addr(ports[1]));
        let paths = vec![p("/t/one"), p("/t/two"), p("/t/three")];
        w.publish(paths.iter().cloned()).await.unwrap();
        converges_to("b", &rb, &paths, &paths).await;
        proxy.set(Link::Cut).await;
        let _ = w.clear().await;
        time::sleep(Duration::from_secs(1)).await;
        assert_eq!(published(&rb, &paths).await.len(), paths.len());
        proxy.set(Link::Up).await;
        let _ = w.publish(iter::once(p("/t/four"))).await;
        converges_to("b", &rb, &paths, &[]).await;
    }
}

mod publisher {
    use crate::{
        config::Config as ClientConfig,
        publisher::{
            BindCfg, DesiredAuth, Event as PEvent, PublishFlags, Publisher,
            PublisherBuilder, Val,
        },
        resolver_client::ResolverRead,
        resolver_server::{Server, config::Config as ServerConfig},
        subscriber::{Event, SubId, Subscriber, SubscriberBuilder, UpdatesFlags, Value},
    };
    use anyhow::Result;
    use arcstr::literal;
    use futures::{channel::mpsc, channel::oneshot, prelude::*, select_biased};
    use log::debug;
    use netidx_core::path::Path;
    use netidx_netproto::resolver::PublisherPriority;
    use parking_lot::Mutex;
    use poolshark::global::GPooled;
    use std::{
        iter,
        net::{IpAddr, SocketAddr},
        sync::Arc,
        time::Duration,
    };
    use tokio::{
        task::{self, JoinHandle},
        time::{self, Instant},
    };

    #[test]
    fn bindcfg() {
        let _ = env_logger::try_init();
        let addr: IpAddr = "192.168.0.0".parse().unwrap();
        let netmask: IpAddr = "255.255.0.0".parse().unwrap();
        assert_eq!(BindCfg::Match { addr, netmask }, "192.168.0.0/16".parse().unwrap());
        let addr: IpAddr = "ffff:1c00:2700:3c00::".parse().unwrap();
        let netmask: IpAddr = "ffff:ffff:ffff:ffff::".parse().unwrap();
        let bc: BindCfg = "ffff:1c00:2700:3c00::/64".parse().unwrap();
        assert_eq!(BindCfg::Match { addr, netmask }, bc);
        let addr: SocketAddr = "127.0.0.1:1234".parse().unwrap();
        assert_eq!(BindCfg::Exact(addr), "127.0.0.1:1234".parse().unwrap());
        let addr: SocketAddr = "[ffff:1c00:2700:3c00::]:1234".parse().unwrap();
        assert_eq!(BindCfg::Exact(addr), "[ffff:1c00:2700:3c00::]:1234".parse().unwrap());
        assert!("192.168.0.1".parse::<BindCfg>().is_err());
        assert!("192.168.0.1:12345/16".parse::<BindCfg>().is_err());
        assert!("192.168.0.1/8/foo".parse::<BindCfg>().is_err());
        assert!("ffff:1c00:2700:3c00::".parse::<BindCfg>().is_err());
    }

    async fn run_publisher(
        cfg: ClientConfig,
        default_destroyed: Arc<Mutex<bool>>,
        tx: oneshot::Sender<()>,
        auth: DesiredAuth,
    ) {
        let check_user = match &auth {
            DesiredAuth::Tls { .. } => true,
            _ => false,
        };
        let publisher = Publisher::new(
            cfg,
            auth,
            "127.0.0.1/32".parse().unwrap(),
            PublisherPriority::Normal,
            768,
            3,
        )
        .await
        .unwrap();
        let vp = publisher.publish("/app/v0".into(), Value::U64(0)).unwrap();
        publisher.alias(vp.id(), "/app/v1".into()).unwrap();
        let mut dfp: Option<Val> = None;
        let mut _adv: Option<Val> = None;
        let mut df = publisher.publish_default("/app/q".into()).unwrap();
        df.advertise("/app/q/adv".into()).unwrap();
        publisher.flushed().await;
        tx.send(()).unwrap();
        let (tx, mut rx) = mpsc::channel(10);
        let (tx_ev, mut rx_ev) = mpsc::unbounded();
        publisher.events(tx_ev);
        publisher.writes(vp.id(), tx);
        loop {
            select_biased! {
                e = rx_ev.select_next_some() => match e {
                    PEvent::Subscribe(_, _) | PEvent::Unsubscribe(_, _) => (),
                    PEvent::Destroyed(id) => {
                        assert!(id == dfp.unwrap().id());
                        dfp = None;
                        *default_destroyed.lock() = true;
                    }
                },
                (p, reply) = df.select_next_some() => {
                    assert!(p.starts_with("/app/q"));
                    if &*p == "/app/q/foo" {
                        let f = PublishFlags::DESTROY_ON_IDLE;
                        let p =
                            publisher.publish_with_flags(f, p, Value::Bool(true)).unwrap();
                        dfp = Some(p);
                        let _ = reply.send(());
                    } else if &*p == "/app/q/adv" {
                        _adv = Some(publisher.publish(p, Value::Bool(false)).unwrap());
                        let _ = reply.send(());
                    } else {
                        panic!("unexpected default subscription {}", p);
                    }
                },
                mut batch = rx.select_next_some() => {
                    let mut ub = publisher.start_batch();
                    for req in batch.drain(..) {
                        if check_user {
                            assert!(publisher.user(&req.client).is_some())
                        }
                        vp.update(&mut ub, req.value);
                    }
                    ub.commit(None).await;
                }
            }
        }
    }

    async fn run_subscriber(
        cfg: ClientConfig,
        default_destroyed: Arc<Mutex<bool>>,
        auth: DesiredAuth,
    ) {
        let subscriber = Subscriber::new(cfg, auth).unwrap();
        let vs =
            subscriber.subscribe_nondurable_one("/app/v0".into(), None).await.unwrap();
        // we should be able to subscribe to an alias and it should
        // behave as if we just cloned the existing
        // subscription. E.G. no extra values in the channel.
        let va =
            subscriber.subscribe_nondurable_one("/app/v1".into(), None).await.unwrap();
        let q =
            subscriber.subscribe_nondurable_one("/app/q/foo".into(), None).await.unwrap();
        assert_eq!(q.last(), Event::Update(Value::Bool(true)));
        let (_, res) =
            subscriber.resolver().resolve(iter::once("/app/q/adv".into())).await.unwrap();
        assert_eq!(res.len(), 1);
        let a =
            subscriber.subscribe_nondurable_one("/app/q/adv".into(), None).await.unwrap();
        assert_eq!(a.last(), Event::Update(Value::Bool(false)));
        drop(q);
        drop(a);
        let mut c: u64 = 0;
        let (tx, mut rx) = mpsc::channel(10);
        let flags = UpdatesFlags::BEGIN_WITH_LAST | UpdatesFlags::NO_SPURIOUS;
        vs.updates(flags, tx.clone());
        va.updates(flags, tx);
        loop {
            match rx.next().await {
                None => panic!("publisher died"),
                Some(mut batch) => {
                    for (_, v) in batch.drain(..) {
                        match v {
                            Event::Update(Value::U64(v)) => {
                                assert_eq!(c, v);
                                c += 1;
                                vs.write(Value::U64(c));
                            }
                            v => panic!("unexpected value from publisher {:?}", v),
                        }
                    }
                }
            }
            if c == 100 {
                break;
            }
        }
        if !*default_destroyed.lock() {
            panic!("default publisher value was not destroyed on idle")
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn publish_subscribe() {
        let _ = env_logger::try_init();
        let server_cfg = ServerConfig::load("../cfg/simple-server.json")
            .expect("load simple server config");
        let mut client_cfg = ClientConfig::load("../cfg/simple-client.json")
            .expect("load simple client config");
        let server = Server::new(server_cfg, false, 0).await.expect("start server");
        client_cfg.addrs[0].0 = *server.local_addr();
        client_cfg.detach();
        let default_destroyed = Arc::new(Mutex::new(false));
        let (tx, ready) = oneshot::channel();
        task::spawn(run_publisher(
            client_cfg.clone(),
            default_destroyed.clone(),
            tx,
            DesiredAuth::Anonymous,
        ));
        time::timeout(Duration::from_secs(1), ready).await.unwrap().unwrap();
        run_subscriber(client_cfg, default_destroyed, DesiredAuth::Anonymous).await;
        drop(server);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tls_publish_subscribe() {
        let _ = env_logger::try_init();
        #[cfg(unix)]
        let server_cfg = ServerConfig::load("../cfg/tls/resolver/resolver.json")
            .expect("load tls server config");
        #[cfg(windows)]
        let server_cfg = ServerConfig::load("../cfg/tls/resolver/resolver-win.json")
            .expect("load tls server config");
        let mut pub_cfg = ClientConfig::load("../cfg/tls/publisher/client.json")
            .expect("failed to load tls publisher config");
        let mut sub_cfg = ClientConfig::load("../cfg/tls/client/client.json")
            .expect("failed to load subscriber cfg");
        let default_destroyed = Arc::new(Mutex::new(false));
        let (tx, ready) = oneshot::channel();
        let server = Server::new(server_cfg, false, 0).await.expect("start server");
        pub_cfg.addrs[0].0 = *server.local_addr();
        pub_cfg.detach();
        sub_cfg.addrs[0].0 = *server.local_addr();
        sub_cfg.detach();
        task::spawn(run_publisher(
            pub_cfg.clone(),
            default_destroyed.clone(),
            tx,
            DesiredAuth::Tls { identity: None },
        ));
        time::timeout(Duration::from_secs(1), ready).await.unwrap().unwrap();
        run_subscriber(pub_cfg, default_destroyed, DesiredAuth::Tls { identity: None })
            .await;
        drop(server)
    }

    // Soak harness for cold one-shot subscribes: a fresh Subscriber (new
    // resolver read connection + new publisher connection + new TLS handshake +
    // new token each time) subscribes once to /app/v0 and is dropped. Hammered
    // in a loop, counting failures. Ignored by default (it is a soak, ~50s);
    // run with `cargo test -p netidx cold_subscribe_churn -- --ignored
    // --nocapture`. The cold path is the one that broke under renewal: every
    // fresh Subscriber builds its own empty-cache `CachedConnector`, unlike a
    // persistent (durable) subscriber that builds its connector once.
    async fn cold_subscribe_churn(
        server_cfg: ServerConfig,
        cfg: ClientConfig,
        auth: DesiredAuth,
        iters: usize,
    ) {
        let server = Server::new(server_cfg, false, 0).await.expect("start server");
        let mut cfg = cfg;
        cfg.addrs[0].0 = *server.local_addr();
        cfg.detach();
        let default_destroyed = Arc::new(Mutex::new(false));
        let (tx, ready) = oneshot::channel();
        task::spawn(run_publisher(cfg.clone(), default_destroyed, tx, auth.clone()));
        time::timeout(Duration::from_secs(5), ready).await.unwrap().unwrap();
        let mut ok = 0usize;
        let mut fail = 0usize;
        for i in 0..iters {
            let sub = Subscriber::new(cfg.clone(), auth.clone()).unwrap();
            let start = Instant::now();
            let r = sub
                .subscribe_nondurable_one("/app/v0".into(), Some(Duration::from_secs(5)))
                .await;
            match r {
                Ok(_) => ok += 1,
                Err(e) => {
                    fail += 1;
                    println!("FAIL iter={i} elapsed={:?} err={e}", start.elapsed());
                }
            }
            drop(sub);
            time::sleep(Duration::from_millis(250)).await;
        }
        println!("cold_subscribe_churn: ok={ok} fail={fail}");
        drop(server);
        assert_eq!(fail, 0, "cold subscribe stalled {fail} / {iters} times");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn cold_subscribe_churn_anon() {
        let _ = env_logger::try_init();
        let server_cfg = ServerConfig::load("../cfg/simple-server.json").unwrap();
        let cfg = ClientConfig::load("../cfg/simple-client.json").unwrap();
        cold_subscribe_churn(server_cfg, cfg, DesiredAuth::Anonymous, 200).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn cold_subscribe_churn_tls() {
        let _ = env_logger::try_init();
        #[cfg(unix)]
        let server_cfg = ServerConfig::load("../cfg/tls/resolver/resolver.json").unwrap();
        #[cfg(windows)]
        let server_cfg =
            ServerConfig::load("../cfg/tls/resolver/resolver-win.json").unwrap();
        let cfg = ClientConfig::load("../cfg/tls/client/client.json").unwrap();
        cold_subscribe_churn(server_cfg, cfg, DesiredAuth::Tls { identity: None }, 200)
            .await;
    }

    // Mint a self-contained renewal lab in a tempdir: NGEN fresh CA-signed
    // (cert, key) generations per identity plus initial "live" files, all
    // chained to the repo's checked-in test CA, with server/client/publisher
    // configs pointing at the live files. Returns None (test skips) if openssl
    // or the CA material isn't available. The CA is copied into the tempdir so
    // signing's serial file doesn't touch the source tree.
    #[cfg(unix)]
    fn mint_renew_lab() -> Option<std::path::PathBuf> {
        use std::{path::Path, process::Command};
        const NGEN: usize = 4;
        let ca_src = Path::new("../cfg/tls/ca");
        if !ca_src.join("certificate").exists()
            || !Command::new("openssl")
                .arg("version")
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false)
        {
            return None;
        }
        let dir =
            std::env::temp_dir().join(format!("netidx-renewlab-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).ok()?;
        std::fs::copy(ca_src.join("certificate"), dir.join("ca.pem")).ok()?;
        std::fs::copy(ca_src.join("private.key"), dir.join("ca.key")).ok()?;
        let ca = dir.join("ca.pem");
        let cak = dir.join("ca.key");
        let run = |args: &[&str]| {
            Command::new("openssl")
                .args(args)
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false)
        };
        let s = |p: &Path| p.to_str().unwrap().to_string();
        let mint = |id: &str, san: &str| -> bool {
            let d = dir.join(id);
            std::fs::create_dir_all(&d).ok();
            let ext = d.join("ext.cnf");
            std::fs::write(
                &ext,
                format!(
                    "basicConstraints=critical,CA:FALSE\n\
                     keyUsage=nonRepudiation,digitalSignature,keyEncipherment\n\
                     subjectAltName=DNS:{san}\n"
                ),
            )
            .ok();
            for g in 0..NGEN {
                let key = s(&d.join(format!("key{g}.pem")));
                let req = s(&d.join(format!("req{g}")));
                let cert = s(&d.join(format!("cert{g}.pem")));
                let subj = format!("/CN={san}/C=US/ST=State/L=City/O=example");
                let ok = run(&["genrsa", "-out", &key, "2048"])
                    && run(&[
                        "req", "-new", "-key", &key, "-sha512", "-out", &req, "-subj",
                        &subj,
                    ])
                    && run(&[
                        "x509",
                        "-req",
                        "-in",
                        &req,
                        "-CA",
                        &s(&ca),
                        "-CAkey",
                        &s(&cak),
                        "-CAcreateserial",
                        "-out",
                        &cert,
                        "-days",
                        "730",
                        "-sha512",
                        "-extfile",
                        &s(&ext),
                    ]);
                if !ok {
                    return false;
                }
            }
            std::fs::copy(d.join("cert0.pem"), d.join("certificate.pem")).is_ok()
                && std::fs::copy(d.join("key0.pem"), d.join("private.key")).is_ok()
        };
        if !(mint("resolver", "resolver.example.com")
            && mint("publisher", "publisher.example.com")
            && mint("client", "client.example.com"))
        {
            return None;
        }
        let live = |id: &str, f: &str| s(&dir.join(id).join(f));
        let server = format!(
            r#"{{"parent":null,"children":[],"member_servers":[{{"pid_file":"",
            "id_map_command":"../cfg/tls/id","addr":"127.0.0.1:0","max_connections":768,
            "hello_timeout":10,"reader_ttl":60,"writer_ttl":120,
            "auth":{{"Tls":{{"name":"resolver.example.com","trusted":"{ca}",
            "certificate":"{cert}","private_key":"{key}"}}}}}}],
            "perms":{{"/":{{"user":"swlpd"}}}}}}"#,
            ca = s(&ca),
            cert = live("resolver", "certificate.pem"),
            key = live("resolver", "private.key"),
        );
        let client = |id: &str| {
            format!(
                r#"{{"addrs":[["127.0.0.1:0",{{"Tls":"resolver.example.com"}}]],
                "base":"/","default_auth":"Tls","tls":{{"identities":{{"example.com":{{
                "trusted":"{ca}","certificate":"{cert}","private_key":"{key}"}}}}}}}}"#,
                ca = s(&ca),
                cert = live(id, "certificate.pem"),
                key = live(id, "private.key"),
            )
        };
        std::fs::write(dir.join("server.json"), server).ok()?;
        std::fs::write(dir.join("client.json"), client("client")).ok()?;
        std::fs::write(dir.join("publisher.json"), client("publisher")).ok()?;
        Some(dir)
    }

    // Regression test for the cold-subscribe-under-renewal failure: cold
    // subscribers churn while a background "renewer" rotates the resolver,
    // publisher, AND client certs through fresh CA-signed generations — cert
    // first, then (after a deliberately exaggerated skew window) key, exactly as
    // `renewd` installs them as two separate files. Before the `Cached::load`
    // retry fix a fresh Subscriber that built its connector during that window
    // hit a rustls KeyMismatch with nothing cached to fall back to and failed
    // the resolve outright (~6% here); persistent subscribers were immune. Must
    // hold at 0 failures.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn cold_subscribe_under_renewal() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let _ = env_logger::try_init();
        let lab = match mint_renew_lab() {
            Some(d) => d,
            None => {
                println!("skipping: openssl or test CA unavailable");
                return;
            }
        };
        let labs = lab.to_str().unwrap().to_string();
        let server_cfg = ServerConfig::load(lab.join("server.json")).unwrap();
        let mut pub_cfg = ClientConfig::load(lab.join("publisher.json")).unwrap();
        let mut sub_cfg = ClientConfig::load(lab.join("client.json")).unwrap();
        let server = Server::new(server_cfg, false, 0).await.expect("start server");
        pub_cfg.addrs[0].0 = *server.local_addr();
        pub_cfg.detach();
        sub_cfg.addrs[0].0 = *server.local_addr();
        sub_cfg.detach();
        let default_destroyed = Arc::new(Mutex::new(false));
        let (tx, ready) = oneshot::channel();
        task::spawn(run_publisher(
            pub_cfg,
            default_destroyed,
            tx,
            DesiredAuth::Tls { identity: None },
        ));
        time::timeout(Duration::from_secs(5), ready).await.unwrap().unwrap();
        // Rotate each identity's cert then key as separate atomic renames, with
        // a skew window in between, so a reader can momentarily see new-cert /
        // old-key. The 40ms window is exaggerated (real renewd is sub-ms) to make
        // the race reliably reproducible.
        let stop = Arc::new(AtomicBool::new(false));
        let renewer = task::spawn({
            let stop = stop.clone();
            async move {
                let ids = ["resolver", "publisher", "client"];
                let mut g = 1usize;
                let mut n = 0usize;
                while !stop.load(Ordering::Relaxed) {
                    let dir = format!("{labs}/{}", ids[n % ids.len()]);
                    let swap = |src: String, live: String| {
                        let tmp = format!("{live}.tmp");
                        if std::fs::copy(&src, &tmp).is_ok() {
                            let _ = std::fs::rename(&tmp, &live);
                        }
                    };
                    swap(format!("{dir}/cert{g}.pem"), format!("{dir}/certificate.pem"));
                    time::sleep(Duration::from_millis(40)).await;
                    swap(format!("{dir}/key{g}.pem"), format!("{dir}/private.key"));
                    n += 1;
                    if n % ids.len() == 0 {
                        g = (g + 1) % 4;
                    }
                    time::sleep(Duration::from_millis(250)).await;
                }
            }
        });
        let mut ok = 0usize;
        let mut fail = 0usize;
        for i in 0..200 {
            let sub =
                Subscriber::new(sub_cfg.clone(), DesiredAuth::Tls { identity: None })
                    .unwrap();
            let start = Instant::now();
            let r = sub
                .subscribe_nondurable_one("/app/v0".into(), Some(Duration::from_secs(5)))
                .await;
            match r {
                Ok(_) => ok += 1,
                Err(e) => {
                    fail += 1;
                    println!("FAIL iter={i} elapsed={:?} err={e}", start.elapsed());
                }
            }
            drop(sub);
            time::sleep(Duration::from_millis(150)).await;
        }
        stop.store(true, Ordering::Relaxed);
        let _ = renewer.await;
        println!("cold_subscribe_under_renewal: ok={ok} fail={fail}");
        drop(server);
        let _ = std::fs::remove_dir_all(&lab);
        assert_eq!(fail, 0, "cold subscribe failed {fail}/200 under active renewal");
    }

    /// Kerberos end-to-end test against an in-process KDC. Mirrors
    /// `tls_publish_subscribe` but uses real GSSAPI handshakes between
    /// resolver/publisher/subscriber. Held by `_env` for the duration to
    /// keep `KRB5_CONFIG`/`KRB5CCNAME`/`KRB5_KTNAME` pointed at this
    /// fixture's tempdir; the guard serializes against other krb5 tests
    /// that share the same process-wide env.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn krb5_publish_subscribe() {
        use super::kdc::TestKdc;
        use crate::config::{self as cconfig, DefaultAuthMech, file as cfile};
        use crate::resolver_server::config::{self as sconfig, PMap, file as sfile};
        use arcstr::ArcStr;
        use std::collections::HashMap;

        let _ = env_logger::try_init();

        const RESOLVER_SPN: &str = "netidx/test.example.com@EXAMPLE.COM";
        const PUBLISHER_SPN: &str = "publisher/test.example.com@EXAMPLE.COM";
        const USER_UPN: &str = "testuser@EXAMPLE.COM";
        const USER_PASS: &str = "testpass";

        // Bring up the KDC and provision principals/keytabs/ccache. All of
        // this is blocking (spawning kdb5_util, krb5kdc, kadmin.local,
        // kinit), so push it onto the blocking pool to keep the runtime
        // unblocked.
        let kdc = task::spawn_blocking(|| {
            let kdc = TestKdc::new();
            kdc.add_principal_with_password("testuser", USER_PASS);
            kdc.add_principal_random_key(RESOLVER_SPN);
            kdc.add_principal_random_key(PUBLISHER_SPN);
            kdc.export_keytab(RESOLVER_SPN);
            kdc.export_keytab(PUBLISHER_SPN);
            kdc.kinit("testuser", USER_PASS);
            kdc
        })
        .await
        .expect("kdc setup");
        let _env = kdc.apply_env().await;

        // Resolver server config: Krb5 auth, DoNotMap so the principal name
        // is used verbatim as the entity (skips id-mapping which would try
        // to resolve testuser@EXAMPLE.COM to a unix user via /bin/id), and
        // perms granting swlpd on / to testuser.
        let mut entity_perms = HashMap::new();
        entity_perms.insert(ArcStr::from(USER_UPN), ArcStr::from("swlpd"));
        let mut paths = HashMap::new();
        paths.insert(ArcStr::from("/"), entity_perms);
        let server_cfg = sfile::ConfigBuilder::default()
            .member_servers(vec![
                sfile::MemberServerBuilder::default()
                    .auth(sfile::Auth::Krb5(ArcStr::from(RESOLVER_SPN)))
                    .addr("127.0.0.1:0".parse().unwrap())
                    .bind_addr("127.0.0.1".parse().unwrap())
                    .id_map_type(sfile::IdMapType::DoNotMap)
                    .build()
                    .unwrap(),
            ])
            .perms(PMap(paths))
            .build()
            .unwrap();
        let server_cfg = sconfig::Config::from_file(server_cfg).expect("from_file");
        let server = Server::new(server_cfg, false, 0).await.expect("start server");
        let addr = *server.local_addr();

        // Client (resolver-side) config: same SPN as the resolver, used
        // when subscribers/publishers obtain a service ticket for the
        // resolver.
        let client_cfg = cfile::ConfigBuilder::default()
            .addrs(vec![(addr, cfile::Auth::Krb5(ArcStr::from(RESOLVER_SPN)))])
            .default_auth(DefaultAuthMech::Krb5)
            .default_bind_config("local")
            .build()
            .unwrap();
        let client_cfg = cconfig::Config::from_file(client_cfg).expect("from_file");

        let default_destroyed = Arc::new(Mutex::new(false));
        let (tx, ready) = oneshot::channel();
        let pub_auth = DesiredAuth::Krb5 {
            upn: Some(USER_UPN.to_string()),
            spn: Some(PUBLISHER_SPN.to_string()),
        };
        let sub_auth = DesiredAuth::Krb5 { upn: Some(USER_UPN.to_string()), spn: None };
        task::spawn(run_publisher(
            client_cfg.clone(),
            default_destroyed.clone(),
            tx,
            pub_auth,
        ));
        time::timeout(Duration::from_secs(5), ready).await.unwrap().unwrap();
        run_subscriber(client_cfg, default_destroyed, sub_auth).await;
        drop(server);
        drop(_env);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn slow_consumer() -> Result<()> {
        let _ = env_logger::try_init();
        let resolver = {
            use crate::resolver_server::config::{self, file};
            let cfg = file::ConfigBuilder::default()
                .member_servers(vec![
                    file::MemberServerBuilder::default()
                        .auth(file::Auth::Anonymous)
                        .addr("127.0.0.1:0".parse()?)
                        .bind_addr("127.0.0.1".parse()?)
                        .build()?,
                ])
                .build()?;
            let cfg = config::Config::from_file(cfg)?;
            crate::resolver_server::Server::new(cfg, false, 0).await?
        };
        let addr = *resolver.local_addr();
        let cfg = {
            use crate::config::{self, DefaultAuthMech, file};
            let cfg = file::ConfigBuilder::default()
                .addrs(vec![(addr, file::Auth::Anonymous)])
                .default_auth(DefaultAuthMech::Anonymous)
                .default_bind_config("local")
                .build()?;
            config::Config::from_file(cfg)?
        };
        let timeout = Duration::from_secs(10);
        let _cfg = cfg.clone();
        let pb: JoinHandle<Result<()>> = task::spawn(async move {
            let publisher = PublisherBuilder::new(_cfg).slack(3).build().await?;
            let v = publisher.publish(Path::from("/local/foo"), Value::from(42))?;
            loop {
                let mut batch = publisher.start_batch();
                for i in 0..1000 {
                    v.update(&mut batch, Value::from(i));
                }
                batch.commit(Some(timeout)).await;
                time::sleep(Duration::from_millis(10)).await;
                print!(".");
            }
        });
        let _cfg = cfg.clone();
        let slow_sub: JoinHandle<Result<()>> = task::spawn(async move {
            use futures::channel::mpsc;
            let subscriber = SubscriberBuilder::new(_cfg).build()?;
            let s = subscriber.subscribe(Path::from("/local/foo"));
            let (tx, rx) = mpsc::channel(3);
            s.updates(UpdatesFlags::empty(), tx);
            future::pending::<()>().await;
            drop(rx);
            Ok(())
        });
        let start = Instant::now();
        let mut last_update = Instant::now();
        let subscriber = SubscriberBuilder::new(cfg).build()?;
        let s = subscriber.subscribe(Path::from("/local/foo"));
        let mut hb = time::interval(Duration::from_secs(1));
        let (tx, mut rx) = mpsc::channel(3);
        s.updates(UpdatesFlags::empty(), tx);
        loop {
            tokio::select! {
                _ = hb.tick() => {
                    if dbg!(last_update.elapsed()) > timeout + Duration::from_secs(1) {
                        bail!("updates stopped for longer than timeout!")
                    }
                    if start.elapsed() > timeout * 5 {
                        break
                    }
                },
                _ = rx.select_next_some() => {
                    last_update = Instant::now();
                    print!("-");
                }
            }
        }
        slow_sub.abort();
        pb.abort();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_receipts() -> Result<()> {
        let _ = env_logger::try_init();
        let resolver = {
            use crate::resolver_server::config::{self, file};
            let cfg = file::ConfigBuilder::default()
                .member_servers(vec![
                    file::MemberServerBuilder::default()
                        .auth(file::Auth::Anonymous)
                        .addr("127.0.0.1:0".parse()?)
                        .bind_addr("127.0.0.1".parse()?)
                        .build()?,
                ])
                .build()?;
            let cfg = config::Config::from_file(cfg)?;
            crate::resolver_server::Server::new(cfg, false, 0).await?
        };
        let addr = *resolver.local_addr();
        let cfg = {
            use crate::config::{self, DefaultAuthMech, file};
            let cfg = file::ConfigBuilder::default()
                .addrs(vec![(addr, file::Auth::Anonymous)])
                .default_auth(DefaultAuthMech::Anonymous)
                .default_bind_config("local")
                .build()?;
            config::Config::from_file(cfg)?
        };
        let _cfg = cfg.clone();
        let (ready, is_ready) = oneshot::channel();
        let pb: JoinHandle<Result<()>> = task::spawn(async move {
            let publisher = PublisherBuilder::new(_cfg).build().await?;
            let v = publisher.publish(Path::from("/local/foo"), Value::from(0u64))?;
            let (tx, mut rx) = mpsc::channel(64);
            publisher.writes(v.id(), tx);
            publisher.flushed().await;
            let _ = ready.send(());
            while let Some(mut batch) = rx.next().await {
                for mut req in batch.drain(..) {
                    match req.send_result.take() {
                        Some(r) => r.send(req.value),
                        None => bail!("write receipt was not requested"),
                    }
                }
            }
            Ok(())
        });
        let timeout = Duration::from_secs(30);
        time::timeout(timeout, is_ready).await??;
        let subscriber = SubscriberBuilder::new(cfg).build()?;
        let s =
            subscriber.subscribe_nondurable_one(Path::from("/local/foo"), None).await?;
        for i in 0..10u64 {
            let rx = s.write_with_recipt(Value::from(i));
            assert_eq!(time::timeout(timeout, rx).await??, Value::from(i));
        }
        let receipts = (0..10_000u64)
            .map(|i| s.write_with_recipt(Value::from(i)))
            .collect::<Vec<_>>();
        for (i, rx) in receipts.into_iter().enumerate() {
            assert_eq!(time::timeout(timeout, rx).await??, Value::from(i as u64));
        }
        pb.abort();
        drop(resolver);
        Ok(())
    }

    struct PTestPub(mpsc::UnboundedSender<(bool, oneshot::Sender<()>)>);

    impl PTestPub {
        fn new(
            priority: PublisherPriority,
            cfg: crate::config::Config,
            v: Value,
        ) -> Self {
            let (tx, mut rx) = mpsc::unbounded();
            let t = Self(tx);
            task::spawn(async move {
                let mut publisher: Option<Publisher> = None;
                let mut _val: Option<Val> = None;
                let mut status = false;
                while let Some((up, reply)) = rx.next().await {
                    if up && !status {
                        let p = PublisherBuilder::new(cfg.clone())
                            .priority(priority)
                            .build()
                            .await?;
                        _val = Some(p.publish(Path::from("/local/foo"), v.clone())?);
                        p.flushed().await;
                        publisher = Some(p);
                        status = true
                    } else if status {
                        _val = None;
                        if let Some(p) = publisher.take() {
                            p.shutdown().await
                        }
                        status = false;
                    }
                    let _ = reply.send(());
                }
                Ok::<(), anyhow::Error>(())
            });
            t
        }

        async fn set_status(&mut self, up: bool) -> Result<()> {
            let (tx, rx) = oneshot::channel();
            self.0.unbounded_send((up, tx))?;
            Ok(rx.await?)
        }
    }

    async fn wait_val(rx: &mut mpsc::Receiver<GPooled<Vec<(SubId, Event)>>>) -> Value {
        while let Some(mut events) = rx.next().await {
            for (_, ev) in events.drain(..) {
                match ev {
                    Event::Unsubscribed => (),
                    Event::Update(v) => return v,
                }
            }
        }
        unreachable!()
    }

    async fn check_resolver(rclient: &ResolverRead) -> Result<()> {
        let (pubs, res) = rclient.resolve([Path::from(literal!("/local/foo"))]).await?;
        debug!("published {pubs:?}, resolved: {res:?}");
        assert_eq!(pubs.len(), 3);
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].as_ref().unwrap().publishers.len(), 3);
        let mut saw_high = false;
        let mut saw_normal = false;
        let mut saw_low = false;
        for (_, pb) in pubs.iter() {
            match pb.priority {
                PublisherPriority::High => saw_high = true,
                PublisherPriority::Normal => saw_normal = true,
                PublisherPriority::Low => saw_low = true,
            }
        }
        assert!(saw_high);
        assert!(saw_normal);
        assert!(saw_low);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn priority() -> Result<()> {
        let _ = env_logger::try_init();
        let resolver = {
            use crate::resolver_server::config::{self, file};
            let cfg = file::ConfigBuilder::default()
                .member_servers(vec![
                    file::MemberServerBuilder::default()
                        .auth(file::Auth::Anonymous)
                        .addr("127.0.0.1:0".parse()?)
                        .bind_addr("127.0.0.1".parse()?)
                        .build()?,
                ])
                .build()?;
            let cfg = config::Config::from_file(cfg)?;
            crate::resolver_server::Server::new(cfg, false, 0).await?
        };
        let addr = *resolver.local_addr();
        let cfg = {
            use crate::config::{self, DefaultAuthMech, file};
            let cfg = file::ConfigBuilder::default()
                .addrs(vec![(addr, file::Auth::Anonymous)])
                .default_auth(DefaultAuthMech::Anonymous)
                .default_bind_config("local")
                .build()?;
            config::Config::from_file(cfg)?
        };
        let rclient = ResolverRead::new(cfg.clone(), DesiredAuth::Anonymous);
        let mut high =
            PTestPub::new(PublisherPriority::High, cfg.clone(), Value::I64(42));
        let mut normal =
            PTestPub::new(PublisherPriority::Normal, cfg.clone(), Value::I64(21));
        let mut low = PTestPub::new(PublisherPriority::Low, cfg.clone(), Value::I64(10));
        low.set_status(true).await?;
        for i in 0..1000 {
            debug!("loop iter {i}");
            debug!("turning on high");
            high.set_status(true).await?;
            debug!("turning on normal");
            normal.set_status(true).await?;
            check_resolver(&rclient).await?;
            let subscriber = SubscriberBuilder::new(cfg.clone()).build()?;
            let (tx, mut rx) = mpsc::channel(10);
            debug!("subscribing");
            let s = subscriber.subscribe_updates(
                Path::from("/local/foo"),
                [(UpdatesFlags::empty(), tx)],
            );
            assert_eq!(wait_val(&mut rx).await, Value::I64(42));
            debug!("turning off high");
            high.set_status(false).await?;
            debug!("waiting resub to normal");
            assert_eq!(wait_val(&mut rx).await, Value::I64(21)); // tweeeeenywon
            debug!("turning off normal");
            normal.set_status(false).await?;
            debug!("waiting resub to low");
            assert_eq!(wait_val(&mut rx).await, Value::I64(10));
            debug!("loop finished, dropping subscriber");
            drop(s)
        }
        Ok(())
    }
}

/// Surfacing why a durable subscription isn't subscribed.
mod errors {
    use crate::{
        config::Config as ClientConfig,
        publisher::{BindCfg, DesiredAuth, Publisher, PublisherBuilder},
        resolver_client::ResolverError,
        resolver_server::{Server, config::Config as ServerConfig},
        subscriber::{
            SubId, SubscribeError, SubscribeErrors, Subscriber, SubscriberBuilder,
        },
    };
    use anyhow::Result;
    use futures::{channel::mpsc, prelude::*};
    use netidx_core::path::Path;
    use poolshark::global::GPooled;
    use std::collections::{HashSet, VecDeque};
    use std::time::Duration;
    use tokio::time;

    const TO: Duration = Duration::from_secs(30);
    /// long enough for many retries of a subscription that keeps failing
    const QUIET: Duration = Duration::from_secs(2);

    fn errs(es: &[SubscribeError]) -> SubscribeErrors {
        let mut r = SubscribeErrors::default();
        for e in es {
            r.insert(*e)
        }
        r
    }

    fn bind() -> BindCfg {
        "127.0.0.1/32".parse().unwrap()
    }

    async fn anon_server() -> Result<(Server, ClientConfig)> {
        let server_cfg = ServerConfig::load("../cfg/simple-server.json")?;
        let mut cfg = ClientConfig::load("../cfg/simple-client.json")?;
        let server = Server::new(server_cfg, false, 0).await?;
        cfg.addrs[0].0 = *server.local_addr();
        cfg.detach();
        Ok((server, cfg))
    }

    async fn tls_server() -> Result<(Server, ClientConfig)> {
        #[cfg(unix)]
        let server_cfg = ServerConfig::load("../cfg/tls/resolver/resolver.json")?;
        #[cfg(windows)]
        let server_cfg = ServerConfig::load("../cfg/tls/resolver/resolver-win.json")?;
        let mut cfg = ClientConfig::load("../cfg/tls/publisher/client.json")?;
        let server = Server::new(server_cfg, false, 0).await?;
        cfg.addrs[0].0 = *server.local_addr();
        cfg.detach();
        Ok((server, cfg))
    }

    async fn publisher(cfg: &ClientConfig, auth: DesiredAuth) -> Result<Publisher> {
        PublisherBuilder::new(cfg.clone())
            .desired_auth(auth)
            .bind_cfg(Some(bind()))
            .build()
            .await
    }

    fn subscriber(cfg: &ClientConfig, auth: DesiredAuth) -> Result<Subscriber> {
        SubscriberBuilder::new(cfg.clone()).desired_auth(auth).build()
    }

    /// The errors channel, flattened back into single items so a test can say
    /// what it expects to happen next.
    struct Errors {
        rx: mpsc::Receiver<GPooled<Vec<(SubId, SubscribeErrors)>>>,
        buf: VecDeque<(SubId, SubscribeErrors)>,
    }

    impl Errors {
        fn attach(subscriber: &Subscriber) -> Self {
            let (tx, rx) = mpsc::channel(10);
            subscriber.errors(tx);
            Self { rx, buf: VecDeque::new() }
        }

        async fn next(&mut self, wait: Duration) -> Option<(SubId, SubscribeErrors)> {
            loop {
                if let Some(i) = self.buf.pop_front() {
                    break Some(i);
                }
                match time::timeout(wait, self.rx.next()).await {
                    Err(_) | Ok(None) => break None,
                    Ok(Some(mut b)) => self.buf.extend(b.drain(..)),
                }
            }
        }

        /// Assert nothing more is said. A subscription that keeps failing for
        /// the same reason it already reported must go quiet.
        async fn expect_quiet(&mut self) {
            if let Some(i) = self.next(QUIET).await {
                panic!("expected silence, got {i:?}")
            }
        }
    }

    /// One denied path used to fail every path in the batch with it, and
    /// `do_resub` batches up to 100,000 of them.
    #[tokio::test(flavor = "multi_thread")]
    async fn one_denied_path_does_not_fail_the_batch() -> Result<()> {
        let _ = env_logger::try_init();
        let (server, cfg) = tls_server().await?;
        let auth = DesiredAuth::Tls { identity: None };
        let pb = publisher(&cfg, auth.clone()).await?;
        let _v0 = pb.publish(Path::from("/app/v0"), 42i64)?;
        let _v1 = pb.publish(Path::from("/denied/x"), 42i64)?;
        pb.flushed().await;
        let subscriber = subscriber(&cfg, auth)?;
        let paths = vec![Path::from("/app/v0"), Path::from("/denied/x")];
        let (publishers, res) =
            time::timeout(TO, subscriber.resolver().resolve(paths)).await??;
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].as_ref().unwrap().publishers.len(), 1);
        assert!(!publishers.is_empty());
        let e = res[1].as_ref().unwrap_err();
        assert_eq!(e.downcast_ref::<ResolverError>(), Some(&ResolverError::Denied));
        drop(server);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_denied_subscription_reports_resolver_denied() -> Result<()> {
        let _ = env_logger::try_init();
        let (server, cfg) = tls_server().await?;
        let auth = DesiredAuth::Tls { identity: None };
        let pb = publisher(&cfg, auth.clone()).await?;
        let _v0 = pb.publish(Path::from("/app/v0"), 42i64)?;
        let _v1 = pb.publish(Path::from("/denied/x"), 42i64)?;
        pb.flushed().await;
        let subscriber = subscriber(&cfg, auth)?;
        let mut e = Errors::attach(&subscriber);
        let denied = subscriber.subscribe(Path::from("/denied/x"));
        let allowed = subscriber.subscribe(Path::from("/app/v0"));
        time::timeout(TO, allowed.wait_subscribed()).await??;
        loop {
            match e.next(TO).await {
                None => panic!("the denied subscription never reported"),
                Some((id, errors)) if id == denied.id() => {
                    assert_eq!(errors, errs(&[SubscribeError::ResolverDenied]));
                    break;
                }
                Some((id, errors)) => {
                    assert_eq!(id, allowed.id());
                    assert!(!errors.is_empty(), "a healthy sub said nothing was wrong")
                }
            }
        }
        // the path it could see is subscribed and has nothing to report
        assert_eq!(allowed.last_error(), None);
        assert_eq!(denied.last_error(), Some(errs(&[SubscribeError::ResolverDenied])));
        drop(server);
        Ok(())
    }

    /// Every new reason is reported once, and a subscription that has run out
    /// of new things to say goes quiet rather than restating itself on every
    /// retry.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_dead_publisher_reports_each_reason_once() -> Result<()> {
        let _ = env_logger::try_init();
        let (server, cfg) = anon_server().await?;
        let pb = publisher(&cfg, DesiredAuth::Anonymous).await?;
        let v = pb.publish(Path::from("/app/v0"), 42i64)?;
        pb.flushed().await;
        let subscriber = subscriber(&cfg, DesiredAuth::Anonymous)?;
        let d = subscriber.subscribe(Path::from("/app/v0"));
        time::timeout(TO, d.wait_subscribed()).await??;
        assert_eq!(d.last_error(), None);
        let mut e = Errors::attach(&subscriber);
        drop(v);
        pb.shutdown().await;
        assert_eq!(
            e.next(TO).await,
            Some((d.id(), errs(&[SubscribeError::ConnectionLost])))
        );
        let gone = errs(&[SubscribeError::ConnectionLost, SubscribeError::NotFound]);
        assert_eq!(e.next(TO).await, Some((d.id(), gone)));
        e.expect_quiet().await;
        assert_eq!(d.last_error(), Some(gone));
        drop(server);
        Ok(())
    }

    /// Recovery is on the same channel, so a consumer watching only errors
    /// isn't left thinking a subscription is still dead.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_recovered_subscription_reports_the_empty_set() -> Result<()> {
        let _ = env_logger::try_init();
        let (server, cfg) = anon_server().await?;
        let pb = publisher(&cfg, DesiredAuth::Anonymous).await?;
        let v = pb.publish(Path::from("/app/v0"), 42i64)?;
        pb.flushed().await;
        let subscriber = subscriber(&cfg, DesiredAuth::Anonymous)?;
        let d = subscriber.subscribe(Path::from("/app/v0"));
        time::timeout(TO, d.wait_subscribed()).await??;
        let mut e = Errors::attach(&subscriber);
        drop(v);
        pb.shutdown().await;
        assert_eq!(
            e.next(TO).await,
            Some((d.id(), errs(&[SubscribeError::ConnectionLost])))
        );
        let pb = publisher(&cfg, DesiredAuth::Anonymous).await?;
        let _v = pb.publish(Path::from("/app/v0"), 42i64)?;
        pb.flushed().await;
        time::timeout(TO, d.wait_subscribed()).await??;
        loop {
            match e.next(TO).await {
                None => panic!("recovery was never reported"),
                Some((id, errors)) => {
                    assert_eq!(id, d.id());
                    if errors.is_empty() {
                        break;
                    }
                }
            }
        }
        assert_eq!(d.last_error(), None);
        // and it says it once, not once per resubscription
        e.expect_quiet().await;
        drop(server);
        Ok(())
    }

    /// The failure everyone actually hits: a resolver or a publisher goes
    /// away and takes every subscription with it.
    #[tokio::test(flavor = "multi_thread")]
    async fn mass_failure_reports_each_subscription_once() -> Result<()> {
        const N: usize = 50;
        let _ = env_logger::try_init();
        let (server, cfg) = anon_server().await?;
        let pb = publisher(&cfg, DesiredAuth::Anonymous).await?;
        let mut vals = Vec::new();
        for i in 0..N {
            vals.push(pb.publish(Path::from(format!("/app/v{i}")), 42i64)?);
        }
        pb.flushed().await;
        let subscriber = subscriber(&cfg, DesiredAuth::Anonymous)?;
        let dvals = (0..N)
            .map(|i| subscriber.subscribe(Path::from(format!("/app/v{i}"))))
            .collect::<Vec<_>>();
        for d in dvals.iter() {
            time::timeout(TO, d.wait_subscribed()).await??;
        }
        let mut e = Errors::attach(&subscriber);
        vals.clear();
        pb.shutdown().await;
        let mut lost: HashSet<SubId> = HashSet::new();
        while lost.len() < N {
            match e.next(TO).await {
                None => panic!("only {} of {N} subscriptions reported", lost.len()),
                Some((id, errors)) => {
                    assert_eq!(errors, errs(&[SubscribeError::ConnectionLost]));
                    assert!(lost.insert(id), "{id:?} reported the same set twice");
                }
            }
        }
        assert_eq!(lost, dvals.iter().map(|d| d.id()).collect::<HashSet<_>>());
        drop(server);
        Ok(())
    }
}
