mod resolver {
    use crate::{
        config::Config as ClientConfig,
        path::Path,
        protocol::glob::{Glob, GlobSet},
        publisher::PublishFlags,
        resolver_client::{ChangeTracker, DesiredAuth, ResolverRead, ResolverWrite},
        resolver_server::{config::Config as ServerConfig, Server},
    };
    use arcstr::literal;
    use netidx_netproto::resolver::TargetAuth;
    use rand::{rng, Rng};
    use std::{iter, net::SocketAddr, time::Duration};
    use tokio::{runtime::Runtime, time};

    fn p(p: &'static str) -> Path {
        Path::from(p)
    }

    #[test]
    fn publish_resolve_simple() {
        Runtime::new().unwrap().block_on(async {
            let server_cfg = ServerConfig::load("../cfg/simple-server.json")
                .expect("load simple server config");
            let mut client_cfg = ClientConfig::load("../cfg/simple-client.json")
                .expect("load simple client config");
            let server = Server::new(server_cfg, false, 0).await.expect("start server");
            client_cfg.addrs[0].0 = *server.local_addr();
            let paddr: SocketAddr = "127.0.0.1:1".parse().unwrap();
            let w = ResolverWrite::new(client_cfg.clone(), DesiredAuth::Anonymous, paddr)
                .unwrap();
            let r = ResolverRead::new(client_cfg, DesiredAuth::Anonymous);
            let paths = vec![p("/foo/bar"), p("/foo/baz"), p("/app/v0"), p("/app/v1")];
            let flags = Some(PublishFlags::USE_EXISTING.bits());
            w.publish_with_flags(paths.iter().map(|p| (p.clone(), flags))).await.unwrap();
            let (publishers, mut resolved) = r.resolve(paths.clone()).await.unwrap();
            for r in resolved.drain(..) {
                assert_eq!(r.publishers.len(), 1);
                let pb = publishers.get(&r.publishers[0].id).unwrap();
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
        });
    }

    #[test]
    fn publish_default() {
        let _ = env_logger::try_init();
        Runtime::new().unwrap().block_on(async {
            let server_cfg = ServerConfig::load("../cfg/simple-server.json")
                .expect("load simple server config");
            let mut client_cfg = ClientConfig::load("../cfg/simple-client.json")
                .expect("load simple client config");
            let server = Server::new(server_cfg, false, 0).await.expect("start server");
            client_cfg.addrs[0].0 = *server.local_addr();
            let paddr: SocketAddr = "127.0.0.1:1".parse().unwrap();
            let w = ResolverWrite::new(client_cfg.clone(), DesiredAuth::Anonymous, paddr)
                .unwrap();
            let r = ResolverRead::new(client_cfg, DesiredAuth::Anonymous);
            w.publish_default(iter::once(p("/default"))).await.unwrap();
            let paths = vec![p("/default/foo/bar"), p("/default/foo/baz")];
            let (publishers, mut resolved) = r.resolve(paths.clone()).await.unwrap();
            for r in resolved.drain(..) {
                assert_eq!(r.publishers.len(), 1);
                let pb = publishers.get(&r.publishers[0].id).unwrap();
                assert_eq!(pb.addr, paddr);
            }
            let l = r.list(p("/")).await.unwrap();
            assert_eq!(&**l, &[p("/default")]);
            w.clear().await.unwrap();
            let (_, mut resolved) = r.resolve(paths.clone()).await.unwrap();
            for r in resolved.drain(..) {
                assert_eq!(r.publishers.len(), 0);
            }
            let l = r.list(p("/")).await.unwrap();
            assert_eq!(&**l, &[]);
            drop(server)
        });
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
            let mut r_addrs =
                r.publishers.iter().map(|pr| publishers[&pr.id].addr).collect::<Vec<_>>();
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

    async fn run_publish_resolve_complex() {
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
        let w0 =
            ResolverWrite::new(ctx.random_server(), DesiredAuth::Anonymous, waddrs[0])
                .unwrap();
        let w1 =
            ResolverWrite::new(ctx.random_server(), DesiredAuth::Anonymous, waddrs[1])
                .unwrap();
        w0.publish(paths.iter().cloned()).await.unwrap();
        let wl =
            ResolverWrite::new(ctx.cfg_local.clone(), DesiredAuth::Anonymous, waddrs[0])
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

    #[test]
    fn publish_resolve_complex() {
        let _ = env_logger::try_init();
        Runtime::new().unwrap().block_on(run_publish_resolve_complex())
    }
}

mod publisher {
    use crate::{
        config::Config as ClientConfig,
        publisher::{
            BindCfg, DesiredAuth, Event as PEvent, PublishFlags, Publisher, Val,
        },
        resolver_server::{config::Config as ServerConfig, Server},
        subscriber::{Event, Subscriber, UpdatesFlags, Value},
    };
    use futures::{channel::mpsc, channel::oneshot, prelude::*, select_biased};
    use parking_lot::Mutex;
    use std::{
        iter,
        net::{IpAddr, SocketAddr},
        sync::Arc,
        time::Duration,
    };
    use tokio::{runtime::Runtime, task, time};

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
        let publisher =
            Publisher::new(cfg, auth, "127.0.0.1/32".parse().unwrap(), 768, 3)
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

    #[test]
    fn publish_subscribe() {
        let _ = env_logger::try_init();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let server_cfg = ServerConfig::load("../cfg/simple-server.json")
                .expect("load simple server config");
            let mut client_cfg = ClientConfig::load("../cfg/simple-client.json")
                .expect("load simple client config");
            let server = Server::new(server_cfg, false, 0).await.expect("start server");
            client_cfg.addrs[0].0 = *server.local_addr();
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
        });
    }

    #[test]
    fn tls_publish_subscribe() {
        let _ = env_logger::try_init();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
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
            sub_cfg.addrs[0].0 = *server.local_addr();
            task::spawn(run_publisher(
                pub_cfg.clone(),
                default_destroyed.clone(),
                tx,
                DesiredAuth::Tls { identity: None },
            ));
            time::timeout(Duration::from_secs(1), ready).await.unwrap().unwrap();
            run_subscriber(
                pub_cfg,
                default_destroyed,
                DesiredAuth::Tls { identity: None },
            )
            .await;
            drop(server)
        })
    }
}
