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

    #[tokio::test(flavor = "multi_thread")]
    async fn publish_resolve_simple() {
        let _ = env_logger::try_init();
        let server_cfg = ServerConfig::load("../cfg/simple-server.json")
            .expect("load simple server config");
        let mut client_cfg = ClientConfig::load("../cfg/simple-client.json")
            .expect("load simple client config");
        let server = Server::new(server_cfg, false, 0).await.expect("start server");
        client_cfg.addrs[0].0 = *server.local_addr();
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
            assert_eq!(r.publishers.len(), 0);
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
        sub_cfg.addrs[0].0 = *server.local_addr();
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
        sub_cfg.addrs[0].0 = *server.local_addr();
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
        assert_eq!(res[0].publishers.len(), 3);
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
