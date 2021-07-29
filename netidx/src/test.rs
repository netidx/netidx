use crate::config;

mod resolver {
    use super::*;
    use crate::{
        chars::Chars,
        path::Path,
        protocol::glob::{Glob, GlobSet},
        publisher::PublishFlags,
        resolver::{Auth, ChangeTracker, ResolverRead, ResolverWrite},
        resolver_server::Server,
    };
    use std::{iter, net::SocketAddr, time::Duration};
    use tokio::{runtime::Runtime, time};

    fn p(p: &'static str) -> Path {
        Path::from(p)
    }

    #[test]
    fn publish_resolve_simple() {
        Runtime::new().unwrap().block_on(async {
            let mut cfg =
                config::Config::load("../cfg/simple.json").expect("load simple config");
            let server = Server::new(cfg.clone(), config::PMap::default(), false, 0)
                .await
                .expect("start server");
            cfg.addrs[0] = *server.local_addr();
            let paddr: SocketAddr = "127.0.0.1:1".parse().unwrap();
            let w = ResolverWrite::new(cfg.clone(), Auth::Anonymous, paddr);
            let r = ResolverRead::new(cfg, Auth::Anonymous);
            let paths = vec![p("/foo/bar"), p("/foo/baz"), p("/app/v0"), p("/app/v1")];
            let flags = Some(PublishFlags::USE_EXISTING.bits());
            w.publish_with_flags(paths.clone().into_iter().map(|p| (p, flags)))
                .await
                .unwrap();
            for r in r.resolve(paths.clone()).await.unwrap().drain(..) {
                assert_eq!(r.addrs.len(), 1);
                assert_eq!(r.addrs[0].0, paddr);
            }
            let mut l = r.list(p("/")).await.unwrap();
            l.sort();
            assert_eq!(&**l, &*vec![p("/app"), p("/foo")]);
            let mut l = r.list(p("/foo")).await.unwrap();
            l.sort();
            assert_eq!(&**l, &*vec![p("/foo/bar"), p("/foo/baz")]);
            let mut l = r.list(p("/app")).await.unwrap();
            l.sort();
            assert_eq!(&**l, &*vec![p("/app/v0"), p("/app/v1")]);
            drop(server)
        });
    }

    #[test]
    fn publish_default() {
        Runtime::new().unwrap().block_on(async {
            let mut cfg =
                config::Config::load("../cfg/simple.json").expect("load simple config");
            let server = Server::new(cfg.clone(), config::PMap::default(), false, 0)
                .await
                .expect("start server");
            cfg.addrs[0] = *server.local_addr();
            let paddr: SocketAddr = "127.0.0.1:1".parse().unwrap();
            let w = ResolverWrite::new(cfg.clone(), Auth::Anonymous, paddr);
            let r = ResolverRead::new(cfg, Auth::Anonymous);
            w.publish_default(iter::once(p("/default"))).await.unwrap();
            let paths = vec![p("/default/foo/bar"), p("/default/foo/baz")];
            for r in r.resolve(paths.clone()).await.unwrap().drain(..) {
                assert_eq!(r.addrs.len(), 1);
                assert_eq!(r.addrs[0].0, paddr);
            }
            let l = r.list(p("/")).await.unwrap();
            assert_eq!(&**l, &[p("/default")]);
            w.clear().await.unwrap();
            for r in r.resolve(paths.clone()).await.unwrap().drain(..) {
                assert_eq!(r.addrs.len(), 0);
            }
            let l = r.list(p("/")).await.unwrap();
            assert_eq!(&**l, &[]);
            drop(server)
        });
    }

    struct Ctx {
        _root: (Server, Server),
        _huge0: (Server, Server),
        _huge1: (Server, Server),
        _huge1_sub: (Server, Server),
        cfg_root: config::Config,
        cfg_huge0: config::Config,
        cfg_huge1: config::Config,
        cfg_huge1_sub: config::Config,
    }

    impl Ctx {
        async fn new() -> Ctx {
            let pmap = config::PMap::default();
            let cfg_root =
                config::Config::load("../cfg/complex-root.json").expect("root config");
            let cfg_huge0 =
                config::Config::load("../cfg/complex-huge0.json").expect("huge0 config");
            let cfg_huge1 =
                config::Config::load("../cfg/complex-huge1.json").expect("huge1 config");
            let cfg_huge1_sub = config::Config::load("../cfg/complex-huge1-sub.json")
                .expect("huge1 sub config");
            let server0_root = Server::new(cfg_root.clone(), pmap.clone(), false, 0)
                .await
                .expect("root server 0");
            let server1_root = Server::new(cfg_root.clone(), pmap.clone(), false, 1)
                .await
                .expect("root server 1");
            let server0_huge0 = Server::new(cfg_huge0.clone(), pmap.clone(), false, 0)
                .await
                .expect("huge0 server0");
            let server1_huge0 = Server::new(cfg_huge0.clone(), pmap.clone(), false, 1)
                .await
                .expect("huge0 server1");
            let server0_huge1 = Server::new(cfg_huge1.clone(), pmap.clone(), false, 0)
                .await
                .expect("huge1 server0");
            let server1_huge1 = Server::new(cfg_huge1.clone(), pmap.clone(), false, 1)
                .await
                .expect("huge1 server0");
            let server0_huge1_sub =
                Server::new(cfg_huge1_sub.clone(), pmap.clone(), false, 0)
                    .await
                    .expect("huge1 sub server0");
            let server1_huge1_sub =
                Server::new(cfg_huge1_sub.clone(), pmap.clone(), false, 1)
                    .await
                    .expect("huge1 sub server0");
            Ctx {
                _root: (server0_root, server1_root),
                _huge0: (server0_huge0, server1_huge0),
                _huge1: (server0_huge1, server1_huge1),
                _huge1_sub: (server0_huge1_sub, server1_huge1_sub),
                cfg_root,
                cfg_huge0,
                cfg_huge1,
                cfg_huge1_sub,
            }
        }
    }

    async fn check_list(r: &ResolverRead) {
        let mut l = r.list(p("/")).await.unwrap();
        l.sort();
        assert_eq!(&*l, &[p("/app"), p("/tmp")]);
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
        let pat = Glob::new(Chars::from("/app/huge*/*")).unwrap();
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
        let mut answer = r.resolve(paths.iter().cloned()).await.unwrap();
        let mut i = 0;
        for (p, mut r) in paths.iter().zip(answer.drain(..)) {
            r.addrs.sort();
            assert_eq!(r.addrs.len(), addrs.len());
            assert!(r.addrs.iter().map(|(a, _)| a).eq(addrs.iter()));
            assert_eq!(r.krb5_spns.len(), 0);
            match p.as_ref() {
                "/tmp/x" | "/tmp/y" | "/tmp/z" => assert!(
                    r.resolver == ctx.cfg_root.addrs[0]
                        || r.resolver == ctx.cfg_root.addrs[1]
                ),
                "/app/huge0/x" | "/app/huge0/y" | "/app/huge0/z" => assert!(
                    r.resolver == ctx.cfg_huge0.addrs[0]
                        || r.resolver == ctx.cfg_huge0.addrs[1]
                ),
                "/app/huge1/x" | "/app/huge1/y" | "/app/huge1/z" => assert!(
                    r.resolver == ctx.cfg_huge1.addrs[0]
                        || r.resolver == ctx.cfg_huge1.addrs[1]
                ),
                "/app/huge1/sub/x" | "/app/huge1/sub/y" | "/app/huge1/sub/z" => assert!(
                    r.resolver == ctx.cfg_huge1_sub.addrs[0]
                        || r.resolver == ctx.cfg_huge1_sub.addrs[1]
                ),
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
        let mut ct_root = ChangeTracker::new(Path::from("/"));
        let mut ct_app = ChangeTracker::new(Path::from("/app"));
        let r_root = ResolverRead::new(ctx.cfg_root.clone(), Auth::Anonymous);
        assert!(r_root.check_changed(&mut ct_root).await.unwrap());
        assert!(r_root.check_changed(&mut ct_app).await.unwrap());
        let w0 = ResolverWrite::new(ctx.cfg_root.clone(), Auth::Anonymous, waddrs[0]);
        let w1 = ResolverWrite::new(ctx.cfg_root.clone(), Auth::Anonymous, waddrs[1]);
        w0.publish(paths.iter().cloned()).await.unwrap();
        time::sleep(Duration::from_millis(1000)).await;
        assert!(r_root.check_changed(&mut ct_root).await.unwrap());
        assert!(!r_root.check_changed(&mut ct_root).await.unwrap());
        assert!(r_root.check_changed(&mut ct_app).await.unwrap());
        assert!(!r_root.check_changed(&mut ct_app).await.unwrap());
        check_list(&r_root).await;
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
        check_list(&r_root).await;
        check_resolve(&ctx, &r_root, &paths, &waddrs).await;
        let r_huge0 = ResolverRead::new(ctx.cfg_huge0.clone(), Auth::Anonymous);
        check_list(&r_huge0).await;
        check_resolve(&ctx, &r_huge0, &paths, &waddrs).await;
        let r_huge1 = ResolverRead::new(ctx.cfg_huge1.clone(), Auth::Anonymous);
        check_list(&r_huge1).await;
        check_resolve(&ctx, &r_huge1, &paths, &waddrs).await;
        let r_huge1_sub = ResolverRead::new(ctx.cfg_huge1.clone(), Auth::Anonymous);
        check_list(&r_huge1_sub).await;
        check_resolve(&ctx, &r_huge1_sub, &paths, &waddrs).await;
        assert!(!r_root.check_changed(&mut ct_root).await.unwrap());
        assert!(!r_root.check_changed(&mut ct_app).await.unwrap());
        w0.unpublish(paths.iter().cloned()).await.unwrap();
        assert!(r_root.check_changed(&mut ct_root).await.unwrap());
        assert!(!r_root.check_changed(&mut ct_root).await.unwrap());
        assert!(r_root.check_changed(&mut ct_app).await.unwrap());
        assert!(!r_root.check_changed(&mut ct_app).await.unwrap());
        check_list(&r_root).await;
        check_resolve(&ctx, &r_root, &paths, &[waddrs[1]][..]).await;
        check_list(&r_huge1_sub).await;
        check_resolve(&ctx, &r_huge1_sub, &paths, &[waddrs[1]][..]).await;
        check_list(&r_huge1).await;
        check_resolve(&ctx, &r_huge1, &paths, &[waddrs[1]][..]).await;
        check_list(&r_huge0).await;
        check_resolve(&ctx, &r_huge0, &paths, &[waddrs[1]][..]).await;
    }

    #[test]
    fn publish_resolve_complex() {
        Runtime::new().unwrap().block_on(run_publish_resolve_complex())
    }
}

mod publisher {
    use super::*;
    use crate::{
        publisher::{BindCfg, Publisher, Val},
        resolver::Auth,
        resolver_server::Server,
        subscriber::{Event, Subscriber, UpdatesFlags, Value},
    };
    use futures::{channel::mpsc, channel::oneshot, prelude::*};
    use std::{
        net::{IpAddr, SocketAddr},
        time::Duration,
    };
    use tokio::{runtime::Runtime, task, time};

    #[test]
    fn bindcfg() {
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

    #[test]
    fn publish_subscribe() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let mut cfg =
                config::Config::load("../cfg/simple.json").expect("load simple config");
            let server = Server::new(cfg.clone(), config::PMap::default(), false, 0)
                .await
                .expect("start server");
            cfg.addrs[0] = *server.local_addr();
            let pcfg = cfg.clone();
            let (tx, ready) = oneshot::channel();
            task::spawn(async move {
                let publisher = Publisher::new(
                    pcfg,
                    Auth::Anonymous,
                    "127.0.0.1/32".parse().unwrap(),
                )
                .await
                .unwrap();
                let vp = publisher.publish("/app/v0".into(), Value::U64(314159)).unwrap();
                let mut dfp: Option<Val> = None;
                let mut df = publisher.publish_default("/app/q".into()).unwrap();
                publisher.flush(None).await;
                tx.send(()).unwrap();
                let (tx, mut rx) = mpsc::channel(10);
                vp.writes(tx);
                let mut c = 1;
                loop {
                    time::sleep(Duration::from_millis(100)).await;
                    while let Ok(r) = df.try_next() {
                        match r {
                            None => panic!("publish default chan closed"),
                            Some((p, reply)) => {
                                assert!(p.starts_with("/app/q"));
                                dfp = Some(publisher.publish(p, Value::True).unwrap());
                                let _ = reply.send(());
                            }
                        }
                    }
                    if let Some(dfp) = &dfp {
                        dfp.update(Value::True);
                    }
                    vp.update(Value::U64(314159 + c));
                    publisher.flush(None).await;
                    if let Some(mut batch) = rx.next().await {
                        for req in batch.drain(..) {
                            match req.value {
                                Value::U64(v) => {
                                    c = v;
                                }
                                v => panic!("unexpected value written {:?}", v),
                            }
                        }
                    }
                }
            });
            time::timeout(Duration::from_secs(1), ready).await.unwrap().unwrap();
            let subscriber = Subscriber::new(cfg, Auth::Anonymous).unwrap();
            let vs = subscriber.subscribe_one("/app/v0".into(), None).await.unwrap();
            let q = subscriber.subscribe_one("/app/q/foo".into(), None).await.unwrap();
            assert_eq!(q.last(), Event::Update(Value::True));
            let mut i: u64 = 0;
            let mut c: u64 = 0;
            let (tx, mut rx) = mpsc::channel(10);
            vs.updates(UpdatesFlags::BEGIN_WITH_LAST, tx);
            loop {
                match rx.next().await {
                    None => panic!("publisher died"),
                    Some(mut batch) => {
                        for (_, v) in batch.drain(..) {
                            match v {
                                Event::Update(Value::U64(v)) => {
                                    if c == 0 {
                                        c = v;
                                        i = v;
                                        vs.write(Value::U64(2));
                                    } else {
                                        assert_eq!(c + 1, v);
                                        c += 1;
                                        vs.write(Value::U64(c - i + 2));
                                    }
                                }
                                _ => panic!("unexpected value from publisher"),
                            }
                        }
                    }
                }
                if c - i == 100 {
                    break;
                }
            }
            drop(server);
        });
    }
}

mod resolver_store {
    use crate::{pack::Z64, path::Path, resolver_store::*};
    use bytes::Bytes;
    use rand::{self, Rng};
    use std::{
        collections::{BTreeMap, HashMap},
        net::SocketAddr,
    };

    #[test]
    fn test_resolver_store() {
        let mut hm = HashMap::new();
        hm.insert(Path::from("foo"), 0);
        assert_eq!(hm.get(&Path::from("foo")).copied(), Some(0));
        let apps = vec![
            (vec!["/app/test/app0/v0", "/app/test/app0/v1"], "127.0.0.1:100"),
            (vec!["/app/test/app0/v0", "/app/test/app0/v1"], "127.0.0.1:101"),
            (
                vec!["/app/test/app1/v2", "/app/test/app1/v3", "/app/test/app1/v4"],
                "127.0.0.1:105",
            ),
        ];
        let mut store = Store::new(None, BTreeMap::new());
        for (paths, addr) in &apps {
            let parsed = paths.iter().map(|p| Path::from(*p)).collect::<Vec<_>>();
            let addr = addr.parse::<SocketAddr>().unwrap();
            for path in parsed.clone() {
                store.publish(path.clone(), addr, false, None);
                if !store.resolve(&path).1.contains(&(addr, Bytes::new())) {
                    panic!()
                }
                if rand::thread_rng().gen_bool(0.5) {
                    // check that this is idempotent
                    store.publish(path.clone(), addr, false, None);
                }
            }
        }
        let paths = store.list(&Path::from("/"));
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].as_ref(), "/app");
        let cols = store.columns(&Path::from("/"));
        assert_eq!(cols.len(), 0);
        let paths = store.list(&Path::from("/app"));
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].as_ref(), "/app/test");
        let cols = store.columns(&Path::from("/app"));
        assert_eq!(cols.len(), 0);
        let paths = store.list(&Path::from("/app/test"));
        assert_eq!(paths.len(), 2);
        assert_eq!(paths[0].as_ref(), "/app/test/app0");
        assert_eq!(paths[1].as_ref(), "/app/test/app1");
        let mut cols = store.columns(&Path::from("/app/test"));
        cols.sort();
        assert_eq!(cols.len(), 5);
        assert_eq!(cols[0].0.as_ref(), "v0");
        assert_eq!(cols[0].1, Z64(2));
        assert_eq!(cols[1].0.as_ref(), "v1");
        assert_eq!(cols[1].1, Z64(2));
        assert_eq!(cols[2].0.as_ref(), "v2");
        assert_eq!(cols[2].1, Z64(1));
        assert_eq!(cols[3].0.as_ref(), "v3");
        assert_eq!(cols[3].1, Z64(1));
        assert_eq!(cols[4].0.as_ref(), "v4");
        assert_eq!(cols[4].1, Z64(1));
        let paths = store.list(&Path::from("/app/test/app0"));
        assert_eq!(paths.len(), 2);
        assert_eq!(paths[0].as_ref(), "/app/test/app0/v0");
        assert_eq!(paths[1].as_ref(), "/app/test/app0/v1");
        let cols = store.columns(&Path::from("/app/test/app0"));
        assert_eq!(cols.len(), 0);
        let paths = store.list(&Path::from("/app/test/app1"));
        assert_eq!(paths.len(), 3);
        assert_eq!(paths[0].as_ref(), "/app/test/app1/v2");
        assert_eq!(paths[1].as_ref(), "/app/test/app1/v3");
        assert_eq!(paths[2].as_ref(), "/app/test/app1/v4");
        let cols = store.columns(&Path::from("/app/test/app1"));
        assert_eq!(cols.len(), 0);
        let paths = store.list(&Path::from("/app/test/"));
        assert_eq!(paths.len(), 2);
        assert_eq!(paths[0].as_ref(), "/app/test/app0");
        assert_eq!(paths[1].as_ref(), "/app/test/app1");
        let (ref paths, ref addr) = apps[2];
        let addr = addr.parse::<SocketAddr>().unwrap();
        let parsed = paths.iter().map(|p| Path::from(*p)).collect::<Vec<_>>();
        for path in parsed.clone() {
            store.unpublish(path.clone(), addr);
            if store.resolve(&path).1.contains(&(addr, Bytes::new())) {
                panic!()
            }
            if rand::thread_rng().gen_bool(0.5) {
                // check that this is idempotent
                store.unpublish(path.clone(), addr);
            }
        }
        let paths = store.list(&Path::from("/"));
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].as_ref(), "/app");
        let cols = store.columns(&Path::from("/"));
        assert_eq!(cols.len(), 0);
        let paths = store.list(&Path::from("/app"));
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].as_ref(), "/app/test");
        let cols = store.columns(&Path::from("/app"));
        assert_eq!(cols.len(), 0);
        let paths = store.list(&Path::from("/app/test"));
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].as_ref(), "/app/test/app0");
        let mut cols = store.columns(&Path::from("/app/test"));
        cols.sort();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].0.as_ref(), "v0");
        assert_eq!(cols[0].1, Z64(2));
        assert_eq!(cols[1].0.as_ref(), "v1");
        assert_eq!(cols[1].1, Z64(2));
        let paths = store.list(&Path::from("/app/test/app1"));
        assert_eq!(paths.len(), 0);
        let cols = store.columns(&Path::from("/app/test/app1"));
        assert_eq!(cols.len(), 0);
        let paths = store.list(&Path::from("/app/test/app0"));
        assert_eq!(paths.len(), 2);
        assert_eq!(paths[0].as_ref(), "/app/test/app0/v0");
        assert_eq!(paths[1].as_ref(), "/app/test/app0/v1");
        let cols = store.columns(&Path::from("/app/test/app0"));
        assert_eq!(cols.len(), 0);
        for (paths, addr) in &apps {
            let parsed = paths.iter().map(|p| Path::from(*p)).collect::<Vec<_>>();
            let addr = addr.parse::<SocketAddr>().unwrap();
            for path in parsed.clone() {
                store.unpublish(path.clone(), addr);
                if store.resolve(&path).1.contains(&(addr, Bytes::new())) {
                    panic!()
                }
            }
        }
        let paths = store.list(&Path::from("/"));
        assert_eq!(paths.len(), 0);
        let cols = store.columns(&Path::from("/app/test"));
        assert_eq!(cols.len(), 0);
    }
}
