use crate::config;

mod resolver {
    use super::*;
    use crate::{
        path::Path,
        resolver::{Auth, ResolverRead, ResolverWrite},
        resolver_server::Server,
    };
    use std::net::SocketAddr;
    use tokio::runtime::Runtime;

    fn p(p: &str) -> Path {
        Path::from(p)
    }

    #[test]
    fn publish_resolve_simple() {
        Runtime::new().unwrap().block_on(async {
            let mut cfg = config::Config::load_from_file("cfg/simple.json")
                .expect("load simple config");
            let server = Server::new(cfg.clone(), config::PMap::default(), false, 0)
                .await
                .expect("start server");
            cfg.addrs[0] = *server.local_addr();
            let paddr: SocketAddr = "127.0.0.1:1".parse().unwrap();
            let w = ResolverWrite::new(cfg.clone(), Auth::Anonymous, paddr);
            let r = ResolverRead::new(cfg, Auth::Anonymous);
            let paths = vec![p("/foo/bar"), p("/foo/baz"), p("/app/v0"), p("/app/v1")];
            w.publish(paths.clone()).await.unwrap();
            for r in r.resolve(paths.clone()).await.unwrap().drain(..) {
                assert_eq!(r.addrs.len(), 1);
                assert_eq!(r.addrs[0].0, paddr);
            }
            assert_eq!(&*r.list(p("/")).await.unwrap(), &*vec![p("/app"), p("/foo")]);
            assert_eq!(
                &*r.list(p("/foo")).await.unwrap(),
                &*vec![p("/foo/bar"), p("/foo/baz")]
            );
            assert_eq!(
                &*r.list(p("/app")).await.unwrap(),
                &*vec![p("/app/v0"), p("/app/v1")]
            );
            drop(server)
        });
    }

    struct Ctx {
        root: (Server, Server),
        huge0: (Server, Server),
        huge1: (Server, Server),
        cfg_root: config::Config,
        cfg_huge0: config::Config,
        cfg_huge1: config::Config,
    }

    impl Ctx {
        fn new() -> Ctx {
            let pmap = config::PMap::default();
            let cfg_root = config::Config::load_from_file("cfg/complex-root.json")
                .expect("root config");
            let cfg_huge0 = config::Config::load_from_file("cfg/complex-huge0.json")
                .expect("huge0 config");
            let cfg_huge1 = config::Config::load_from_file("cfg/complex-huge1.json")
                .expect("huge1 config");
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
            Ctx {
                root: (server0_root, server1_root),
                huge0: (server0_huge0, server1_huge0),
                huge1: (server0_huge1, server1_huge1),
                cfg_root,
                cfg_huge0,
                cfg_huge1,
            }
        }
    }

    #[test]
    fn publish_resolve_complex() {
        Runtime::new().unwrap().block_on(async {
            let ctx = Ctx::new();
            let waddr: SocketAddr = "127.0.0.1:5543".parse().unwrap();
            let paths = &[
                "/tmp/x",
                "/tmp/y",
                "/tmp/z",
                "/app/huge0/x",
                "/app/huge0/y",
                "/app/huge0/z",
                "/app/huge1/x",
                "/app/huge1/y",
                "/app/huge1/z",
            ]
            .into_iter()
            .map(Path::from)
            .collect::<Vec<_>>();
            let mut w = ResolverWrite::new(ctx.cfg_root.clone(), Auth::Anonymous, waddr);
            w.publish(paths.iter().cloned()).await.unwrap();
        })
    }
}

mod publisher {
    use super::*;
    use crate::{
        publisher::{BindCfg, Publisher},
        resolver::Auth,
        resolver_server::Server,
        subscriber::{Subscriber, Value},
    };
    use futures::{channel::mpsc, prelude::*};
    use std::{
        net::{IpAddr, SocketAddr},
        time::Duration,
    };
    use tokio::{runtime::Runtime, sync::oneshot, task, time};

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
        let mut rt = Runtime::new().unwrap();
        rt.block_on(async {
            let mut cfg = config::Config::load_from_file("cfg/simple.json")
                .expect("load simple config");
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
                publisher.flush(None).await.unwrap();
                tx.send(()).unwrap();
                let (tx, mut rx) = mpsc::channel(10);
                vp.writes(tx);
                let mut c = 1;
                loop {
                    time::delay_for(Duration::from_millis(100)).await;
                    vp.update(Value::U64(314159 + c));
                    publisher.flush(None).await.unwrap();
                    if let Some(mut batch) = rx.next().await {
                        for (_, v) in batch.drain(..) {
                            match v {
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
            let mut i: u64 = 0;
            let mut c: u64 = 0;
            let (tx, mut rx) = mpsc::channel(10);
            vs.updates(true, tx);
            loop {
                match rx.next().await {
                    None => panic!("publisher died"),
                    Some(mut batch) => {
                        for (_, v) in batch.drain(..) {
                            match v {
                                Value::U64(v) => {
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
    use crate::{path::Path, resolver_store::*};
    use bytes::Bytes;
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
        let store = Store::<()>::new(None, BTreeMap::new());
        {
            let mut store = store.write();
            for (paths, addr) in &apps {
                let parsed = paths.iter().map(|p| Path::from(*p)).collect::<Vec<_>>();
                let addr = addr.parse::<SocketAddr>().unwrap();
                for path in parsed.clone() {
                    store.publish(path.clone(), addr, false);
                    if !store.resolve(&path).contains(&(addr, Bytes::new())) {
                        panic!()
                    }
                }
            }
        }
        {
            let store = store.read();
            let paths = store.list(&Path::from("/"));
            assert_eq!(paths.len(), 1);
            assert_eq!(paths[0].as_ref(), "/app");
            let paths = store.list(&Path::from("/app"));
            assert_eq!(paths.len(), 1);
            assert_eq!(paths[0].as_ref(), "/app/test");
            let paths = store.list(&Path::from("/app/test"));
            assert_eq!(paths.len(), 2);
            assert_eq!(paths[0].as_ref(), "/app/test/app0");
            assert_eq!(paths[1].as_ref(), "/app/test/app1");
            let paths = store.list(&Path::from("/app/test/app0"));
            assert_eq!(paths.len(), 2);
            assert_eq!(paths[0].as_ref(), "/app/test/app0/v0");
            assert_eq!(paths[1].as_ref(), "/app/test/app0/v1");
            let paths = store.list(&Path::from("/app/test/app1"));
            assert_eq!(paths.len(), 3);
            assert_eq!(paths[0].as_ref(), "/app/test/app1/v2");
            assert_eq!(paths[1].as_ref(), "/app/test/app1/v3");
            assert_eq!(paths[2].as_ref(), "/app/test/app1/v4");
        }
        let (ref paths, ref addr) = apps[2];
        let addr = addr.parse::<SocketAddr>().unwrap();
        let parsed = paths.iter().map(|p| Path::from(*p)).collect::<Vec<_>>();
        {
            let mut store = store.write();
            for path in parsed.clone() {
                store.unpublish(path.clone(), addr);
                if store.resolve(&path).contains(&(addr, Bytes::new())) {
                    panic!()
                }
            }
        }
        {
            let store = store.read();
            let paths = store.list(&Path::from("/"));
            assert_eq!(paths.len(), 1);
            assert_eq!(paths[0].as_ref(), "/app");
            let paths = store.list(&Path::from("/app"));
            assert_eq!(paths.len(), 1);
            assert_eq!(paths[0].as_ref(), "/app/test");
            let paths = store.list(&Path::from("/app/test"));
            assert_eq!(paths.len(), 1);
            assert_eq!(paths[0].as_ref(), "/app/test/app0");
            let paths = store.list(&Path::from("/app/test/app1"));
            assert_eq!(paths.len(), 0);
            let paths = store.list(&Path::from("/app/test/app0"));
            assert_eq!(paths.len(), 2);
            assert_eq!(paths[0].as_ref(), "/app/test/app0/v0");
            assert_eq!(paths[1].as_ref(), "/app/test/app0/v1");
        }
    }
}
