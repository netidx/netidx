use crate::config;
use std::net::SocketAddr;

fn resolver_server_config_simple() -> config::resolver_server::Config {
    use config::resolver_server::Config;
    Config::load("cfg/simple/resolver-server.json")
        .expect("load simple resolver server config")
}

fn resolver_config_simple(server: SocketAddr) -> config::resolver::Config {
    use config::resolver::Config;
    Config {
        addrs: vec![server],
        ..Config::load("cfg/simple/resolver.json").expect("load simple resolver config")
    }
}

mod resolver {
    use super::*;
    use crate::{
        path::Path,
        resolver::{Auth, ResolverRead, ResolverWrite},
        resolver_server::Server,
    };
    use std::net::SocketAddr;

    fn p(p: &str) -> Path {
        Path::from(p)
    }

    #[test]
    fn publish_resolve() {
        use tokio::runtime::Runtime;
        let mut rt = Runtime::new().unwrap();
        rt.block_on(async {
            let cfg = resolver_server_config_simple();
            let server = Server::new(cfg, false).await.expect("start server");
            let paddr: SocketAddr = "127.0.0.1:1".parse().unwrap();
            let cfg = resolver_config_simple(*server.local_addr());
            let w = ResolverWrite::new(cfg.clone(), Auth::Anonymous, paddr);
            let r = ResolverRead::new(cfg.clone(), Auth::Anonymous);
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
        });
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
            let cfg = resolver_server_config_simple();
            let server = Server::new(cfg, false).await.expect("start server");
            let cfg = resolver_config_simple(*server.local_addr());
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
