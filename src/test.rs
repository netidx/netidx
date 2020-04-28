use crate::{config, protocol::resolver::ResolverId};
use std::net::SocketAddr;

fn server_config() -> config::resolver_server::Config {
    use config::resolver_server::{Auth, Config};
    Config {
        pid_file: "".into(),
        id: ResolverId::mk(0),
        addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 768,
        auth: Auth::Anonymous,
    }
}

fn client_config(server: SocketAddr) -> config::resolver::Config {
    use config::resolver::{Auth, Config};
    Config { servers: vec![(ResolverId::mk(0), server)], auth: Auth::Anonymous }
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
            dbg!("starting server");
            let server = Server::new(server_config()).await.expect("start server");
            dbg!("server started");
            let paddr: SocketAddr = "127.0.0.1:1".parse().unwrap();
            let cfg = client_config(*server.local_addr());
            let w = ResolverWrite::new(cfg.clone(), Auth::Anonymous, paddr).unwrap();
            let r = ResolverRead::new(cfg, Auth::Anonymous).unwrap();
            dbg!("clients created");
            let paths = vec![p("/foo/bar"), p("/foo/baz"), p("/app/v0"), p("/app/v1")];
            dbg!("before publish");
            w.publish(paths.clone()).await.unwrap();
            dbg!("after publish");
            for addrs in r.resolve(paths.clone()).await.unwrap().addrs {
                assert_eq!(addrs.len(), 1);
                assert_eq!(addrs[0].0, paddr);
            }
            assert_eq!(r.list(p("/")).await.unwrap(), vec![p("/app"), p("/foo")]);
            assert_eq!(
                r.list(p("/foo")).await.unwrap(),
                vec![p("/foo/bar"), p("/foo/baz")]
            );
            assert_eq!(
                r.list(p("/app")).await.unwrap(),
                vec![p("/app/v0"), p("/app/v1")]
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
    use futures::prelude::*;
    use std::time::Duration;
    use tokio::{runtime::Runtime, sync::oneshot, task, time};

    #[test]
    fn publish_subscribe() {
        let mut rt = Runtime::new().unwrap();
        rt.block_on(async {
            let server = Server::new(server_config()).await.expect("start server");
            let cfg = client_config(*server.local_addr());
            let pcfg = cfg.clone();
            let (tx, ready) = oneshot::channel();
            task::spawn(async move {
                let publisher =
                    Publisher::new(pcfg, Auth::Anonymous, BindCfg::Local).await.unwrap();
                let vp =
                    publisher.publish_val("/app/v0".into(), Value::U64(314159)).unwrap();
                publisher.flush(None).await.unwrap();
                tx.send(()).unwrap();
                let mut c = 0;
                loop {
                    time::delay_for(Duration::from_millis(100)).await;
                    vp.update(Value::U64(314159 + c));
                    publisher.flush(None).await.unwrap();
                    c += 1
                }
            });
            time::timeout(Duration::from_secs(1), ready).await.unwrap().unwrap();
            let subscriber = Subscriber::new(cfg, Auth::Anonymous).unwrap();
            let vs =
                subscriber.subscribe_val::<u64>("/app/v0".into(), None).await.unwrap();
            let mut c: u64 = 0;
            let mut s = vs.updates(true);
            loop {
                match s.next().await {
                    None => panic!("publisher died"),
                    Some(Err(e)) => panic!("error: {}", e),
                    Some(Ok(v)) => {
                        assert_eq!(314159 + c, v);
                        c += 1
                    }
                }
                if c == 100 {
                    break;
                }
            }
            drop(server);
        });
    }
}

mod resolver_store {
    use std::{collections::HashMap, net::SocketAddr};
    use bytes::Bytes;
    use crate::{resolver_store::*, path::Path};
    
    #[test]
    fn test_resolver_store() {
        let mut hm = HashMap::new();
        hm.insert(Path::from("foo"), 0);
        assert_eq!(hm.get(&Path::from("foo")).copied(), Some(0));
        let apps = vec![
            (
                vec!["/app/test/app0/v0", "/app/test/app0/v1"],
                "127.0.0.1:100",
            ),
            (
                vec!["/app/test/app0/v0", "/app/test/app0/v1"],
                "127.0.0.1:101",
            ),
            (
                vec![
                    "/app/test/app1/v2",
                    "/app/test/app1/v3",
                    "/app/test/app1/v4",
                ],
                "127.0.0.1:105",
            ),
        ];
        let store = Store::<()>::new();
        {
            let mut store = store.write();
            for (paths, addr) in &apps {
                let parsed = paths.iter().map(|p| Path::from(*p)).collect::<Vec<_>>();
                let addr = addr.parse::<SocketAddr>().unwrap();
                for path in parsed.clone() {
                    store.publish(path.clone(), addr);
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