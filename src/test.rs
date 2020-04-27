mod resolver {
    use crate::{
        config,
        path::Path,
        protocol::resolver::ResolverId,
        resolver::{ResolverRead, ResolverWrite, Auth},
        resolver_server::Server,
    };
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

    fn p(p: &str) -> Path {
        Path::from(p)
    }

    #[test]
    fn publish_resolve() {
        use tokio::runtime::Runtime;
        let mut rt = Runtime::new().unwrap();
        rt.block_on(async {
            let server = Server::new(server_config()).await.expect("start server");
            let paddr: SocketAddr = "127.0.0.1:1".parse().unwrap();
            let cfg = client_config(*server.local_addr());
            let w = ResolverWrite::new(cfg.clone(), Auth::Anonymous, paddr).unwrap();
            let r = ResolverRead::new(cfg, Auth::Anonymous).unwrap();
            let paths = vec![p("/foo/bar"), p("/foo/baz"), p("/app/v0"), p("/app/v1")];
            w.publish(paths.clone()).await.unwrap();
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
