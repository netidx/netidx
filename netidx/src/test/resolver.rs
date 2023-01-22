use crate::{
    chars::Chars,
    config::Config as ClientConfig,
    path::Path,
    protocol::glob::{Glob, GlobSet},
    publisher::PublishFlags,
    resolver_client::{ChangeTracker, DesiredAuth, ResolverRead, ResolverWrite},
    resolver_server::{config::Config as ServerConfig, Server},
};
use netidx_netproto::resolver::TargetAuth;
use rand::{thread_rng, Rng};
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
        match thread_rng().gen_range(0. ..=1.) {
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
        let cfg_huge1_sub = ClientConfig::load("../cfg/complex-huge1-sub-client.json")
            .expect("huge1 sub client config");
        let server_local =
            Server::new(server_cfg_local, false, 0).await.expect("local server");
        let server0_root =
            Server::new(server_cfg_root.clone(), false, 0).await.expect("root server 0");
        let server1_root =
            Server::new(server_cfg_root, false, 1).await.expect("root server 1");
        let server0_huge0 =
            Server::new(server_cfg_huge0.clone(), false, 0).await.expect("huge0 server0");
        let server1_huge0 =
            Server::new(server_cfg_huge0, false, 1).await.expect("huge0 server1");
        let server0_huge1 =
            Server::new(server_cfg_huge1.clone(), false, 0).await.expect("huge1 server0");
        let server1_huge1 =
            Server::new(server_cfg_huge1, false, 1).await.expect("huge1 server0");
        let server0_huge1_sub = Server::new(server_cfg_huge1_sub.clone(), false, 0)
            .await
            .expect("huge1 sub server0");
        let server1_huge1_sub =
            Server::new(server_cfg_huge1_sub, false, 1).await.expect("huge1 sub server0");
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
        &[p("/app/huge1/sub"), p("/app/huge1/x"), p("/app/huge1/y"), p("/app/huge1/z")]
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
    let local_paths =
        ["/local/foo", "/local/bar"].iter().map(|r| Path::from(*r)).collect::<Vec<_>>();
    let mut ct_root = ChangeTracker::new(Path::from("/"));
    let mut ct_app = ChangeTracker::new(Path::from("/app"));
    let r_root = ResolverRead::new(ctx.cfg_root.clone(), DesiredAuth::Anonymous);
    assert!(r_root.check_changed(&mut ct_root).await.unwrap());
    assert!(r_root.check_changed(&mut ct_app).await.unwrap());
    let w0 = ResolverWrite::new(ctx.random_server(), DesiredAuth::Anonymous, waddrs[0])
        .unwrap();
    let w1 = ResolverWrite::new(ctx.random_server(), DesiredAuth::Anonymous, waddrs[1])
        .unwrap();
    w0.publish(paths.iter().cloned()).await.unwrap();
    let wl = ResolverWrite::new(ctx.cfg_local.clone(), DesiredAuth::Anonymous, waddrs[0])
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
    let r_huge1_sub = ResolverRead::new(ctx.cfg_huge1.clone(), DesiredAuth::Anonymous);
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
    Runtime::new().unwrap().block_on(run_publish_resolve_complex())
}
