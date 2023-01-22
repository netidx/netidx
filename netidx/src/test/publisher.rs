use crate::{
    config::Config as ClientConfig,
    publisher::{BindCfg, DesiredAuth, Event as PEvent, PublishFlags, Publisher, Val},
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
    let publisher =
        Publisher::new(cfg, auth, "127.0.0.1/32".parse().unwrap(), 768).await.unwrap();
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
                        publisher.publish_with_flags(f, p, Value::True).unwrap();
                    dfp = Some(p);
                    let _ = reply.send(());
                } else if &*p == "/app/q/adv" {
                    _adv = Some(publisher.publish(p, Value::False).unwrap());
                    let _ = reply.send(());
                } else {
                    panic!("unexpected default subscription {}", p);
                }
            },
            mut batch = rx.select_next_some() => {
                let mut ub = publisher.start_batch();
                for req in batch.drain(..) {
                    vp.update(&mut ub, dbg!(req.value));
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
    let vs = subscriber.subscribe_one("/app/v0".into(), None).await.unwrap();
    // we should be able to subscribe to an alias and it should
    // behave as if we just cloned the existing
    // subscription. E.G. no extra values in the channel.
    let va = subscriber.subscribe_one("/app/v1".into(), None).await.unwrap();
    let q = subscriber.subscribe_one("/app/q/foo".into(), None).await.unwrap();
    assert_eq!(q.last(), Event::Update(Value::True));
    let (_, res) =
        subscriber.resolver().resolve(iter::once("/app/q/adv".into())).await.unwrap();
    assert_eq!(res.len(), 1);
    let a = subscriber.subscribe_one("/app/q/adv".into(), None).await.unwrap();
    assert_eq!(a.last(), Event::Update(Value::False));
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
                    match dbg!(v) {
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
fn publish_subscribe_tls() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let server_cfg = ServerConfig::load("../cfg/tls/resolver/resolver.json")
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
    })
}
