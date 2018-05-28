extern crate tokio_timer;
use resolver_client::Resolver;
use resolver_server::Server;
use subscriber::Subscriber;
use publisher::{Publisher, BindCfg};
use path::Path;
use futures::{prelude::*, sync::oneshot::{Sender, channel}};
use tokio;
use tokio_timer::{Delay, Deadline};
use std::{
  net::{SocketAddr, IpAddr, Ipv4Addr},
  time::{Instant, Duration}, result::Result
};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Test {
  field0: i64,
  field1: String,
  field2: Option<f64>
}

impl Test {
  fn new() -> Self {
    Test {
      field0: 0,
      field1: "foo bar baz".into(),
      field2: Some(42.),
    }
  }
}

#[async]
fn startup(port: u16) -> Result<(Publisher, Subscriber, Server), ()> {
  let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
  let server = await!(Server::new(addr)).unwrap();
  let resolver = await!(Resolver::new(addr)).unwrap();
  let publisher = Publisher::new(resolver.clone(), BindCfg::Any).unwrap();
  let subscriber = Subscriber::new(resolver.clone());
  Ok((publisher, subscriber, server))
}

#[async]
fn start_subscriber(subscriber: Subscriber, done: Sender<()>) -> Result<(), ()> {
  let s = await!(subscriber.subscribe::<Test>(Path::from("/test/v"))).unwrap();
  let mut i = 0;
  let mut initial = 0;
  #[async]
  for v in s.updates().map_err(|_| ()) {
    if i == 0 { initial = v.field0; }
    else if v.field0 != i + initial {
      panic!("expected {} got {}", i + initial, v.field0)
    }
    i += 1
  }
  done.send(()).unwrap();
  Ok(())
}

#[test]
fn test_basic_pub_sub() {
  tokio::run(async_block! {
    let (publisher, subscriber, server) = await!(startup(1234)).unwrap();
    let (send0_done, recv0_done) = channel();
    let (send1_done, recv1_done) = channel();
    let v =
      await!(publisher.clone().publish(Path::from("/test/v"), Test::new()))
      .unwrap();
    tokio::spawn(async_block! {
      await!(publisher.clone().wait_client(1)).unwrap();
      let mut test = Test::new();
      for i in 1..11 {
        test.field0 = i;
        v.update(&test).unwrap();
        await!(Delay::new(Instant::now() + Duration::from_millis(100))).unwrap();
      }
      Ok(())
    });
    tokio::spawn(start_subscriber(subscriber.clone(), send0_done));
    tokio::spawn(start_subscriber(subscriber, send1_done));
    let to = Instant::now() + Duration::from_secs(15);
    let done = recv0_done.map_err(|_| ()).join(recv1_done.map_err(|_| ()));
    await!(Deadline::new(done, to)).unwrap();
    drop(server);
    Ok(())
  })
}
