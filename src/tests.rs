extern crate tokio_timer;
use resolver_client::Resolver;
use resolver_server::Server;
use subscriber::Subscriber;
use publisher::{Publisher, Published, BindCfg};
use path::Path;
use futures::{prelude::*, sync::oneshot::{channel, Sender}};
use tokio;
use tokio_timer::{Delay, Deadline};
use std::{net::SocketAddr, time::{Instant, Duration}, result::Result};

#[derive(Serialize, Deserialize, Debug)]
struct Test {
  field0: i64,
  field1: String,
  field2: Option<f64>
}

impl Test {
  fn new() -> Self {
    Test {
      field0: 42,
      field1: "foo bar baz".into(),
      field2: Some(42.),
    }
  }
}

#[async]
fn startup() -> Result<(Publisher, Subscriber, Server), ()> {
  let addr = "127.0.0.1:1234".parse::<SocketAddr>().unwrap();
  let server = await!(Server::new(addr)).unwrap();
  let resolver = await!(Resolver::new(addr)).unwrap();
  let publisher = Publisher::new(resolver.clone(), BindCfg::Any).unwrap();
  let subscriber = Subscriber::new(resolver.clone());
  Ok((publisher, subscriber, server))
}

#[async]
fn test_pub(publisher: Publisher, v: Published<Test>) -> Result<(), ()> {
  await!(publisher.clone().wait_client()).unwrap();
  for _ in 0..10 {
    v.update(&Test::new()).unwrap();
    await!(Delay::new(Instant::now() + Duration::from_secs(1))).unwrap();
  }
  Ok(())
}

#[async]
fn test_sub(subscriber: Subscriber, done: Sender<()>) -> Result<(), ()> {
  let s = await!(subscriber.subscribe::<Test>(Path::from("/test/v"))).unwrap();
  #[async]
  for v in s.updates().map_err(|_| ()) { println!("{:#?}", v); }
  println!("stream ended");
  done.send(()).unwrap();
  Ok(())
}

#[test]
fn test_pub_sub() {
  tokio::run(async_block! {
    let (publisher, subscriber, server) = await!(startup()).unwrap();
    let (send_done, recv_done) = channel();
    let v =
      await!(publisher.clone().publish(Path::from("/test/v"), Test::new()))
      .unwrap();
    tokio::spawn(test_pub(publisher, v));
    tokio::spawn(test_sub(subscriber, send_done));
    let to = Instant::now() + Duration::from_secs(15);
    await!(Deadline::new(recv_done.map_err(|_| ()), to)).unwrap();
    drop(server);
    Ok(())
  })
}
