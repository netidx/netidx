extern crate tokio_timer;
use resolver_client::Resolver;
use resolver_server;
use subscriber::Subscriber;
use publisher::{Publisher, BindCfg};
use futures::prelude::*;
use tokio::{self, prelude::*};
use tokio_timer::Delay;
use std::{net::SocketAddr, time::{Instant, Duration}};
use error::*;

#[derive(Serialize, Deserialize, Debug)]
struct Test {
  field0: int,
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
fn startup() -> Result<(Publisher, Subscriber)> {
  let addr = "127.0.0.1:1234".parse<SocketAddr>()?;
  tokio::spawn(resolver_server::run(addr));
  let resolver = await!(Resolver::new(addr))?;
  let publisher = Publisher::new(resolver.clone(), BindCfg::Any)?;
  let subscriber = Subscriber::new(resolver.clone());
  Ok((publisher, subscriber))
}

#[test]
fn test_pub_sub() {
  tokio::run(async_block! {
    tokio::spawn(async_block! {
      let v =
        await!(publisher.clone().publish(Path::from("/test/v"), Test::new()))
        .unwrap();
      await!(publisher.clone().wait_client()).unwrap();
      for i in 0..10 {
        v.update(Test::new());
        await!(Delay::new(Instant::now() + Duration::from_secs(1))).unwrap();
      }
      Ok(())
    });
    tokio::spawn(async_block! {
      let s = await!(subscriber.subscribe::<Test>(Path::from("/test/v"))).unwrap();
      #[async]
      for v in s.updates() { println!("{:#?}", v); }
      Ok(())
    })
    Ok(())
  })
}
