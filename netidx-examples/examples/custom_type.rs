use anyhow::Result;
use futures::{channel::mpsc, StreamExt};
use netidx::{
    config::Config,
    path::Path,
    subscriber::{Event, SubscriberBuilder, UpdatesFlags},
};
use netidx_derive::Pack;
use netidx_value::{Abstract, Value};
use std::{env, process::Command, sync::LazyLock};
use tokio::{signal, task};
use uuid::Uuid;

const VAR_CONTAINER: &str = "START_NETIDX_CONTAINER";
static BASE: LazyLock<Path> = LazyLock::new(|| Path::from("/local/db"));

// start the container server if we are the background process
fn maybe_run_container(cfg: Config) -> Result<()> {
    use netidx_container::{Container, ParamsBuilder, Txn};
    use std::env;
    use tempdir::TempDir;
    use tokio::sync::oneshot;
    if let Some(_) = env::var(VAR_CONTAINER).ok() {
        let temp = TempDir::new("netidx_container")?;
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
        let (tx_stop, rx_stop) = oneshot::channel();
        let mut tx_stop = Some(tx_stop);
        ctrlc::set_handler(move || {
            if let Some(tx) = tx_stop.take() {
                let _ = tx.send(());
            }
        })?;
        rt.block_on(async {
            let auth = cfg.default_auth();
            let db_path = format!("{}", temp.path().display());
            let params = ParamsBuilder::default().db(db_path).build()?;
            let container = Container::start(cfg, auth, params).await?;
            let mut txn = Txn::new();
            txn.add_root(BASE.clone(), None);
            container.commit(txn).await?;
            let _ = rx_stop.await?;
            Ok::<_, anyhow::Error>(())
        })?;
        drop((temp, rt));
        std::process::exit(0)
    }
    Ok(())
}

#[derive(Debug, Pack, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct CustomType {
    foo: String,
    bar: u16,
    baz: u32,
}

#[tokio::main]
async fn tokio_main(cfg: Config) -> Result<()> {
    let wrapper = Abstract::register::<CustomType>(Uuid::new_v4())?;
    let subscriber = SubscriberBuilder::new(cfg).build()?;
    let (tx, mut rx) = mpsc::channel(10);
    let val =
        subscriber.subscribe_updates(BASE.join("custom"), [(UpdatesFlags::empty(), tx)]);
    task::spawn(async move {
        while let Some(mut batch) = rx.next().await {
            for (_sub_id, ev) in batch.drain(..) {
                match ev {
                    Event::Update(Value::Abstract(a)) => {
                        if let Some(c) = a.downcast_ref::<CustomType>() {
                            println!("Update from container: {c:?}")
                        } else {
                            println!("Unknown custom type from container")
                        }
                    }
                    Event::Update(Value::Null) => {
                        println!("Initial value from container: null")
                    }
                    Event::Update(v) => {
                        println!("Unexpected value from container: {v}")
                    }
                    Event::Unsubscribed => println!("Lost connection to container"),
                }
            }
        }
    });
    val.write(wrapper.wrap(CustomType { foo: "foo".into(), bar: 42, baz: 42 }));
    Ok(signal::ctrl_c().await?)
}

fn main() -> Result<()> {
    // init logging
    env_logger::init();
    // maybe start up the local resolver
    Config::maybe_run_machine_local_resolver()?;
    // load the config either from the file system or fall back to machine local
    let cfg = Config::load_default_or_local_only()?;
    maybe_run_container(cfg.clone())?;
    // start ourselves again with the environment variable set so we run the
    // container in a background process. This is necessary because type
    // registrations are process global and we want to show that it is not
    // necessary to register the custom types in the container in order to use
    // them, so we must run the container in a new process that will never
    // register them
    Command::new(env::current_exe()?).env(VAR_CONTAINER, "true").spawn()?;
    tokio_main(cfg)
}
