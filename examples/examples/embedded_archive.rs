//! Embedded Archive Example
//!
//! This example demonstrates how to embed netidx-archive directly in your application
//! for high-performance time-series data recording and playback. The key feature shown
//! here is that you can write to the archive AND publish via the recorder simultaneously.
//!
//! This example:
//! 1. Starts a recorder to publish the archive
//! 2. Gets the archive writer from the recorder's shards
//! 3. Records synthetic sensor data while SIMULTANEOUSLY publishing it
//! 4. Shows tail-mode playback to see live updates as they're written
//!
//! Run this with:
//! ```
//! cargo run --example embedded_archive
//! ```

use anyhow::Result;
use chrono::prelude::*;
use futures::{channel::mpsc, prelude::*};
use netidx::{
    config::Config,
    path::Path,
    publisher::PublisherBuilder,
    subscriber::{Event, Subscriber, SubscriberBuilder, UpdatesFlags},
};
use netidx_archive::{
    config::{ConfigBuilder, PublishConfigBuilder, RecordConfigBuilder},
    logfile::{BatchItem, Seek, BATCH_POOL},
    logfile_collection::ArchiveCollectionWriter,
    recorder::{BCastMsg, Recorder, State},
    recorder_client::{Client, Speed},
};
use netidx_netproto::glob::GlobSet;
use netidx_value::Value;
use rand::random_range;
use std::sync::Arc;
use tempdir::TempDir;
use tokio::{
    signal,
    sync::{broadcast, oneshot},
    task::{self, JoinHandle},
    time::{self, Duration},
};

const SHARD: &str = "sensors";
const BASE: &str = "/local/archive/sensors";
const TEMPERATURE_PATH: &str = "/sensors/temperature";
const HUMIDITY_PATH: &str = "/sensors/humidity";
const PRESSURE_PATH: &str = "/sensors/pressure";

struct SensorLogger {
    _recorder: Recorder,
    subscriber: Subscriber,
    stop: oneshot::Sender<()>,
    log_task: JoinHandle<()>,
}

impl SensorLogger {
    async fn log_task(
        mut writer: ArchiveCollectionWriter,
        mut stop: oneshot::Receiver<()>,
        bcast: broadcast::Sender<BCastMsg>,
    ) -> Result<()> {
        // Register the paths we'll be recording
        let paths = [
            Path::from(TEMPERATURE_PATH),
            Path::from(HUMIDITY_PATH),
            Path::from(PRESSURE_PATH),
        ];

        writer.add_paths(paths.iter())?;
        writer.flush_pathindex()?;

        // Get IDs for each path
        let temp_id = writer.id_for_path(&paths[0]).unwrap();
        let humidity_id = writer.id_for_path(&paths[1]).unwrap();
        let pressure_id = writer.id_for_path(&paths[2]).unwrap();

        println!("\n=== Starting Data Recording ===");
        println!("Recording will happen concurrently with playback!\n");

        let start = Utc::now();
        let mut sample_count = 0;

        // our initial samples
        let mut temp = 20.;
        let mut humidity = 45.;
        let mut pressure = 1013.;

        // log data until we are told to stop
        loop {
            let timestamp = Utc::now();

            // Simulate sensor readings
            temp = f64::clamp(temp + random_range(-0.05..0.05), 15., 25.);
            humidity = f64::clamp(humidity + random_range(-1.0..1.0), 35., 90.);
            pressure = f64::clamp(pressure + random_range(-1.0..1.0), 915., 1150.);

            // Create a batch of sensor readings
            let mut batch = BATCH_POOL.take();
            batch.push(BatchItem(temp_id, Event::Update(Value::F64(temp))));
            batch.push(BatchItem(humidity_id, Event::Update(Value::F64(humidity))));
            batch.push(BatchItem(pressure_id, Event::Update(Value::F64(pressure))));

            // Write the batch to the archive
            writer.add_batch(false, timestamp, &batch).unwrap();

            // CRITICAL: Also send to broadcast channel for real-time publishing
            // This is what enables tail mode and concurrent read/write
            let batch_arc = Arc::new(batch);
            let _ = bcast.send(BCastMsg::Batch(timestamp, batch_arc));

            // Flush periodically
            if sample_count % 10 == 0 {
                writer.flush_current().unwrap();
                println!("  Recorded {} samples...", sample_count);
            }

            sample_count += 1;
            tokio::select! {
                _ = &mut stop => break,
                _ = time::sleep(Duration::from_millis(500)) => (),
            }
        }

        writer.flush_current().unwrap();
        let duration = Utc::now() - start;
        println!("\n=== Recording Complete ===");
        println!(
            "Recorded {} samples in {} seconds\n",
            sample_count,
            duration.num_seconds()
        );
        Ok(())
    }

    async fn start(cfg: Config, archive_dir: &str) -> Result<Self> {
        // Create publisher and subscriber for the recorder
        let publisher = PublisherBuilder::new(cfg.clone()).build().await?;
        let subscriber = SubscriberBuilder::new(cfg).build()?;

        // Configure the recorder to publish our archive
        // The spec is empty because we're recording internally (not subscribing to paths)
        // We still need a record config so the shard will be published
        let record_cfg =
            RecordConfigBuilder::default().spec(GlobSet::new(true, [])?).build()?;

        let publish_cfg =
            PublishConfigBuilder::default().bind("local".parse()?).base(BASE).build()?;

        let recorder_cfg = ConfigBuilder::default()
            .record([(SHARD.into(), record_cfg)])
            .publish(publish_cfg)
            .archive_directory(archive_dir)
            .build()?;

        // Start the recorder FIRST
        println!("=== Starting Recorder ===\n");
        let recorder = Recorder::start_with(
            recorder_cfg,
            Some(publisher.clone()),
            Some(subscriber.clone()),
        )
        .await?;

        // Get the writer and broadcast channel from the recorder
        let shard_id = *recorder
            .shards
            .by_name
            .get(SHARD)
            .ok_or_else(|| anyhow::anyhow!("shard not found"))?;

        let writer = recorder
            .shards
            .writers
            .lock()
            .remove(&shard_id)
            .ok_or_else(|| anyhow::anyhow!("writer not found"))?;

        let bcast = recorder
            .shards
            .bcast
            .get(&shard_id)
            .ok_or_else(|| anyhow::anyhow!("broadcast channel not found"))?
            .clone();

        println!("Got archive writer and broadcast channel");
        let (stop, rx_stop) = oneshot::channel();
        let log_task = task::spawn(async move {
            if let Err(e) = Self::log_task(writer, rx_stop, bcast).await {
                eprintln!("log task failed {e:?}")
            }
        });
        Ok(Self { _recorder: recorder, subscriber, stop, log_task })
    }

    async fn stop(self) -> Result<()> {
        let _ = self.stop.send(());
        Ok(self.log_task.await?)
    }
}

#[tokio::main]
async fn tokio_main(cfg: Config) -> Result<()> {
    println!("\n╔═══════════════════════════════════════════════╗");
    println!("║  Embedded Archive Example                     ║");
    println!("║  Concurrent Write & Publish                   ║");
    println!("╚═══════════════════════════════════════════════╝\n");

    // Create a temporary directory for the archive
    let temp_dir = TempDir::new("netidx-archive-example")?;
    let archive_dir = temp_dir.path().to_str().unwrap();
    println!("Archive directory: {}\n", archive_dir);

    let sensor_logger = SensorLogger::start(cfg.clone(), archive_dir).await?;

    // Set up a tail-mode playback session to see live updates
    println!("=== Starting Tail-Mode Playback ===");
    println!("This will show updates as they're being written!\n");

    let client = Client::new(&sensor_logger.subscriber, &Path::from(BASE))?;

    // Create a session that will start at the beginning and then
    // enter TAIL mode once it's sent all the historical data - this
    // follows live updates
    let session =
        client.session().pos(Seek::Beginning)?.speed(Speed::Unlimited).build().await?;

    println!("Session created, subscribing to live data...");

    // Subscribe to the sensor data
    let (tx, mut rx) = mpsc::channel(10);
    let flags = [(UpdatesFlags::empty(), tx)];

    let temp_sub = sensor_logger
        .subscriber
        .subscribe_updates(session.data_path().append(TEMPERATURE_PATH), flags.clone());
    let humidity_sub = sensor_logger
        .subscriber
        .subscribe_updates(session.data_path().append(HUMIDITY_PATH), flags.clone());
    let pressure_sub = sensor_logger
        .subscriber
        .subscribe_updates(session.data_path().append(PRESSURE_PATH), flags);

    tokio::try_join![
        temp_sub.wait_subscribed(),
        humidity_sub.wait_subscribed(),
        pressure_sub.wait_subscribed()
    ]?;

    println!("Subscribed! Starting playback...\n");
    // tell the session to play
    session.set_state(State::Play).await?;

    // Collect and display live updates
    let mut batch_count = 0;

    loop {
        tokio::select! {
            biased;
            _ = signal::ctrl_c() => {
                println!("\n=== Exit Requested ===");
                println!("Received {} batches", batch_count);
                break;
            }
            Some(batch) = rx.next() => {
                batch_count += 1;
                print!("Batch {}: ", batch_count);
                for (_, event) in &*batch {
                    if let Event::Update(value) = event {
                        match value {
                            Value::F64(v) => print!("{:.2} ", v),
                            Value::Null => print!("null "),
                            _ => print!("{:?} ", value),
                        }
                    }
                }
                println!();
            }
        }
    }

    println!("\n╔═══════════════════════════════════════════════╗");
    println!("║  Success!                                     ║");
    println!("║  Demonstrated concurrent write & publish      ║");
    println!("║  Archive files will be cleaned up on exit     ║");
    println!("╚═══════════════════════════════════════════════╝\n");

    sensor_logger.stop().await
}

fn main() -> Result<()> {
    // init logging
    env_logger::init();
    // maybe start the machine local resolver
    Config::maybe_run_machine_local_resolver()?;
    // Load the netidx config
    tokio_main(Config::load_default_or_local_only()?)
}
