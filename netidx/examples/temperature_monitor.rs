//! Temperature monitoring publisher
//!
//! This example simulates a temperature sensor that publishes readings from
//! multiple sensors with statistics. You can look at the output with,
//!
//! ```
//! netidx browser
//! ```
//!
//! or run
//!
//! ```
//! cargo run --example temperature_display`
//! ```
//!
//! Run this with:
//! ```
//! cargo run --example temperature_monitor
//! ```

use anyhow::Result;
use netidx::{
    config::Config,
    path::Path,
    publisher::{Publisher, PublisherBuilder, UpdateBatch, Val, Value},
};
use rand::random_range;
use tokio::time::{self, Duration};

struct Pv<T> {
    cur: T,
    val: Val,
}

impl<T: Into<Value> + Clone> Pv<T> {
    fn new(publisher: &Publisher, path: Path, v: T) -> Result<Self> {
        let val = publisher.publish(path, v.clone())?;
        Ok(Self { cur: v, val })
    }

    fn update(&mut self, batch: &mut UpdateBatch, v: T) {
        self.cur = v.clone();
        // only update the value if it changed
        self.val.update_changed(batch, v);
    }
}

struct Sensor {
    base: Path,
    count: usize,
    sum: f64,
    temperature: Pv<f64>,
    min: Pv<f64>,
    max: Pv<f64>,
    avg: Pv<f64>,
}

impl Sensor {
    fn new(publisher: &Publisher, base: Path, temp: f64) -> anyhow::Result<Self> {
        Ok(Sensor {
            base: base.clone(),
            count: 1,
            sum: temp,
            temperature: Pv::new(publisher, base.join("current"), temp)?,
            min: Pv::new(publisher, base.join("min"), temp)?,
            max: Pv::new(publisher, base.join("max"), temp)?,
            avg: Pv::new(publisher, base.join("avg"), temp)?,
        })
    }

    fn update(&mut self, batch: &mut UpdateBatch) {
        // Simulate temperature fluctuation
        let variation = random_range(-0.1..0.1);
        let temp = self.temperature.cur + variation;
        self.temperature.update(batch, temp);
        self.sum += temp;
        self.count += 1;
        self.avg.update(batch, self.sum / (self.count as f64));
        self.min.update(batch, f64::min(self.min.cur, temp));
        self.max.update(batch, f64::max(self.max.cur, temp));
        println!("{}: {:.1}°C", Path::basename(&self.base).unwrap_or(""), temp);
    }
}

#[tokio::main]
async fn tokio_main(cfg: Config) -> anyhow::Result<()> {
    let base = Path::from("/local/sensors/temperature");
    let publisher = PublisherBuilder::new(cfg).build().await?;
    println!("Temperature monitoring system started");
    println!("Publishing sensor data under {base}");
    // Create sensors for different rooms
    let mut sensors = vec![
        Sensor::new(&publisher, base.join("zones/living_room"), 21.5)?,
        Sensor::new(&publisher, base.join("zones/bedroom"), 19.0)?,
        Sensor::new(&publisher, base.join("zones/kitchen"), 22.0)?,
        Sensor::new(&publisher, base.join("zones/outside"), 15.0)?,
    ];
    // Publish a status indicator
    let status = publisher.publish(base.join("system/status"), "running")?;
    let sensor_count = publisher.publish(base.join("system/count"), sensors.len())?;
    loop {
        time::sleep(Duration::from_secs(2)).await;
        let mut batch = publisher.start_batch();
        // Update all sensors
        for sensor in &mut sensors {
            sensor.update(&mut batch);
        }
        status.update_changed(&mut batch, "running");
        sensor_count.update_changed(&mut batch, sensors.len());
        // all the sensor data that changed in this batch is guaranteed to
        // arrive at every subscriber together in the same batch. Preservation
        // of batch structure from publisher to subscriber is an important
        // netidx feature.
        batch.commit(None).await;
    }
}

fn main() -> Result<()> {
    // start logging
    env_logger::init();
    // maybe start the local machine resolver
    Config::maybe_run_machine_local_resolver()?;
    // load the config and go
    tokio_main(Config::load_default_or_local_only()?)
}
