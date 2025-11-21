//! Temperature monitoring publisher
//!
//! This example simulates a temperature sensor that publishes readings
//! from multiple sensors with statistics.
//!
//! Run this with:
//! ```
//! cargo run --example temperature_monitor
//! ```

use netidx::{
    config::Config,
    path::Path,
    publisher::{DesiredAuth, Publisher, PublisherBuilder, Value},
};
use rand::Rng;
use std::collections::HashMap;
use tokio::time::{self, Duration};

struct Sensor {
    name: String,
    base_temp: f64,
    temperature: netidx::publisher::Val,
    min: netidx::publisher::Val,
    max: netidx::publisher::Val,
    avg: netidx::publisher::Val,
}

impl Sensor {
    fn new(
        publisher: &Publisher,
        name: &str,
        base_temp: f64,
    ) -> anyhow::Result<Self> {
        let base_path = format!("/sensors/temperature/{}", name);

        Ok(Sensor {
            name: name.to_string(),
            base_temp,
            temperature: publisher.publish(
                Path::from(format!("{}/current", base_path)),
                Value::F64(base_temp),
            )?,
            min: publisher.publish(
                Path::from(format!("{}/min", base_path)),
                Value::F64(base_temp),
            )?,
            max: publisher.publish(
                Path::from(format!("{}/max", base_path)),
                Value::F64(base_temp),
            )?,
            avg: publisher.publish(
                Path::from(format!("{}/avg", base_path)),
                Value::F64(base_temp),
            )?,
        })
    }

    fn update(&mut self, batch: &mut netidx::publisher::UpdateBatch) {
        let mut rng = rand::thread_rng();

        // Simulate temperature fluctuation
        let variation = rng.gen_range(-2.0..2.0);
        let temp = self.base_temp + variation;

        self.temperature.update(batch, Value::F64(temp));

        println!("{}: {:.1}°C", self.name, temp);
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::load_default()?;

    let publisher = PublisherBuilder::new(cfg)
        .desired_auth(DesiredAuth::Anonymous)
        .build()
        .await?;

    println!("Temperature monitoring system started");
    println!("Publishing sensor data under /sensors/temperature/");

    // Create sensors for different rooms
    let mut sensors = vec![
        Sensor::new(&publisher, "living_room", 21.5)?,
        Sensor::new(&publisher, "bedroom", 19.0)?,
        Sensor::new(&publisher, "kitchen", 22.0)?,
        Sensor::new(&publisher, "outside", 15.0)?,
    ];

    // Publish a status indicator
    let status = publisher.publish(
        Path::from("/sensors/temperature/system/status"),
        Value::String("running".into()),
    )?;

    let sensor_count = publisher.publish(
        Path::from("/sensors/temperature/system/count"),
        Value::U32(sensors.len() as u32),
    )?;

    loop {
        time::sleep(Duration::from_secs(2)).await;

        let mut batch = publisher.start_batch();

        // Update all sensors
        for sensor in &mut sensors {
            sensor.update(&mut batch);
        }

        status.update(&mut batch, Value::String("running".into()));

        batch.commit(None).await;
    }
}
