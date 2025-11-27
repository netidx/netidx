//! Building Sensors Publisher - Glob Pattern Discovery Example
//!
//! This example simulates a smart building monitoring system that publishes
//! sensor data across multiple floors and rooms. The publisher starts with
//! an initial set of sensors, then dynamically adds new sensors after 10 seconds
//! to demonstrate structural changes that the glob subscriber can detect.
//!
//! ## Path Structure
//!
//! Sensors are published in a hierarchical structure:
//! ```
//! /local/building/floor{N}/room{NNN}/{sensor_type}
//! ```
//!
//! ## Dynamic Expansion
//!
//! After 10 seconds, the system simulates building expansion by adding:
//! - New sensors in existing rooms
//! - New rooms on existing floors
//! - Entirely new floors
//!
//! ## Running the Example
//!
//! Start the publisher:
//! ```bash
//! cargo run --example building_sensors_publisher
//! ```
//!
//! Then in another terminal, run the subscriber with glob patterns:
//! ```bash
//! cargo run --example building_sensors_glob_subscriber "/local/building/**/temperature"
//! ```
//!
//! The subscriber will discover initial sensors, then automatically detect and
//! subscribe to new sensors as they appear.

use anyhow::Result;
use netidx::{
    config::Config,
    path::Path,
    publisher::{Publisher, PublisherBuilder, UpdateBatch, Val, Value},
};
use rand::random_range;
use tokio::{
    signal,
    time::{self, Duration},
};

/// Helper struct for publishing values with change tracking
struct Pv<T> {
    cur: T,
    val: Val,
}

impl<T: Into<Value> + Clone> Pv<T> {
    fn new(publisher: &Publisher, path: Path, v: T) -> Result<Self> {
        let val = publisher.publish(path, v.clone())?;
        Ok(Self { cur: v, val })
    }

    fn update_with<F: FnOnce(&T) -> T>(&mut self, batch: &mut UpdateBatch, f: F) {
        self.cur = f(&self.cur);
        self.val.update_changed(batch, self.cur.clone());
    }
}

/// Sensor type with embedded Pv for type-safe value tracking
enum SensorType {
    Temperature(Pv<f64>),
    Humidity(Pv<f64>),
    Occupancy(Pv<bool>),
}

/// Represents a single sensor with its name and typed value
struct Sensor {
    name: String,
    value: SensorType,
}

impl Sensor {
    fn new_temperature(
        publisher: &Publisher,
        base: &Path,
        floor: u8,
        room: &str,
        initial: f64,
    ) -> Result<Self> {
        let path = base.join(&format!("floor{}/room{}/temperature", floor, room));
        let name = format!("Floor{}-Room{}-Temp", floor, room);
        Ok(Sensor {
            name,
            value: SensorType::Temperature(Pv::new(publisher, path, initial)?),
        })
    }

    fn new_humidity(
        publisher: &Publisher,
        base: &Path,
        floor: u8,
        room: &str,
        initial: f64,
    ) -> Result<Self> {
        let path = base.join(&format!("floor{}/room{}/humidity", floor, room));
        let name = format!("Floor{}-Room{}-Humidity", floor, room);
        Ok(Sensor {
            name,
            value: SensorType::Humidity(Pv::new(publisher, path, initial)?),
        })
    }

    fn new_occupancy(
        publisher: &Publisher,
        base: &Path,
        floor: u8,
        room: &str,
        initial: bool,
    ) -> Result<Self> {
        let path = base.join(&format!("floor{}/room{}/occupancy", floor, room));
        let name = format!("Floor{}-Room{}-Occupancy", floor, room);
        Ok(Sensor {
            name,
            value: SensorType::Occupancy(Pv::new(publisher, path, initial)?),
        })
    }

    // For special locations like hallways, roof, etc.
    fn new_temperature_at(
        publisher: &Publisher,
        base: &Path,
        location: &str,
        name: &str,
        initial: f64,
    ) -> Result<Self> {
        let path = base.join(&format!("{}/temperature", location));
        Ok(Sensor {
            name: name.to_string(),
            value: SensorType::Temperature(Pv::new(publisher, path, initial)?),
        })
    }

    fn update(&mut self, batch: &mut UpdateBatch) {
        match &mut self.value {
            SensorType::Temperature(pv) => {
                // Random walk for temperature
                pv.update_with(batch, |temp| {
                    (temp + random_range(-0.5..0.5)).clamp(15.0, 30.0)
                });
            }
            SensorType::Humidity(pv) => {
                // Random walk for humidity
                pv.update_with(batch, |humidity| {
                    (humidity + random_range(-2.0..2.0)).clamp(20.0, 80.0)
                });
            }
            SensorType::Occupancy(pv) => {
                // Occasionally toggle occupancy
                if random_range(0..100) < 5 {
                    pv.update_with(batch, |occupied| !occupied);
                }
            }
        }
    }
}

fn create_initial_sensors(publisher: &Publisher, base: &Path) -> Result<Vec<Sensor>> {
    let mut sensors = Vec::new();

    println!("\n=== Creating Initial Sensor Network ===");

    // Floor 1 - Room 101
    sensors.push(Sensor::new_temperature(publisher, base, 1, "101", 21.5)?);
    sensors.push(Sensor::new_humidity(publisher, base, 1, "101", 45.0)?);
    sensors.push(Sensor::new_occupancy(publisher, base, 1, "101", false)?);

    // Floor 1 - Room 102
    sensors.push(Sensor::new_temperature(publisher, base, 1, "102", 22.0)?);
    sensors.push(Sensor::new_humidity(publisher, base, 1, "102", 48.0)?);

    // Floor 1 - Hallway
    sensors.push(Sensor::new_temperature_at(
        publisher,
        base,
        "floor1/hallway",
        "Floor1-Hallway-Temp",
        20.0,
    )?);

    // Floor 2 - Room 201
    sensors.push(Sensor::new_temperature(publisher, base, 2, "201", 21.0)?);
    sensors.push(Sensor::new_occupancy(publisher, base, 2, "201", true)?);

    // Floor 2 - Room 202
    sensors.push(Sensor::new_temperature(publisher, base, 2, "202", 21.5)?);
    sensors.push(Sensor::new_humidity(publisher, base, 2, "202", 50.0)?);

    println!("✓ Created {} initial sensors", sensors.len());
    println!("\nInitial sensor paths:");
    for sensor in &sensors {
        println!("  {}", sensor.name);
    }

    Ok(sensors)
}

fn add_expansion_sensors(publisher: &Publisher, base: &Path) -> Result<Vec<Sensor>> {
    let mut sensors = Vec::new();

    println!("\n=== Building Expansion Detected! ===");
    println!("Adding new sensors...\n");

    // Add humidity sensor to existing room that didn't have one
    sensors.push(Sensor::new_humidity(publisher, base, 2, "201", 47.0)?);
    println!("✓ Added humidity sensor to Floor 2 Room 201");

    // Add new conference room on floor 2 (special location)
    sensors.push(Sensor::new_temperature_at(
        publisher,
        base,
        "floor2/conference_room",
        "Floor2-ConferenceRoom-Temp",
        22.5,
    )?);
    sensors.push(Sensor::new_humidity(publisher, base, 2, "conference_room", 45.0)?);
    sensors.push(Sensor::new_occupancy(publisher, base, 2, "conference_room", true)?);
    println!("✓ Added new conference room on Floor 2");

    // Add entirely new floor
    sensors.push(Sensor::new_temperature(publisher, base, 3, "301", 20.5)?);
    sensors.push(Sensor::new_humidity(publisher, base, 3, "301", 46.0)?);
    println!("✓ Added new Floor 3 with Room 301");

    // Add roof sensors (new area)
    sensors.push(Sensor::new_temperature_at(
        publisher,
        base,
        "roof/solar_panel",
        "Roof-SolarPanel-Temp",
        35.0,
    )?);
    println!("✓ Added rooftop solar panel sensor");

    println!("\nAdded {} new sensors", sensors.len());
    println!("Subscribers with change trackers should detect these automatically!\n");

    Ok(sensors)
}

#[tokio::main]
async fn tokio_main(cfg: Config) -> Result<()> {
    println!("╔════════════════════════════════════════════════╗");
    println!("║  Building Sensors Publisher                    ║");
    println!("║  Dynamic Sensor Network Demonstration          ║");
    println!("╚════════════════════════════════════════════════╝");

    let base = Path::from("/local/building");
    let publisher = PublisherBuilder::new(cfg).build().await?;

    // Create initial sensors
    let mut sensors = create_initial_sensors(&publisher, &base)?;

    println!("\n=== Publishing Updates Every 2 Seconds ===");
    println!("In 10 seconds, new sensors will be added dynamically...\n");

    let start_time = time::Instant::now();
    let mut expansion_added = false;

    loop {
        tokio::select! {
            biased;
            _ = signal::ctrl_c() => break Ok(()),
            _ = time::sleep(Duration::from_secs(2)) => (),
        }

        // After 10 seconds, add expansion sensors
        if !expansion_added && start_time.elapsed() >= Duration::from_secs(10) {
            let new_sensors = add_expansion_sensors(&publisher, &base)?;
            sensors.extend(new_sensors);
            expansion_added = true;
        }

        // Update all sensors
        let mut batch = publisher.start_batch();
        for sensor in &mut sensors {
            sensor.update(&mut batch);
        }
        batch.commit(None).await;
    }
}

fn main() -> Result<()> {
    env_logger::init();
    Config::maybe_run_machine_local_resolver()?;
    tokio_main(Config::load_default_or_local_only()?)
}
