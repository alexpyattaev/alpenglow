// Copyright (c) Anza Technology, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utilities for working with a real-world ping dataset.
//!
//! The specfic dataset is from [WonderProxy](https://wonderproxy.com/blog/a-day-in-the-life-of-the-internet/).
//! It contains ping measurements between 200+ servers all around the world.
//! These ping measurements were collected over the 24 hours of 2020-07-19.
//!
//! If not already done, the dataset can be downloaded with the following command:
//! ```bash
//! ./download_data.sh
//! ```
//!
//! # Examples
//!
//! ```
//! use alpenglow::network::simulated::ping_data::{find_closest_ping_server, get_ping};
//!
//! let berlin = find_closest_ping_server(52.516, 13.378);
//! let zurich = find_closest_ping_server(47.376, 8.547);
//! let ping_b2z = get_ping(berlin.id, zurich.id);
//! ```

use std::collections::HashMap;
use std::fs::File;
use std::sync::LazyLock;

use geo::{Distance, Haversine, Point};
use serde::Deserialize;

const MAX_PING_SERVERS: usize = 300;

static PING_SERVERS: LazyLock<Vec<PingServer>> = LazyLock::new(|| {
    let mut output = Vec::with_capacity(MAX_PING_SERVERS);
    let file = File::open("data/servers-2020-07-19.csv").unwrap();
    let mut rdr = csv::Reader::from_reader(file);
    for result in rdr.deserialize() {
        let record: PingServer = result.unwrap();
        output.push(record);
    }
    assert!(output.len() <= MAX_PING_SERVERS);
    output
});

static PING_DATA: LazyLock<Vec<f64>> = LazyLock::new(|| {
    let mut output = vec![0.0; MAX_PING_SERVERS * MAX_PING_SERVERS];
    let mut counts = vec![0; MAX_PING_SERVERS * MAX_PING_SERVERS];
    let file = File::open("data/pings-2020-07-19-2020-07-20.csv").unwrap();
    let mut rdr = csv::Reader::from_reader(file);
    for result in rdr.deserialize() {
        let record: PingMeasurement = result.unwrap();
        assert!(record.source < MAX_PING_SERVERS);
        assert!(record.destination < MAX_PING_SERVERS);
        let index = get_index(record.source, record.destination);
        let count = counts.get_mut(index).unwrap();
        if *count == 0 {
            output[index] = record.avg;
        } else {
            let avg = output[index];
            let new_avg = (avg * *count as f64 + record.avg) / (*count + 1) as f64;
            output[index] = new_avg;
        }
        *count += 1;
    }
    output
});

static COUNTRY_TO_CONTINENT: LazyLock<HashMap<String, String>> = LazyLock::new(|| {
    let mut map = HashMap::new();
    let file = File::open("data/countries_iso3166.csv").unwrap();
    let mut rdr = csv::Reader::from_reader(file);
    for result in rdr.records() {
        let record = result.unwrap();
        let alpha2 = record[1].to_owned();
        let region = record[5].to_owned();
        map.insert(alpha2, region);
    }
    map
});

/// A ping server from the dataset.
#[derive(Clone, Debug, Deserialize)]
pub struct PingServer {
    /// Server ID, to be used as `source` or `destination` in ping measurements.
    pub id: usize,
    name: String,
    title: String,
    /// City of the server.
    pub location: String,
    state: String,
    country: String,
    state_abbv: String,
    contintent: Option<u8>,
    latitude: f64,
    longitude: f64,
}

/// A ping measurement from the dataset.
#[derive(Clone, Debug, Deserialize)]
struct PingMeasurement {
    source: usize,
    destination: usize,
    timestamp: String,
    min: f64,
    avg: f64,
    max: f64,
    mdev: f64,
}

/// Gives the coordinates for a city from the ping dataset.
///
/// Returns `None` if no ping server with the given city is in the dataset.
pub fn coordinates_for_city(city: &str) -> Option<(f64, f64)> {
    PING_SERVERS.iter().find_map(|server| {
        if server.location == city {
            Some(server.coordinates())
        } else {
            None
        }
    })
}

/// Gives the ping server from the dataset that is closest to the given coordinates.
pub fn find_closest_ping_server(lat: f64, lon: f64) -> &'static PingServer {
    PING_SERVERS
        .iter()
        .min_by_key(|server| {
            let server_pos = Point::new(server.longitude, server.latitude);
            let target_pos = Point::new(lon, lat);
            Haversine.distance(server_pos, target_pos) as u64
        })
        .unwrap()
}

/// Gives the average ping from one server to another from the dataset.
///
/// Returns `None` if the servers are not in the dataset or ping measurements
/// for this specific pair are not available.
pub fn get_ping(source: usize, destination: usize) -> Option<f64> {
    let index = get_index(source, destination);
    PING_DATA.get(index).copied()
}

fn get_index(source: usize, destination: usize) -> usize {
    source * PING_SERVERS.len() + destination
}

impl PingServer {
    pub fn coordinates(&self) -> (f64, f64) {
        (self.latitude, self.longitude)
    }
}
