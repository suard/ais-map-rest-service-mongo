use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, PartialEq)]
enum MessageType {
    PositionReport,
    ShipStaticData,
}

#[derive(Deserialize, Debug, Serialize)]
#[allow(dead_code)]
pub struct MetaData {
    #[serde(rename = "MMSI")]
    mmsi: u32,
    #[serde(
        rename = "ShipName",
        deserialize_with = "super::trim_string::deserialize"
    )]
    ship_name: String,
    #[serde(rename = "latitude")]
    latitude: f32,
    #[serde(rename = "longitude")]
    longitude: f32,
    #[serde(rename = "time_utc", with = "super::datetime")]
    time_utc: DateTime<Utc>,
}

#[derive(Deserialize, Debug, Serialize)]
#[allow(dead_code)]
pub struct Dimension {
    #[serde(rename = "A")]
    a: u16,
    #[serde(rename = "B")]
    b: u16,
    #[serde(rename = "C")]
    c: u16,
    #[serde(rename = "D")]
    d: u16,
}
