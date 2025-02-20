use chrono::{DateTime, Utc};
use serde::{self, Deserialize, Deserializer, Serializer};

const FORMAT: &str = "%Y-%m-%d %H:%M:%S %z";

pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let datetime_str = String::deserialize(deserializer)?;
    DateTime::parse_from_str(&datetime_str, "%Y-%m-%d %H:%M:%S%.f %z %Z")
        .map_err(serde::de::Error::custom)
        .map(DateTime::<Utc>::from)
}

pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let s = format!("{}", date.format(FORMAT));
    serializer.serialize_str(&s)
}
