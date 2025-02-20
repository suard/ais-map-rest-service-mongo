use serde::{Deserialize, Deserializer};

pub fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(String::deserialize(deserializer)?.trim().to_string())
}
