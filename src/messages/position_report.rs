use serde::{Deserialize, Serialize};

/**
 * @todo make message_type enum
 * @todo flatten message property
 */

#[derive(Deserialize, Debug, Serialize)]
#[allow(dead_code)]
pub struct PositionReport {
    #[serde(rename = "Message")]
    message: Message,
    #[serde(rename = "MessageType")]
    message_type: String,
    #[serde(rename = "MetaData")]
    meta_data: super::meta_data::MetaData,
}

#[derive(Deserialize, Debug, Serialize)]
#[allow(dead_code)]
pub struct Message {
    #[serde(rename = "PositionReport")]
    position_report: PositionReportMessage,
}

#[derive(Deserialize, Debug, Serialize)]
#[allow(dead_code)]
struct PositionReportMessage {
    #[serde(rename = "Cog")]
    cog: f32,
    #[serde(rename = "CommunicationState")]
    communication_state: u32,
    #[serde(rename = "Latitude")]
    latitude: f32,
    #[serde(rename = "Longitude")]
    longitude: f32,
    #[serde(rename = "MessageID")]
    message_id: u32,
    #[serde(rename = "NavigationalStatus")]
    navigational_status: u32,
    #[serde(rename = "PositionAccuracy")]
    position_accuracy: bool,
    #[serde(rename = "Raim")]
    raim: bool,
    #[serde(rename = "RateOfTurn")]
    rate_of_turn: i32,
    #[serde(rename = "RepeatIndicator")]
    repeat_indicator: u32,
    #[serde(rename = "Sog")]
    sog: f64,
    #[serde(rename = "Spare")]
    spare: u32,
    #[serde(rename = "SpecialManoeuvreIndicator")]
    special_manoeuvre_indicator: u32,
    #[serde(rename = "Timestamp")]
    timestamp: u32,
    #[serde(rename = "TrueHeading")]
    true_heading: u32,
    #[serde(rename = "UserID")]
    user_id: u32,
    #[serde(rename = "Valid")]
    valid: bool,
}
