mod config;
mod messages;

use crate::config::Config;
use crate::messages::position_report::PositionReport;
use axum::extract::Path;
use axum::{extract, http::StatusCode, routing::get, Json, Router};
use extract::State;
use log::info;
use mongodb::bson::doc;
use mongodb::options::ClientOptions;
use mongodb::{Client, Collection};
use serde_env::from_env;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    dotenvy::dotenv()?;
    env_logger::init();

    info!("Starting AIS Map Service REST Client");

    let configuration: Config = from_env()?;

    let client = Client::with_options(ClientOptions::parse(configuration.mongodb_url).await?)?;

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app(client)).await.unwrap();

    Ok(())
}

fn app(client: Client) -> Router {
    let collection: Collection<PositionReport> =
        client.database("ais_map").collection("position_reports");

    Router::new()
        .route("/hello", get(hello_handler))
        .route("/ship/{id}", get(fetch_ship_handler))
        .with_state(collection)
}

async fn hello_handler() -> &'static str {
    "world!"
}

// handler to read an existing member
async fn fetch_ship_handler(
    State(collection): State<Collection<PositionReport>>,
    Path(id): Path<u32>,
) -> Result<Json<Option<PositionReport>>, (StatusCode, String)> {

    info!("fetching ship with MMSI: {}", id);

    let result = collection
        .find_one(doc! { "MetaData.MMSI": id })
        .await
        .map_err(internal_error)?;

    Ok(Json(result))
}

fn internal_error<E>(err: E) -> (StatusCode, String)
where
    E: std::error::Error,
{
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}
