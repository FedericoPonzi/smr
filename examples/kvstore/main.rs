#[macro_use]
extern crate rocket;

use crate::kvstore::{InnerStateMachine, KeyValueStore};

use rocket::http::Status;
use rocket::response::status;
use rocket::serde::json::Json;
use rocket::{Config, State};
use smr::multipaxos::storage::{CommandLog, PaxosStorage};
use smr::storage::SledStorage;
use smr::{SmrConfig, SmrRuntime};

mod kvstore;

#[get("/<item_key>")]
async fn get_item(
    item_key: &str,
    state_kvstore: &State<KeyValueStore>,
) -> Result<Json<String>, status::Custom<Json<String>>> {
    match state_kvstore.get(item_key).await {
        Ok(Some(value)) => Ok(Json(value)),
        Ok(None) => Err(status::Custom(
            Status::NotFound,
            Json(format!("Key '{}' not found", item_key)),
        )),
        Err(err) => Err(status::Custom(
            Status::InternalServerError,
            Json(format!("Error retrieving key: {}", err)),
        )),
    }
}

#[post("/<item_key>", data = "<item_value>")]
async fn set_item(
    item_key: &str,
    item_value: &str,
    state_kvstore: &State<KeyValueStore>,
) -> Result<Status, status::Custom<Json<String>>> {
    state_kvstore.set(item_key, item_value).await.map_err(|e| {
        status::Custom(
            Status::InternalServerError,
            Json(format!("Error proposing command: {}", e)),
        )
    })?;
    Ok(Status::Ok)
}

#[get("/")]
fn index() -> &'static str {
    "Hello, world!\n"
}

#[launch]
async fn rocket() -> _ {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let config = SmrConfig::from_cli_args().expect("Failed to load config");
    let nid = config.node_id as u16;

    let data_dir = format!("data/kvstore-node-{}", nid);
    std::fs::create_dir_all(&data_dir).expect("Failed to create data directory");

    let paxos_db = SledStorage::open(std::path::Path::new(&format!("{}/paxos", data_dir)))
        .expect("Failed to open paxos storage");
    let log_db = SledStorage::open(std::path::Path::new(&format!("{}/log", data_dir)))
        .expect("Failed to open command log");

    let paxos_storage = PaxosStorage::new(Box::new(paxos_db));
    let command_log = CommandLog::new(Box::new(log_db));

    let smr_runtime =
        SmrRuntime::with_storage(config, InnerStateMachine::new(), paxos_storage, command_log)
            .unwrap();
    let state_kvstore = KeyValueStore::new(smr_runtime);

    let http_port = std::env::var("ROCKET_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8080 + nid);

    let rocket_config = Config {
        port: http_port,
        address: "0.0.0.0".parse().unwrap(),
        ..Config::default()
    };

    rocket::custom(rocket_config)
        .manage(state_kvstore)
        .mount("/", routes![index, get_item, set_item])
}
