#[macro_use]
extern crate rocket;

use crate::counter::CounterStore;

use rocket::http::Status;
use rocket::response::status;
use rocket::serde::json::Json;
use rocket::{Config, State};
use smr::{SmrConfig, SmrRuntime};

mod counter;

#[post("/increment")]
async fn increment(store: &State<CounterStore>) -> Result<Json<i64>, status::Custom<Json<String>>> {
    store
        .increment()
        .await
        .map(Json)
        .map_err(|e| status::Custom(Status::InternalServerError, Json(format!("Error: {}", e))))
}

#[post("/decrement")]
async fn decrement(store: &State<CounterStore>) -> Result<Json<i64>, status::Custom<Json<String>>> {
    store
        .decrement()
        .await
        .map(Json)
        .map_err(|e| status::Custom(Status::InternalServerError, Json(format!("Error: {}", e))))
}

#[get("/value")]
async fn value(store: &State<CounterStore>) -> Result<Json<i64>, status::Custom<Json<String>>> {
    store
        .get()
        .await
        .map(Json)
        .map_err(|e| status::Custom(Status::InternalServerError, Json(format!("Error: {}", e))))
}

#[get("/")]
fn index() -> &'static str {
    "Distributed Counter\n\nEndpoints:\n  POST /increment\n  POST /decrement\n  GET /value\n"
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
    let smr_runtime = SmrRuntime::new(config, counter::CounterStateMachine::new()).unwrap();
    let store = CounterStore::new(smr_runtime);

    let http_port = std::env::var("ROCKET_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9080 + nid);

    let rocket_config = Config {
        port: http_port,
        address: "0.0.0.0".parse().unwrap(),
        ..Config::default()
    };

    rocket::custom(rocket_config)
        .manage(store)
        .mount("/", routes![index, increment, decrement, value])
}
