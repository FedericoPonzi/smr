#[macro_use]
extern crate rocket;

use crate::kvstore::{InnerStateMachine, KeyValueStore};

use rocket::http::Status;
use rocket::response::status;
use rocket::serde::json::Json;
use rocket::yansi::Paint;
use rocket::{Config, State};
use smr::{SmrConfig, SmrRuntime};

mod kvstore;

fn config() -> anyhow::Result<smr::SmrConfig> {
    use std::env;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    let args: Vec<String> = env::args().collect();

    if args.len() != 3 {
        anyhow::bail!("Usage: {} <node_id> <port1,port2,port3>", args[0]);
    }

    let node_id: u16 = args[1]
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid node ID"))?;

    if node_id >= 3 {
        anyhow::bail!("Node ID must be 0, 1, or 2");
    }

    let ports_str = &args[2];
    let ports: Vec<u16> = ports_str
        .split(',')
        .map(|s| {
            s.parse()
                .map_err(|_| anyhow::anyhow!("Invalid port number"))
        })
        .collect::<Result<_, _>>()?;

    if ports.len() != 3 {
        anyhow::bail!("Must provide 3 port numbers separated by commas");
    }

    let bind_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), ports[node_id as usize]);

    let other_nodes = (0..3)
        .filter(|&n| n != node_id)
        .map(|n| {
            (
                n as u32,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), ports[n as usize]).to_string(),
            )
        })
        .collect();

    SmrConfig::new(node_id as u32, Some(bind_address.to_string()), other_nodes)
}

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
    let config = config().expect("Failed to load config");
    let nid = config.node_id as u16;
    let smr_runtime = SmrRuntime::new(config, InnerStateMachine::new()).unwrap();
    let state_kvstore = KeyValueStore::new(smr_runtime); // Initialize Arc<Mutex>

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
