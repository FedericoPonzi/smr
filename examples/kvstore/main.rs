#[macro_use]
extern crate rocket;

use crate::kvstore::{Command, InnerStateMachine, KeyValueStore};

use rocket::http::Status;
use rocket::response::status;
use rocket::serde::json::Json;
use rocket::yansi::Paint;
use rocket::State;
use smr::{SmrConfig, SmrRuntime};
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;

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

    let other_nodes: Vec<SocketAddr> = (0..3)
        .filter(|&n| n != node_id)
        .map(|n| SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), ports[n as usize]))
        .collect();

    SmrConfig::new(node_id as u32, Some(bind_address), other_nodes) // Use bind_address
}

#[get("/<item_key>")]
async fn get_item(
    item_key: &str,
    state_kvstore: &State<Arc<Mutex<KeyValueStore>>>,
) -> Result<Json<String>, status::Custom<Json<String>>> {
    let kvstore = state_kvstore.lock().await;
    match kvstore.get(item_key).await {
        // Use .await here as well
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
    state_kvstore: &State<Arc<Mutex<KeyValueStore>>>,
    smr_runtime: &State<SmrRuntime<InnerStateMachine>>,
) -> Result<Status, status::Custom<Json<String>>> {
    let cmd = Command::Set {
        // Use the imported Command
        key: item_key.to_string(),
        value: item_value.to_string(),
    };

    let rx = smr_runtime.propose(cmd).await.map_err(|e| {
        status::Custom(
            Status::InternalServerError,
            Json(format!("Error proposing command: {}", e)),
        )
    })?;

    let result = rx.await.map_err(|e| {
        status::Custom(
            Status::InternalServerError,
            Json(format!("Error waiting for result: {}", e)),
        )
    })?;

    match result {
        Ok(_) => Ok(Status::Ok),
        Err(err) => Err(status::Custom(
            Status::InternalServerError,
            Json(format!("Error setting key: {}", err)),
        )),
    }
}

#[get("/")]
fn index() -> &'static str {
    "Hello, world!\n"
}

#[launch]
async fn rocket() -> _ {
    let config = config().expect("Failed to load config");
    let state_kvstore = Arc::new(Mutex::new(KeyValueStore::default())); // Initialize Arc<Mutex>

    let smr_runtime = SmrRuntime::new(config, KeyValueStore::default())
        .await
        .expect("Failed to create SMR runtime");
    let smr_runtime_state = State::new(smr_runtime);

    let runtime_clone = smr_runtime_state.clone();
    tokio::spawn(async move {
        runtime_clone.run().await.unwrap();
    });

    rocket::build()
        .manage(state_kvstore) // Manage the Arc<Mutex>
        .manage(smr_runtime_state)
        .mount("/", routes![index, get_item, set_item])
}
