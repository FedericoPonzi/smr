use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(60);

const SMR_PORTS: [u16; 3] = [29100, 29101, 29102];
const HTTP_PORTS: [u16; 3] = [29200, 29201, 29202];

struct HttpResponse {
    status: u16,
    body: String,
}

fn http_request(port: u16, request: &str, read_timeout: Duration) -> HttpResponse {
    let stream = TcpStream::connect_timeout(
        &format!("127.0.0.1:{}", port).parse().unwrap(),
        Duration::from_secs(2),
    );
    let mut stream = match stream {
        Ok(s) => s,
        Err(_) => {
            return HttpResponse {
                status: 0,
                body: "connection refused".to_string(),
            };
        }
    };
    stream.set_read_timeout(Some(read_timeout)).unwrap();
    if stream.write_all(request.as_bytes()).is_err() {
        return HttpResponse {
            status: 0,
            body: "write failed".to_string(),
        };
    }
    let mut response = String::new();
    let _ = stream.read_to_string(&mut response);
    parse_response(&response)
}

fn http_get(port: u16, path: &str) -> HttpResponse {
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
        path
    );
    http_request(port, &request, Duration::from_secs(15))
}

fn http_post(port: u16, path: &str, body: &str) -> HttpResponse {
    let request = format!(
        "POST {} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        path,
        body.len(),
        body
    );
    http_request(port, &request, Duration::from_secs(15))
}

fn parse_response(raw: &str) -> HttpResponse {
    let status_line = raw.lines().next().unwrap_or("");
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);
    let body = raw.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    HttpResponse { status, body }
}

fn kvstore_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("debug")
        .join("examples")
        .join("kvstore")
}

fn spawn_node(node_id: u16, smr_ports: &[u16; 3]) -> Child {
    let ports_str = smr_ports
        .iter()
        .map(|p| p.to_string())
        .collect::<Vec<_>>()
        .join(",");

    Command::new(kvstore_binary())
        .arg(node_id.to_string())
        .arg(&ports_str)
        .env("ROCKET_PORT", HTTP_PORTS[node_id as usize].to_string())
        .env("ROCKET_LOG_LEVEL", "off")
        .env("RUST_LOG", "error")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap_or_else(|e| panic!("Failed to spawn node {}: {}", node_id, e))
}

/// Wait for an HTTP port to accept connections.
async fn wait_for_ready(http_port: u16) {
    for _ in 0..500 {
        if TcpStream::connect_timeout(
            &format!("127.0.0.1:{}", http_port).parse().unwrap(),
            Duration::from_millis(100),
        )
        .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("Node on port {} never became ready", http_port);
}

/// Wait for all SMR ports to accept connections, then give a brief grace
/// period for the TCP mesh handshakes to complete.
async fn wait_for_cluster_ready() {
    for _ in 0..500 {
        let all_up = SMR_PORTS.iter().all(|&port| {
            TcpStream::connect_timeout(
                &format!("127.0.0.1:{}", port).parse().unwrap(),
                Duration::from_millis(50),
            )
            .is_ok()
        });
        if all_up {
            // Grace period for inter-node TCP handshakes to complete
            tokio::time::sleep(Duration::from_secs(2)).await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("Cluster never became ready");
}

fn kill_all(nodes: &mut [Child]) {
    for node in nodes.iter_mut() {
        let _ = node.kill();
        let _ = node.wait();
    }
}

#[tokio::test]
async fn test_kvstore_cluster() {
    // Build the example first (no-op if already built)
    let build = Command::new(env!("CARGO"))
        .args(["build", "--example", "kvstore"])
        .status()
        .expect("Failed to build kvstore example");
    assert!(build.success(), "kvstore example failed to build");

    let mut nodes: Vec<Child> = (0..3).map(|id| spawn_node(id, &SMR_PORTS)).collect();

    let result = timeout(TIMEOUT, async {
        // Wait for all HTTP servers and SMR mesh to be ready
        for &port in &HTTP_PORTS {
            wait_for_ready(port).await;
        }
        wait_for_cluster_ready().await;

        // 1. POST key "a" to node 0
        let resp = http_post(HTTP_PORTS[0], "/a", "value_a");
        assert_eq!(
            resp.status, 200,
            "POST /a failed ({}): {}",
            resp.status, resp.body
        );

        // 2. POST key "b" to node 1
        let resp = http_post(HTTP_PORTS[1], "/b", "value_b");
        assert_eq!(resp.status, 200, "POST /b should succeed");

        // 3. GET key "a" from node 2 (different node than writer)
        let resp = http_get(HTTP_PORTS[2], "/a");
        assert_eq!(resp.status, 200, "GET /a should find the key");
        assert!(resp.body.contains("value_a"), "GET /a body: {}", resp.body);

        // 4. GET key "b" from node 0
        let resp = http_get(HTTP_PORTS[0], "/b");
        assert_eq!(resp.status, 200, "GET /b should find the key");
        assert!(resp.body.contains("value_b"), "GET /b body: {}", resp.body);

        // 5. POST key "a" with updated value to node 2
        let resp = http_post(HTTP_PORTS[2], "/a", "value_a_updated");
        assert_eq!(resp.status, 200, "POST /a update should succeed");

        // 6. GET key "a" from node 1 — should see updated value
        let resp = http_get(HTTP_PORTS[1], "/a");
        assert_eq!(resp.status, 200, "GET /a after update should succeed");
        assert!(
            resp.body.contains("value_a_updated"),
            "GET /a should be updated: {}",
            resp.body
        );

        // 7. GET key "b" from node 2 — should be unchanged
        let resp = http_get(HTTP_PORTS[2], "/b");
        assert_eq!(resp.status, 200, "GET /b should still exist");
        assert!(
            resp.body.contains("value_b"),
            "GET /b should be unchanged: {}",
            resp.body
        );
    })
    .await;

    kill_all(&mut nodes);
    result.expect("Test timed out");
}
