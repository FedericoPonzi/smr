use rocket::form::validate::{contains, msg, Contains};
use smr::TcpChannel;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::{sync::mpsc, time::timeout};
use tracing::{debug, info};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_channel_happy_path() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let addr1: SocketAddr = "127.0.0.1:6001".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:6002".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:6003".parse().unwrap();

    let peers1 = vec![(2, addr2), (3, addr3)];
    let peers2 = vec![(1, addr1), (3, addr3)];
    let peers3 = vec![(1, addr1), (2, addr2)];

    let mut channel1 = TcpChannel::new(1, addr1, peers1).await;
    let mut channel2: TcpChannel<String> = TcpChannel::new(2, addr2, peers2).await;
    let mut channel3: TcpChannel<String> = TcpChannel::new(3, addr3, peers3).await;
    timeout(Duration::from_secs(3), async {
        loop {
            let connected1 = channel1.connected_peers().await;
            let connected2 = channel2.connected_peers().await;
            let connected3 = channel3.connected_peers().await;

            let a = connected1.contains(&2) && connected1.contains(&3);
            let b = connected2.contains(&1) && connected2.contains(&3);
            let c = connected3.contains(&1) && connected3.contains(&2);

            if a && b && c {
                return;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("Channels did not complete connections on time.");

    // Channel 1 sends a message
    let msg1 = "task-1".to_string();
    channel1.send(msg1.clone()).await;
    let msg2 = "task-2".to_string();
    let msg3 = "task-3".to_string();

    let (tx, mut rx) = mpsc::channel::<String>(1);
    let tx1 = tx.clone();
    // Spawn tasks to receive messages
    let recv_task2 = tokio::spawn(async move {
        if let Some(received) = channel2.receive().await {
            assert_eq!(received, "task-1");
            tx1.send(msg2).await.unwrap();
        }
    });

    let recv_task3 = tokio::spawn(async move {
        if let Some(received) = channel3.receive().await {
            assert_eq!(received, msg1);
            tx.send(msg3).await.unwrap();
        }
    });
    // Wait for one of the nodes to receive the message
    let received = timeout(Duration::from_secs(7), rx.recv())
        .await
        .expect("Did not receive message in time")
        .unwrap();

    info!("Received message: {:?}", received);
    let mut expected = HashSet::from(["task-2".to_string(), "task-3".to_string()]);
    assert!(expected.remove(&received));

    // Wait for one of the nodes to receive the message
    let received = timeout(Duration::from_secs(7), rx.recv())
        .await
        .expect("Did not receive message in time")
        .unwrap();
    assert!(expected.remove(&received));

    recv_task2.abort();
    recv_task3.abort();
}
