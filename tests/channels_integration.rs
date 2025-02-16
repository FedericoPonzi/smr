use rocket::form::validate::Contains;
use smr::TcpChannel;
use std::net::SocketAddr;
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
    let mut channel2 = TcpChannel::new(2, addr2, peers2).await;
    let mut channel3 = TcpChannel::new(3, addr3, peers3).await;

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Ensure connections are established
    let connected1 = channel1.connected_peers().await;
    let connected2 = channel2.connected_peers().await;
    let connected3 = channel3.connected_peers().await;

    debug!("Node 1 connected to: {:?}", connected1);
    debug!("Node 2 connected to: {:?}", connected2);
    debug!("Node 3 connected to: {:?}", connected3);

    assert!(connected1.contains(&2) || connected1.contains(&3));
    assert!(connected2.contains(&1) || connected2.contains(&3));
    assert!(connected3.contains(&1) || connected3.contains(&2));

    // Channel 1 sends a message
    let msg = "test_message".to_string();
    channel1.send(msg.clone()).await;

    let (tx, mut rx) = mpsc::channel::<String>(1);
    let tx1 = tx.clone();
    // Spawn tasks to receive messages
    let recv_task2 = tokio::spawn(async move {
        if let Some(received) = channel2.receive().await {
            tx1.send(received).await.unwrap();
        }
    });

    let recv_task3 = tokio::spawn(async move {
        if let Some(received) = channel3.receive().await {
            tx.clone().send(received).await.unwrap();
        }
    });
    // Wait for one of the nodes to receive the message
    let received = timeout(std::time::Duration::from_secs(7), rx.recv())
        .await
        .expect("Did not receive message in time")
        .unwrap();

    info!("Received message: {:?}", received);
    assert_eq!(received, msg);

    recv_task2.abort();
    recv_task3.abort();
}
