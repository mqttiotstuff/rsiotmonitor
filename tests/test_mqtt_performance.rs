use std::io;

use futures_util::future::try_join_all;
use log::{info, debug};
use mqtt_v5_broker::{broker::{Broker, BrokerMessage}, client};
use tokio::{runtime::Builder, net::TcpListener};



async fn tcp_server_loop(broker_tx: tokio::sync::mpsc::Sender<BrokerMessage>, bind_options: String) -> io::Result<()> {
    
    info!("Listening on {}", bind_options);

    let listener = TcpListener::bind(bind_options).await?;

    loop {
        let (stream, addr) = listener.accept().await?;
        debug!("Client {} connected (tcp)", addr);
        client::spawn(stream, broker_tx.clone());
    }
}


async fn launch_mqtt_server() -> Result<(), Box<dyn std::error::Error>> {
    let broker = Broker::new();

    let broker_tx = broker.sender(); // : tokio::task::JoinHandle<Result(), _>
    let broker = tokio::task::spawn(async {
        broker.run().await;
    });

    let local_bind_option = "0.0.0.0:1884".to_string();
    let local_broker_tx = broker_tx.clone();
    let tcp_listener = 
        tokio::task::spawn(async move { 
            tcp_server_loop(local_broker_tx.clone(), local_bind_option).await;} );

    // tcp_listener,
    // ,  websocket_listener
    try_join_all([broker, tcp_listener]).await?;

    Ok(())
}

#[test]
fn test_mqtt_performances() {
    let rt  = Builder::new_multi_thread().enable_all().build().unwrap();

    rt.block_on(async {
        launch_mqtt_server().await;
    });
}
