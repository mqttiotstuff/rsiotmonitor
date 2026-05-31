//! Embedded MQTT v5 broker stress test.
//!
//! Starts a broker on `127.0.0.1:0`, runs concurrent publishers (MQTT 3.1.1 clients),
//! prints throughput metrics, then shuts the broker down.
//!
//! **Throughput expectations**
//!
//! ~250–300 msg/s with QoS 1, sequential publish, and a subscriber is normal for this
//! stack — it matches the historical `mosquitto_pub` loop in `validation_and_tests/PERF_NOTE.md`
//! (~266 msg/s for 10k messages). That measures **10 000 total messages**, not 10 000 msg/s.
//!
//! For higher throughput, disable the subscriber and/or raise in-flight pipelining:
//!
//! ```sh
//! # publish-only, QoS 0, 32 in-flight per client (~10k+ msg/s on localhost)
//! MQTT_STRESS_CLIENTS=10 MQTT_STRESS_MESSAGES=1000 MQTT_STRESS_QOS=0 \
//!   MQTT_STRESS_INFLIGHT=32 MQTT_STRESS_SUBSCRIBER=false \
//!   cargo test test_mqtt_performances -- --nocapture
//! ```
//!
//! Defaults (CI-friendly): 4 clients × 250 QoS-1 sequential publishes + subscriber.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::join_all;
use log::info;
use mqtt_async_client::client::{Client, Publish, QoS, Subscribe, SubscribeTopic};
use mqtt_v5_broker::{
    broker::Broker,
    client,
};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, Instant};

const STRESS_TOPIC: &str = "stress_topic";
const STRESS_PAYLOAD: &[u8] = b"hello";

#[derive(Clone, Debug)]
struct StressConfig {
    clients: u32,
    messages_per_client: u32,
    qos: QoS,
    in_flight: u32,
    subscriber: bool,
}

impl StressConfig {
    fn from_env() -> Self {
        let qos = match env_u32("MQTT_STRESS_QOS", 1) {
            0 => QoS::AtMostOnce,
            2 => QoS::ExactlyOnce,
            _ => QoS::AtLeastOnce,
        };
        Self {
            clients: env_u32("MQTT_STRESS_CLIENTS", 4),
            messages_per_client: env_u32("MQTT_STRESS_MESSAGES", 250),
            qos,
            in_flight: env_u32("MQTT_STRESS_INFLIGHT", 1).max(1),
            subscriber: env_bool("MQTT_STRESS_SUBSCRIBER", true),
        }
    }

    fn total_messages(&self) -> u64 {
        u64::from(self.clients) * u64::from(self.messages_per_client)
    }
}

#[derive(Debug)]
struct StressMetrics {
    config: StressConfig,
    messages_sent: u64,
    messages_received: u64,
    publish_errors: u64,
    elapsed: Duration,
    broker_addr: SocketAddr,
}

impl StressMetrics {
    fn messages_per_sec(&self) -> f64 {
        if self.elapsed.is_zero() {
            return 0.0;
        }
        self.messages_sent as f64 / self.elapsed.as_secs_f64()
    }

    fn report(&self) {
        eprintln!("=== MQTT stress metrics ===");
        eprintln!("broker: {}", self.broker_addr);
        eprintln!(
            "config: {} clients × {} msgs (QoS {}, in_flight={}, subscriber={})",
            self.config.clients,
            self.config.messages_per_client,
            qos_label(self.config.qos),
            self.config.in_flight,
            self.config.subscriber,
        );
        eprintln!("duration: {:.3}s", self.elapsed.as_secs_f64());
        eprintln!("published (ok): {}", self.messages_sent);
        eprintln!("publish errors: {}", self.publish_errors);
        eprintln!("subscriber received: {}", self.messages_received);
        if self.messages_sent > 0 {
            eprintln!(
                "delivery ratio: {:.2}%",
                100.0 * self.messages_received as f64 / self.messages_sent as f64
            );
        }
        eprintln!("throughput: {:.1} msg/s", self.messages_per_sec());
        eprintln!("===========================");
    }
}

struct MqttTestServer {
    addr: SocketAddr,
    broker_handle: JoinHandle<()>,
    listener_handle: JoinHandle<()>,
}

impl MqttTestServer {
    async fn start() -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let broker = Broker::new();
        let broker_tx = broker.sender();
        let broker_handle = tokio::spawn(async move {
            broker.run().await;
        });

        let listener_handle = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(conn) => conn,
                    Err(_) => break,
                };
                client::spawn(stream, broker_tx.clone());
            }
        });

        Ok(Self {
            addr,
            broker_handle,
            listener_handle,
        })
    }

    fn addr(&self) -> SocketAddr {
        self.addr
    }

    fn stop(self) {
        self.broker_handle.abort();
        self.listener_handle.abort();
    }
}

fn mqtt_url(addr: SocketAddr) -> String {
    format!("mqtt://{addr}")
}

fn build_client(url: &str, client_id: &str) -> mqtt_async_client::Result<Client> {
    Client::builder()
        .set_url_string(url)?
        .set_client_id(Some(client_id.to_owned()))
        .set_connect_retry_delay(Duration::from_millis(100))
        .set_operation_timeout(Duration::from_secs(30))
        .build()
}

async fn wait_for_broker(url: &str) -> mqtt_async_client::Result<()> {
    for attempt in 0..50 {
        match build_client(url, &format!("probe-{attempt}")) {
            Ok(mut client) => {
                if client.connect().await.is_ok() {
                    let _ = client.disconnect().await;
                    return Ok(());
                }
            }
            Err(_) => {}
        }
        sleep(Duration::from_millis(20)).await;
    }
    Err(mqtt_async_client::Error::from(
        "broker did not become ready in time",
    ))
}

async fn run_subscriber(
    url: String,
    expected: u64,
    received: Arc<AtomicU64>,
) -> mqtt_async_client::Result<()> {
    let mut client = build_client(&url, "stress-subscriber")?;
    client.connect().await?;

    let sub = Subscribe::new(vec![SubscribeTopic {
        qos: QoS::AtLeastOnce,
        topic_path: STRESS_TOPIC.to_owned(),
    }]);
    client.subscribe(sub).await?.any_failures()?;

    while received.load(Ordering::Relaxed) < expected {
        match timeout(Duration::from_millis(500), client.read_subscriptions()).await {
            Ok(Ok(_msg)) => {
                received.fetch_add(1, Ordering::Relaxed);
            }
            Ok(Err(_)) => break,
            Err(_) => continue,
        }
    }

    let _ = client.disconnect().await;
    Ok(())
}

async fn run_publisher(
    url: String,
    client_id: String,
    messages: u32,
    qos: QoS,
    in_flight: u32,
    sent: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
) {
    let mut client = match build_client(&url, &client_id) {
        Ok(c) => c,
        Err(_) => {
            errors.fetch_add(u64::from(messages), Ordering::Relaxed);
            return;
        }
    };

    if client.connect().await.is_err() {
        errors.fetch_add(u64::from(messages), Ordering::Relaxed);
        return;
    }

    let topic = STRESS_TOPIC.to_owned();
    let payload = STRESS_PAYLOAD.to_vec();
    let batch_size = in_flight as usize;

    for batch_start in (0..messages as usize).step_by(batch_size) {
        let batch_end = batch_start.saturating_add(batch_size).min(messages as usize);
        let mut publishes = Vec::with_capacity(batch_end - batch_start);
        for _ in batch_start..batch_end {
            let mut publish = Publish::new(topic.clone(), payload.clone());
            publish.set_qos(qos);
            publishes.push(publish);
        }
        let results = join_all(publishes.iter().map(|p| client.publish(p))).await;
        for result in results {
            match result {
                Ok(()) => {
                    sent.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    let _ = client.disconnect().await;
}

async fn run_stress(broker_addr: SocketAddr, config: StressConfig) -> StressMetrics {
    let url = mqtt_url(broker_addr);
    wait_for_broker(&url)
        .await
        .expect("broker should accept connections");

    let expected = config.total_messages();
    let sent = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let received = Arc::new(AtomicU64::new(0));

    let subscriber_handle = if config.subscriber {
        let url = url.clone();
        let received = received.clone();
        Some(tokio::spawn(async move {
            if let Err(e) = run_subscriber(url, expected, received).await {
                eprintln!("subscriber error: {e}");
            }
        }))
    } else {
        None
    };

    let started = Instant::now();
    let publishers: Vec<_> = (0..config.clients)
        .map(|client_idx| {
            let url = url.clone();
            let sent = sent.clone();
            let errors = errors.clone();
            let client_id = format!("stress-publisher-{client_idx}");
            tokio::spawn(run_publisher(
                url,
                client_id,
                config.messages_per_client,
                config.qos,
                config.in_flight,
                sent,
                errors,
            ))
        })
        .collect();

    for handle in publishers {
        let _ = handle.await;
    }

    sleep(Duration::from_millis(200)).await;

    if let Some(handle) = subscriber_handle {
        handle.abort();
        let _ = timeout(Duration::from_millis(500), handle).await;
    }

    sleep(Duration::from_millis(100)).await;

    StressMetrics {
        config,
        messages_sent: sent.load(Ordering::Relaxed),
        messages_received: received.load(Ordering::Relaxed),
        publish_errors: errors.load(Ordering::Relaxed),
        elapsed: started.elapsed(),
        broker_addr,
    }
}

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(default)
}

fn qos_label(qos: QoS) -> &'static str {
    match qos {
        QoS::AtMostOnce => "0",
        QoS::AtLeastOnce => "1",
        QoS::ExactlyOnce => "2",
    }
}

#[test]
fn test_mqtt_performances() {
    let _ = env_logger::builder().is_test(true).try_init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let config = StressConfig::from_env();
        info!(
            "starting MQTT stress: {} clients x {} messages",
            config.clients, config.messages_per_client
        );

        let server = MqttTestServer::start()
            .await
            .expect("failed to start embedded MQTT broker");

        let metrics = run_stress(server.addr(), config).await;
        metrics.report();

        server.stop();

        assert_eq!(
            metrics.publish_errors, 0,
            "expected zero publish errors during stress run"
        );
        assert_eq!(
            metrics.messages_sent,
            metrics.config.total_messages(),
            "not all messages were published successfully"
        );
        if metrics.config.subscriber {
            let min_received = metrics.messages_sent * 99 / 100;
            assert!(
                metrics.messages_received >= min_received,
                "subscriber received {} / {} messages (expected >= 99%)",
                metrics.messages_received,
                metrics.messages_sent,
            );
        }
    });
}
