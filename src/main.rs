// use futures_core::future;

#[allow(unused_imports)]
// #![deny(warnings)]
use futures_util::stream::{futures_unordered::FuturesUnordered, StreamExt};

use log::{log, warn};
#[allow(unused_imports)]
use log::{debug, error, info, trace};
use mqtt_async_client::{
    client::{
        Client, KeepAlive, Publish as PublishOpts, QoS, Subscribe as SubscribeOpts, SubscribeTopic,
    },
    Error, Result,
};

#[cfg(feature = "tls")]
use rustls;
#[cfg(feature = "tls")]
use std::io::Cursor;
use structopt::StructOpt;
use tokio::{time::Duration};
#[cfg(feature = "tls")]
use webpki_roots;

use rsiotmonitor::*;
use std::{collections::HashMap, time::SystemTime};

#[derive(Debug)]
struct Config {
    /// Username to authenticate with, optional.
    username: Option<String>,

    /// Password to authenticate with, optional.
    password: Option<String>,

    /// Host to connect to, REQUIRED.
    url: String,

    /// monitoring base topic
    base_topic: String,

    /// Client ID to identify as, optional.
    client_id: Option<String>,

    /// Enable TLS and set the path to a PEM file containing the
    /// CA certificate that signs the remote server's certificate.
    tls_server_ca_file: Option<String>,

    /// Enable TLS and trust the CA certificates in the webpki-roots
    /// crate, ultimately Mozilla's root certificates.
    tls_mozilla_root_cas: bool,

    /// Enable TLS and set the path to a PEM file containing the
    /// client certificate for client authentication.
    tls_client_crt_file: Option<String>,

    /// Enable TLS and set the path to a PEM file containing the
    /// client rsa key for client authentication.
    tls_client_rsa_key_file: Option<String>,

    /// Keepalive interval in seconds
    keep_alive: u16,

    /// Operation timeout in seconds
    op_timeout: u16,

    /// monitored elements
    monitored_devices: HashMap<String, Box<MonitoringInfo>>,
}

use toml_parse::*;

fn update_monitorinfo_from_config_table(
    monitor_info: &mut MonitoringInfo,
    table: &toml_parse::Table,
) {
    for kv in table.items() {
        if let Some(keyname) = kv.key() {
            match kv.value() {
                Value::StrLit(s) => {
                    if keyname == "watchTopics" {
                        let mut topicList = Vec::new();
                        topicList.push(s.clone());
                        monitor_info.watch_topics = topicList;
                    }
                }
                _ => (),
            }
        }
    }
}

fn read_mqtt_config_table(config: &mut Config, table: &toml_parse::Table) {
    assert!(table.header() == "mqtt");
    for kv in table.items() {
        if let Some(keyname) = kv.key() {
            match kv.value() {
                Value::StrLit(s) => match keyname {
                    "serverAddress" => {
                        config.url = s.clone();
                    }
                    "baseTopic" => {
                        config.base_topic = s.clone();
                    }
                    "password" => {
                        config.password = Some(s.clone());
                    }
                    "clientid" => {
                        config.client_id = Some(s.clone());
                    }
                    "user" => {
                        config.username = Some(s.clone());
                    }
                    s => {
                        panic!("unknown mqtt section property : {}", s);
                    }
                },
                _ => (),
            }
        }
    }
}


async fn read_configuration() -> Result<Config> {
    let mut config = Config {
        username: None,
        password: None,
        url: "".into(),
        base_topic: "iotmonitor/monitoring".into(),
        client_id: None,
        tls_server_ca_file: None,
        tls_mozilla_root_cas: false,
        tls_client_crt_file: None,
        tls_client_rsa_key_file: None,
        keep_alive: 10,
        op_timeout: 10,
        monitored_devices: HashMap::new(),
    };

    use std::fs;
    let contents = fs::read_to_string("config.toml").expect("cannot read config.toml");

    let t = Toml::new(&contents);

    let devices: Vec<Box<MonitoringInfo>> = t.iter().fold(Vec::new(), |acc, i| {
        let mut m = acc;
        match i {
            toml_parse::Value::Table(table) => {
                if table.header() == "mqtt" {
                    read_mqtt_config_table(&mut config, table);
                } else {
                    // create MonitorInfo
                    let name = table.header().into();
                    let mut monitor_info = MonitoringInfo::create(name);

                    update_monitorinfo_from_config_table(&mut monitor_info, &table);

                    // if this is an agent, add the additional elements
                    m.push(monitor_info);
                }
            }
            _ => {
                // ignored
            }
        };
        m
    });

    let hash: HashMap<String, Box<MonitoringInfo>> =
        HashMap::from_iter(devices.into_iter().map(|e| (e.name.clone(), e)));

    config.monitored_devices = hash;

    Ok(config)
}

async fn wait_1s() -> Result<()> {
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

async fn subscribe_and_run(config: &mut Config, client: &mut Client) -> Result<()> {
    let subopts = SubscribeOpts::new(
        config
            .monitored_devices
            .iter()
            .flat_map(|t| {
                t.1.watch_topics.iter().map(|e| SubscribeTopic {
                    qos: int_to_qos(1),
                    topic_path: e.clone(),
                })
            })
            .collect(),
    );

    let subres = client.subscribe(subopts).await?;
    subres.any_failures()?;

    loop {
        let r = client.read_subscriptions().await;

        println!("Read r={:?}", r);

        if let Err(Error::Disconnected) = r {
            return Err(Error::Disconnected);
        }
    }
}

async fn start() -> Result<()> {
    let mut config = read_configuration().await?;
    println!("config : {:?}\n", &config);

    let mut client = match client_from_args(&config) {
        Ok(client) => client,
        Err(e) => panic!("{}", e),
    };

    // main loop
    loop {
        let conn_result = client.connect().await;
        if let Ok(ok_result) = conn_result {
            println!("connected : {:?}\n", ok_result);

            let mut config_watchdog = read_configuration().await?;
            if let Some(clientid) = config_watchdog.client_id {

                config_watchdog.client_id = Some(clientid + "_outbounds".into());
                let mut client2 = match client_from_args(&config_watchdog) {
                    Ok(client) => client,
                    Err(e) => panic!("{}", e),
                };

                client2.connect().await.unwrap();

                tokio::spawn(async move {

                    let c = client2;

                    loop {
                        let mut p = PublishOpts::new(
                            "hello".into(),
                            format!("{:?}", SystemTime::now()).as_bytes().to_vec(),
                        );
                        p.set_qos(int_to_qos(1));
                        p.set_retain(false);

                        if let Err(e) = c.publish(&p).await {
                            warn!("error in publishing the health check");                            
                        }

                        wait_1s().await;
                    }
                });

                if let Err(e) = subscribe_and_run(&mut config, &mut client).await {
                    error!("{}", e);
                    break;
                }

            } else {
                client.disconnect().await;
            }
        } else {
            println!("error in connection : {:?}\n", conn_result);
        }

        wait_1s().await;
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    env_logger::init();
    start().await.unwrap();
}


fn client_from_args(args: &Config) -> Result<Client> {
    let mut b = Client::builder();
    b.set_url_string(&args.url)?
        .set_username(args.username.clone())
        .set_password(args.password.clone().map(|s| s.as_bytes().to_vec()))
        .set_client_id(args.client_id.clone())
        .set_connect_retry_delay(Duration::from_secs(1))
        .set_keep_alive(KeepAlive::from_secs(args.keep_alive))
        .set_operation_timeout(Duration::from_secs(args.op_timeout as u64))
        .set_automatic_connect(true);

    #[cfg(feature = "tls")]
    {
        let cc = if let Some(s) = &args.tls_server_ca_file {
            let mut cc = rustls::ClientConfig::new();
            let cert_bytes = std::fs::read(s)?;
            let cert = rustls::internal::pemfile::certs(&mut Cursor::new(&cert_bytes[..]))
                .map_err(|_| Error::from("Error parsing server CA cert file"))?[0]
                .clone();
            cc.root_store
                .add(&cert)
                .map_err(|e| Error::from_std_err(e))?;
            Some(cc)
        } else if args.tls_mozilla_root_cas {
            let mut cc = rustls::ClientConfig::new();
            cc.root_store
                .add_server_trust_anchors(&webpki_roots::TLS_SERVER_ROOTS);
            Some(cc)
        } else {
            None
        };

        let cc = if let Some((crt_file, key_file)) = args
            .tls_client_crt_file
            .clone()
            .zip(args.tls_client_rsa_key_file.clone())
        {
            let cert_bytes = std::fs::read(crt_file)?;
            let client_cert = rustls::internal::pemfile::certs(&mut Cursor::new(&cert_bytes[..]))
                .map_err(|_| Error::from("Error parsing client cert file"))?[0]
                .clone();

            let key_bytes = std::fs::read(key_file)?;
            let client_key =
                rustls::internal::pemfile::rsa_private_keys(&mut Cursor::new(&key_bytes[..]))
                    .map_err(|_| Error::from("Error parsing client key file"))?[0]
                    .clone();

            let mut cc = cc.unwrap_or_else(rustls::ClientConfig::new);
            cc.set_single_client_cert(vec![client_cert], client_key)
                .map_err(|e| Error::from(format!("Error setting client cert: {}", e)))?;
            Some(cc)
        } else {
            cc
        };

        if let Some(cc) = cc {
            b.set_tls_client_config(cc);
        }
    }

    b.build()
}

fn int_to_qos(qos: u8) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => panic!("Not reached"),
    }
}
