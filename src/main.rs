// use futures_core::future;

use futures_util::future::try_join_all;
#[allow(unused_imports)]
// #![deny(warnings)]
use futures_util::stream::{futures_unordered::FuturesUnordered, StreamExt};

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use mqtt_async_client::{
    client::{
        Client, KeepAlive, Publish as PublishOpts, QoS, Subscribe as SubscribeOpts, SubscribeTopic,
    },
    Error,
};

use mqtt_v5_broker::{
    broker::{Broker, BrokerMessage},
    client,
};
#[cfg(feature = "tls")]
use rustls;
#[cfg(feature = "tls")]
use std::io::Cursor;

use structopt::StructOpt;

use tokio::{net::TcpListener, task::JoinHandle, time::Duration};
#[cfg(feature = "tls")]
use webpki_roots;

use rsiotmonitor::{process::ProcessIterator, *};
use std::{
    borrow::Borrow,
    collections::HashMap,
    io,
    net::{Ipv6Addr, SocketAddrV6},
    ops::{Deref, Index},
    sync::{mpsc::Sender, Arc, RwLock},
    time::SystemTime,
};

use toml_parse::*;

/// read configuration from config.toml
async fn read_configuration() -> mqtt_async_client::Result<IOTMonitor> {
    let mut mqtt_config = MqttConfig::default();

    use std::fs;
    let contents = fs::read_to_string("config.toml").expect("cannot read config.toml");

    let t = Toml::new(&contents);

    let devices: Vec<Box<MonitoringInfo>> = t.iter().fold(Vec::new(), |acc, i| {
        let mut m = acc;
        match i {
            toml_parse::Value::Table(table) => {
                if table.header() == "mqtt" {
                    rsiotmonitor::read_mqtt_config_table(&mut mqtt_config, table);
                } else {
                    // create MonitorInfo
                    let mut name: String = table.header().into();
                    let mut isagent: bool = false;

                    if table.header().starts_with("agent_") {
                        if let Some(without_suffix) = name.strip_prefix("agent_") {
                            name = without_suffix.into();
                            isagent = true;
                        }
                    }

                    let mut monitor_info = MonitoringInfo::create(name);

                    rsiotmonitor::update_monitorinfo_from_config_table(&mut monitor_info, &table);

                    // only agent have process informations
                    if isagent {
                        debug!("reading process informations");
                        rsiotmonitor::read_process_informations_from_config_table(
                            &mut monitor_info,
                            &table,
                        )
                    }

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

    // monitoring info by device name
    let hash: HashMap<String, Box<MonitoringInfo>> =
        HashMap::from_iter(devices.into_iter().map(|e| (e.name.clone(), e)));

    let iotmonitor = IOTMonitor::new(mqtt_config, None, hash);

    Ok(iotmonitor)
}

async fn wait_2s() -> mqtt_async_client::Result<()> {
    tokio::time::sleep(Duration::from_secs(2)).await;
    Ok(())
}

/**
 * does the evaluated topic contains the tested_topic match
 */
fn does_topic_match(tested_topic: &String, evaluated_topic: &String) -> bool {
    let mut tested = tested_topic.clone();
    if tested_topic.ends_with("#") {
        tested = (tested[0..tested.len() - 1]).to_string();
        evaluated_topic.starts_with(&tested)
    } else if tested_topic.eq("") {
        true
    } else {
        evaluated_topic.eq(&tested)
    }
}

#[test]
fn test_does_topic_match() {
    assert_eq!(
        does_topic_match(&"home".to_string(), &"home/toto".to_string()),
        false
    );
    assert_eq!(
        does_topic_match(&"home/#".to_string(), &"home/toto".to_string()),
        true
    );
    assert_eq!(
        does_topic_match(&"toto".to_string(), &"tutu".to_string()),
        false
    );

    assert_eq!(does_topic_match(&"".to_string(), &"tutu".to_string()), true);
    assert_eq!(
        does_topic_match(&"#".to_string(), &"tutu".to_string()),
        true
    );
}

async fn subscribe_and_run(
    config: &Arc<tokio::sync::RwLock<IOTMonitor>>,
    client: &mut Client,
) -> mqtt_async_client::Result<()> {
    {
        debug!("getting configuration");
        let config_ref = config.write().await;

        let mut subscribed_topics: Vec<SubscribeTopic> = config_ref
            .monitored_devices()
            .iter()
            .flat_map(|t| {
                t.1.watch_topics.iter().map(|e| SubscribeTopic {
                    qos: int_to_qos(1),
                    topic_path: e.clone(),
                })
            })
            .collect();

        let mut state_topics: Vec<SubscribeTopic> = config_ref
            .monitored_devices()
            .iter()
            .flat_map(|t| match &t.1.state_topic {
                Some(h) => vec![SubscribeTopic {
                    qos: int_to_qos(1),
                    topic_path: h.clone(),
                }],
                _ => vec![],
            })
            .collect();

        let mut hello_topics: Vec<SubscribeTopic> = config_ref
            .monitored_devices()
            .iter()
            .flat_map(|t| match &t.1.hello_topic {
                Some(h) => vec![SubscribeTopic {
                    qos: int_to_qos(1),
                    topic_path: h.clone(),
                }],
                _ => vec![],
            })
            .collect();

        subscribed_topics.append(&mut hello_topics);
        subscribed_topics.append(&mut state_topics);

        let subopts = SubscribeOpts::new(subscribed_topics);
        debug!("subscriptions to watch : {:?}", &subopts);
        let subres = client.subscribe(subopts).await?;
        subres.any_failures()?;

        // subscription to hello, and state
    } // lock section on config

    // events loop on subscription receive
    loop {
        // message received ?
        let r = client.read_subscriptions().await;

        debug!("Read r={:?}", r);

        if let Err(Error::Disconnected) = r {
            return Err(Error::Disconnected);
        }

        if let Ok(result) = r {
            let topic = result.topic().to_string();
            let payload = result.payload();
            {
                let mut config_ref = config.write().await;
                let connection = &config_ref.state_connection.to_owned();
                for (name, c) in config_ref.monitored_devices_mut().iter_mut() {
                    // hello
                    if let Some(hello_topic) = c.hello_topic.clone() {
                        if hello_topic.eq(&topic) {
                            // c.hello_count += 1;
                            info!("hello topic {} received", &hello_topic);
                            match connection {
                                Some(c) => {
                                    let get_all_states_result = state::get_all_states(c, &name);
                                    if let Ok(state) = get_all_states_result {
                                        for i in state {
                                            info!(
                                                "restored state for {}, topic {}, state {:?}",
                                                &name, &i.0, &i.1
                                            );
                                            let mut p = PublishOpts::new(i.0, i.1);
                                            p.set_qos(int_to_qos(1));
                                            p.set_retain(false);

                                            // async publish
                                            if let Err(e) = client.publish(&p).await {
                                                error!("error while publishing , {}", e);
                                            }
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }

                    // state
                    if let Some(state_topic) = c.state_topic.clone() {
                        if does_topic_match(&state_topic, &topic) {
                            match connection {
                                Some(c) => {
                                    // send state associated to the
                                    debug!("saving state for {}", &topic);
                                    if let Err(save_error) =
                                        state::save_state(c, &topic, &name, &payload)
                                    {
                                        warn!(
                                            "error in saving the state for {} : {}",
                                            name, save_error
                                        );
                                    }
                                }
                                _ => {
                                    warn!("no connection to storage");
                                }
                            };
                        } else {
                            debug!("no match for topic {}, with {}", &state_topic, &topic);
                        }
                    } else {
                        debug!("no state topic for {}", &name);
                    }

                    // update watch topics
                    for watch_topics in &c.watch_topics {
                        if does_topic_match(&watch_topics, &topic) {
                            c.next_contact = Some(SystemTime::now() + c.timeout_value);
                        }
                    }
                }
            }
        }
    }
}

/// look for already running process to get theirs associated pid
/// and be able to check for alive process
fn wrap_already_exists_processes(config: IOTMonitor) -> IOTMonitor {
    // TODO refactor this
    debug!("start checking already exists processes");

    let MAGICPROCSSHEADER: String = String::from(process::MAGIC) + "_";

    let mut c: IOTMonitor = config;

    // get the already running processes
    let pi = ProcessIterator::new().unwrap();
    for p in pi {
        for e in p.commmand_line_elements {
            // println!("evaluate {}",e);
            if e.contains(&MAGICPROCSSHEADER) {
                debug!("found pattern evaluate {}", e);
                match e.find(&MAGICPROCSSHEADER) {
                    Some(idx1) => {
                        let s = &e[idx1 + MAGICPROCSSHEADER.len()..];
                        match s.find(";") {
                            Some(idx2) => {
                                let name = String::from(&s[0..idx2]);
                                debug!("{} found", &name);
                                match c.monitored_devices_mut().get_mut(&name) {
                                    Some(mi) => match &mut mi.associated_process_information {
                                        Some(api) => {
                                            api.pid = Some(p.pid);
                                            info!("{} attached with pid {}", &name, p.pid);
                                        }
                                        None => {}
                                    },
                                    None => {}
                                }
                            }
                            None => {}
                        }
                    }
                    None => {}
                }
            }
        }
    }
    debug!("end of checking already exists processes");
    c
}

use chrono::offset::Utc;
use chrono::DateTime;

/// start method
async fn start(config: IOTMonitor) -> mqtt_async_client::Result<()> {
    // search for existing processes, and wrap their declaration

    let mut populated_config = wrap_already_exists_processes(config);

    let base_topic = populated_config.mqtt_config.base_topic.clone();

    let config_mqtt = populated_config.mqtt_config.clone();

    let mut client = match client_from_args(&config_mqtt) {
        Ok(client) => client,
        Err(e) => panic!("{}", e),
    };

    info!("opening state storage");
    populated_config.state_connection = Some(Arc::new(state::init().unwrap()));

    let mut config_mqtt_watchdoc = populated_config.mqtt_config.clone();

    let rw: tokio::sync::RwLock<IOTMonitor> = tokio::sync::RwLock::new(populated_config);
    let config_ref = Arc::new(rw);

    // main loop, reconnect
    loop {
        let conn_result = client.connect().await;
        if let Ok(ok_result) = conn_result {
            info!("connected : {:?}\n", ok_result);

            if let Some(clientid) = config_mqtt_watchdoc.client_id {
                config_mqtt_watchdoc.client_id = Some(clientid + "_outbounds".into());
                let mut client2 = match client_from_args(&config_mqtt_watchdoc) {
                    Ok(client) => client,
                    Err(e) => panic!("{}", e), // cannot connect twice
                };

                let config_ref_check = config_ref.clone();

                client2.connect().await.unwrap();

                let l_base_topic = base_topic.clone();
                tokio::spawn(async move {
                    debug!("start watchdog");
                    let mut cnx_mqtt = client2;

                    loop {
                        // each check epoch
                        debug!("send watch dog");
                        let datetime: DateTime<Utc> = SystemTime::now().into();
                        let mut p = PublishOpts::new(
                            format!("{}/alive", l_base_topic),
                            format!("{}", datetime.format("%d/%m/%Y %T"))
                                .as_bytes()
                                .to_vec(),
                        );
                        p.set_qos(int_to_qos(1));
                        p.set_retain(false);

                        if let Err(e) = cnx_mqtt.publish(&p).await {
                            warn!("error in publishing the health check : {}", e);
                            break;
                        }

                        let mut expired: Vec<String> = vec![];

                        // check processes
                        {
                            let current_time = SystemTime::now();

                            let mut conf = config_ref_check.write().await;

                            for entries in conf.monitored_devices_mut().iter_mut() {
                                let name = entries.0;
                                let info = entries.1.as_mut();

                                if let Some(additional_infos) =
                                    info.associated_process_information.as_deref_mut()
                                {
                                    if let Some(pid) = additional_infos.pid {
                                        if let Ok(_pinfo) = process::get_process_information(pid) {
                                            continue;
                                        }
                                    }

                                    debug!("launching process {}", &additional_infos.exec);
                                    process::run_process_with_fork(name, additional_infos).unwrap();
                                    additional_infos.restarted_count += 1;
                                }

                                debug!("check next contact");
                                if let Some(next_contact) = info.next_contact {
                                    if next_contact < current_time {
                                        info!("time out for {}", info.name);

                                        // send the expired elements
                                        expired.push(name.clone());
                                    }
                                }
                            } // loop
                        }

                        if !expired.is_empty() {
                            for name in expired.iter() {
                                let datetime: DateTime<Utc> = SystemTime::now().into();

                                let mut pexpired = PublishOpts::new(
                                    format!("{}/expired/{}", l_base_topic.to_string(), name),
                                    format!("{}", datetime.format("%d/%m/%Y %T"))
                                        .as_bytes()
                                        .to_vec(),
                                );
                                pexpired.set_qos(int_to_qos(1));
                                pexpired.set_retain(false);

                                let publish_result = cnx_mqtt.publish(&pexpired).await;
                                if let Err(e) = publish_result {
                                    warn!("error in publishing the health check");
                                    break;
                                }
                            }
                        }

                        wait_2s().await;
                    }

                    cnx_mqtt.disconnect().await;
                });

                if let Err(e) = subscribe_and_run(&config_ref, &mut client).await {
                    error!("error from subscribe and run, {}", e);
                    // try reconnecting
                    // break;
                    client.disconnect().await;
                }
            } else {
                let disconnect_result = client.disconnect().await;
                if let Err(e) = disconnect_result {
                    warn!("error in disconnecting : {} , continue", e);
                }
            }
        } else {
            error!("error in connection : {:?}\n", conn_result);
        }
        // wait 1s before retry to reconnect
        wait_2s().await;
    } // loop to reconnect

    Ok(())
}

#[derive(Debug, StructOpt, Clone)]
#[structopt(name = "rsiotmonitor", about = "Command line arguments")]
struct Opt {
    /// Activate debug mode
    // short and long flags (-d, --debug) will be deduced from the field's name
    #[structopt(long)]
    debug: bool,

    #[structopt(long)]
    embeddedMqtt: bool,

    #[structopt(long, default_value = "0.0.0.0:1884")]
    embeddedMqttBindOptions: String,

    #[structopt(long)]
    disable: Option<String>,

    #[structopt(long)]
    enable: Option<String>,
}

async fn tcp_server_loop(
    broker_tx: tokio::sync::mpsc::Sender<BrokerMessage>,
    bind_options: String,
) -> io::Result<()> {
    info!("Listening on {}", bind_options);

    let listener = TcpListener::bind(bind_options).await?;

    loop {
        let (stream, addr) = listener.accept().await?;
        debug!("Client {} connected (tcp)", addr);
        client::spawn(stream, broker_tx.clone());
    }
}

/// Websocket tcp address TODO: make this configurable
const WEBSOCKET_TCP_LISTENER_ADDR: &str = "0.0.0.0:8088";

async fn websocket_server_loop(
    broker_tx: tokio::sync::mpsc::Sender<BrokerMessage>,
) -> io::Result<()> {
    info!("Listening on {}", WEBSOCKET_TCP_LISTENER_ADDR);

    let listener = TcpListener::bind(WEBSOCKET_TCP_LISTENER_ADDR).await?;

    loop {
        let (socket, addr) = listener.accept().await?;
        debug!("Client {} connected (websocket)", addr);
        client::spawn_websocket(socket, broker_tx.clone()).await;
    }
}

async fn launch_mqtt_server(opt: Opt) -> Result<(), Box<dyn std::error::Error>> {
    let broker = Broker::new();

    let broker_tx = broker.sender();
    let broker = tokio::task::spawn(async {
        broker.run().await;
    });

    let local_bind_option = opt.embeddedMqttBindOptions.clone();
    let local_broker_tx = broker_tx.clone();
    let tcp_listener = tokio::task::spawn(async move {
        tcp_server_loop(local_broker_tx.clone(), local_bind_option).await;
    });

    let websocket_listener = tokio::task::spawn(async move {
        websocket_server_loop(broker_tx).await;
    });

    // tcp_listener,
    // ,  websocket_listener

    let http_server = tokio::task::spawn(async move {
        httpserver::server_start().await;
    });

    try_join_all([broker, tcp_listener, websocket_listener, http_server]).await?;

    Ok(())
}

/// main procedure
#[tokio::main]
async fn main() {
    env_logger::init();

    let opt = Opt::from_args();
    let mut broker_spawn: JoinHandle<()>;
    if opt.embeddedMqtt {
        info!("starting embedded mqtt {}", opt.embeddedMqttBindOptions);
        let cloned_opt = opt.clone();
        broker_spawn = tokio::spawn(async move {
            launch_mqtt_server(cloned_opt).await;
        });
    }

    let config = read_configuration().await.unwrap();

    debug!("config : {:?}\n", &config);

    // todo, communicate with the rsiotmonitor instance and trigger
    if let Some(name) = opt.enable {
        // enable the element
    }

    if let Some(name) = opt.disable {
        // disable the element
    }

    start(config).await.unwrap();
}

/// create a mqtt client using config properties
fn client_from_args(args: &MqttConfig) -> mqtt_async_client::Result<Client> {
    debug!("create client for parameters : {:?}", args);
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

/// convert integer to QOS enum
fn int_to_qos(qos: u8) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => panic!("Not reached"),
    }
}
