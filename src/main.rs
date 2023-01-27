// use futures_core::future;

#[allow(unused_imports)]
// #![deny(warnings)]
use futures_util::stream::{futures_unordered::FuturesUnordered, StreamExt};

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use mqtt_async_client::{
    client::{
        Client, KeepAlive, Publish as PublishOpts, QoS, Subscribe as SubscribeOpts, SubscribeTopic,
    },
    Error, Result,
};

#[cfg(feature = "tls")]
use rustls;
use sqlite::ConnectionWithFullMutex;
#[cfg(feature = "tls")]
use std::io::Cursor;

use structopt::StructOpt;

use tokio::time::Duration;
#[cfg(feature = "tls")]
use webpki_roots;

use rsiotmonitor::{process::ProcessIterator, *};
use std::{
    collections::HashMap,
    ops::{Deref, Index},
    sync::{Arc, RwLock},
    time::SystemTime,
};

use toml_parse::*;

/// read configuration from config.toml
async fn read_configuration() -> Result<Config> {
    let mut config = Config {
        mqtt_config: MqttConfig {
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
        },
        monitored_devices: HashMap::new(),
        state_connection: None,
    };

    use std::fs;
    let contents = fs::read_to_string("config.toml").expect("cannot read config.toml");

    let t = Toml::new(&contents);

    let devices: Vec<Box<MonitoringInfo>> = t.iter().fold(Vec::new(), |acc, i| {
        let mut m = acc;
        match i {
            toml_parse::Value::Table(table) => {
                if table.header() == "mqtt" {
                    rsiotmonitor::read_mqtt_config_table(&mut config.mqtt_config, table);
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

                    if isagent {
                        info!("reading process informations");
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

    let hash: HashMap<String, Box<MonitoringInfo>> =
        HashMap::from_iter(devices.into_iter().map(|e| (e.name.clone(), e)));

    config.monitored_devices = hash;

    Ok(config)
}

async fn wait_1s() -> Result<()> {
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

/**
 * does the evaluated topic contains the tested_topic match
 */
fn does_topic_match(tested_topic: &String, evaluated_topic: &String) -> bool {
    let mut tested = tested_topic.clone();
    if tested_topic.ends_with("#") {
        tested = (tested[0..tested.len()]).to_string();
    }

    evaluated_topic.starts_with(&tested)
}


async fn subscribe_and_run(config: &Arc<RwLock<Config>>, client: &mut Client) -> Result<()> {
    {
        let config_ref = config.write().unwrap();

        let subopts = SubscribeOpts::new(
            config_ref
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
    } // lock section

    // events loop
    loop {
        let r = client.read_subscriptions().await;

        debug!("Read r={:?}", r);

        if let Err(Error::Disconnected) = r {
            return Err(Error::Disconnected);
        }

        let result = r.unwrap();
        let topic = result.topic().to_string();
        let payload = result.payload();

        {
            let mut config_ref = config.read().unwrap();
            let connection = &config_ref.state_connection.to_owned();
            for (_name, c) in config_ref.monitored_devices.iter() {
                // hello
                if let Some(hello_topic) = c.hello_topic.clone() {
                    if does_topic_match(&hello_topic, &topic) {
                        // c.hello_count += 1;

                        match &c.state_topic {
                            Some(state_topic) => {
                                // send state associated to mqtt if exists
                                match connection {
                                    Some(conn) => {
                                        let state_result = state::read_state(conn, &state_topic);
                                        if let Ok(s) = state_result {
                                            let mut p = PublishOpts::new(state_topic.into(), s);
                                            p.set_qos(int_to_qos(1));
                                            p.set_retain(false);

                                            // async publish
                                            client.publish(&p);
                                        }
                                    }
                                    _ => {}
                                };
                            }
                            _ => {}
                        }
                    }
                }

                if let Some(state_topic) = c.state_topic.clone() {
                    if does_topic_match(&state_topic, &topic) {
                        match connection {
                            Some(c) => {
                                // send state associated to the
                                debug!("save state for {}", &state_topic);
                                if let Err(save_error) = state::save_state(c, &topic, &payload) {
                                    warn!(
                                        "error in saving the state for {} : {}",
                                        _name, save_error
                                    );
                                }
                            }
                            _ => {}
                        };
                    }
                }
            }
        }
    }
}

/// look for already running process to get theirs associated pid
/// and be able to check for alive process
fn wrap_already_exists_processes(config: Config) -> Config {
    // TODO refactor this
    debug!("start checking already exists processes");

    const MAGIC: &str = "IOTMONITORMAGIC";
    let MAGICPROCSSHEADER: String = String::from(MAGIC) + "_";

    let mut c: Config = config;

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
                                match c.monitored_devices.get_mut(&name) {
                                    Some(mi) => match &mut mi.associated_process_information {
                                        Some(api) => {
                                            api.pid = Some(p.pid);
                                            info!("process attached with pid {}", p.pid);
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

/// start method
async fn start(config: Config) -> Result<()> {
    // search for existing processes, and wrap their declaration

    let mut populated_config = wrap_already_exists_processes(config);

    let config_mqtt = populated_config.mqtt_config.clone();

    let mut client = match client_from_args(&config_mqtt) {
        Ok(client) => client,
        Err(e) => panic!("{}", e),
    };

    info!("opening database");
    populated_config.state_connection = Some(Arc::new(state::init().unwrap()));

    let mut config_mqtt_watchdoc = populated_config.mqtt_config.clone();

    let rw: RwLock<Config> = RwLock::new(populated_config);
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
                tokio::spawn(async move {
                    debug!("start watchdog");
                    let c = client2;

                    loop {
                        debug!("send hello");
                        let mut p = PublishOpts::new(
                            "hello".into(),
                            format!("{:?}", SystemTime::now()).as_bytes().to_vec(),
                        );
                        p.set_qos(int_to_qos(1));
                        p.set_retain(false);

                        if let Err(e) = c.publish(&p).await {
                            warn!("error in publishing the health check");
                        }

                        // check processes
                        {
                            let mut c = config_ref_check.write().unwrap();
                            for entries in c.monitored_devices.iter_mut() {
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
                                    additional_infos.restartedCount += 1;
                                }
                            }
                        }
                        wait_1s().await;
                    }
                });

                if let Err(e) = subscribe_and_run(&config_ref, &mut client).await {
                    error!("{}", e);
                    break;
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
        wait_1s().await;
    }

    Ok(())
}

#[derive(Debug, StructOpt)]
#[structopt(name = "rsiotmonitor", about = "Command line arguments")]
struct Opt {
    /// Activate debug mode
    // short and long flags (-d, --debug) will be deduced from the field's name
    #[structopt(long)]
    debug: bool,

    #[structopt(long)]
    disable: Option<String>,

    #[structopt(long)]
    enable: Option<String>,
}

/// main procedure
#[tokio::main]
async fn main() {
    env_logger::init();

    let opt = Opt::from_args();

    let mut config = read_configuration().await.unwrap();
    debug!("config : {:?}\n", &config);

    if let Some(name) = opt.enable {
        // enable the element
    }

    if let Some(name) = opt.disable {
        // disable the element
    }

    start(config).await.unwrap();
}

/// create a mqtt client using config properties
fn client_from_args(args: &MqttConfig) -> Result<Client> {
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
