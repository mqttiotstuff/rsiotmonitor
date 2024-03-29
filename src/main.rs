// use futures_core::future;

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

use futures_util::future::try_join_all;
#[allow(unused_imports)]
// #![deny(warnings)]
use futures_util::stream::{futures_unordered::FuturesUnordered, StreamExt};

use mqtt_async_client::{
    client::{Client, Publish as PublishOpts, Subscribe as SubscribeOpts, SubscribeTopic},
    Error,
};

use mqtt_v5_broker::{
    broker::{Broker, BrokerMessage},
    client,
};

use structopt::StructOpt;

use tokio::{net::TcpListener, time::Duration};

use rsiotmonitor::{history::History, process::ProcessIterator, *};
use std::{error::Error as RustError, io, path::PathBuf, sync::Arc, time::SystemTime};
use tokio_cron_scheduler::{Job, JobScheduler};

use crate::mqtt_utils::*;

async fn wait_2s() -> mqtt_async_client::Result<()> {
    tokio::time::sleep(Duration::from_secs(2)).await;
    Ok(())
}

async fn history_and_run(
    histo_topic: String,
    monitor: &Arc<tokio::sync::RwLock<IOTMonitor>>,
    client: &mut Client,
) -> mqtt_async_client::Result<()> {
    {
        debug!("history getting configuration");
        let _config_ref = monitor.write().await;

        debug!("history subscribing to topic :{}", histo_topic);
        let history_topic = SubscribeTopic {
            qos: int_to_qos(1),
            topic_path: histo_topic,
        };

        let subscribed_topics: Vec<SubscribeTopic> = vec![history_topic];

        let subopts = SubscribeOpts::new(subscribed_topics);
        debug!("history subscriptions to watch : {:?}", &subopts);
        let subres = client.subscribe(subopts).await?;
        subres.any_failures()?;

        info!("history activated");
    } // lock section on iotmonitor structure

    // events loop on subscription receive
    loop {
        // message received ?
        let r = client.read_subscriptions().await;

        debug!("receive for history r={:?}", r);

        if let Err(Error::Disconnected) = r {
            info!("disconnect received from subscription");
            return Err(Error::Disconnected);
        }

        if let Ok(result) = r {
            let topic = result.topic().to_string();
            let payload = result.payload();
            {
                let config_ref = monitor.write().await;
                if let Some(history) = &config_ref.history {
                    debug!("storing event");
                    if let Err(e) = history.store_event(topic, payload) {
                        error!("error in storing events : {}", e);
                    }
                }
            }
        }
    }
}

/// this function process events
async fn subscribe_and_run(
    monitor: &Arc<tokio::sync::RwLock<IOTMonitor>>,
    client: &mut Client,
) -> mqtt_async_client::Result<()> {
    {
        debug!("getting configuration");
        let config_ref = monitor.write().await;

        debug!("collecting and registering topics");
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
            info!("disconnect received from subscription");
            return Err(Error::Disconnected);
        }

        if let Ok(result) = r {
            let topic = result.topic().to_string();
            let payload = result.payload();
            {
                let mut config_ref = monitor.write().await;
                let connection = &config_ref.state_connection.to_owned();
                for (name, c) in config_ref.monitored_devices_mut().iter_mut() {
                    // hello
                    if let Some(hello_topic) = c.hello_topic.clone() {
                        if hello_topic.eq(&topic) {
                            // c.hello_count += 1;
                            info!("hello topic {} received", &hello_topic);
                            if let Some(c) = connection {
                                let get_all_states_result = state::get_all_states(c, name);
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
                                        state::save_state(c, &topic, name, payload)
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
                        if does_topic_match(watch_topics, &topic) {
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

    let magic_process_header: String = String::from(process::MAGIC) + "_";

    let mut c: IOTMonitor = config;

    // get the already running processes
    let pi = ProcessIterator::new().unwrap();
    for p in pi {
        for e in p.commmand_line_elements {
            // println!("evaluate {}",e);
            if e.contains(&magic_process_header) {
                debug!("found pattern evaluate {}", e);

                if let Some(idx1) = e.find(&magic_process_header) {
                    let s = &e[idx1 + magic_process_header.len()..];
                    if let Some(idx2) = s.find(';') {
                        let name = String::from(&s[0..idx2]);
                        debug!("{} found", &name);

                        if let Some(mi) = c.monitored_devices_mut().get_mut(&name) {
                            match &mut mi.associated_process_information {
                                Some(api) => {
                                    api.pid = Some(p.pid);
                                    info!("{} attached with pid {}", &name, p.pid);
                                }
                                None => {}
                            }
                        }

                        // match c.monitored_devices_mut().get_mut(&name) {
                        //     Some(mi) => match &mut mi.associated_process_information {
                        //         Some(api) => {
                        //             api.pid = Some(p.pid);
                        //             info!("{} attached with pid {}", &name, p.pid);
                        //         }
                        //         None => {}
                        //     },
                        //     None => {}
                        // }
                    }
                }
            }
        }
    }
    debug!("end of checking already exists processes");
    c
}

use chrono::DateTime;
use chrono::{offset::Utc, Datelike, Days};

fn export_history_day_from_datetime(
    hist_database: Arc<History>,
    triggered_date: DateTime<Utc>,
) -> Result<(), Box<dyn RustError>> {
    let previous_day = triggered_date - Days::new(1);

    let date = previous_day.date_naive();
    let year = date.year();
    let month = date.month();
    let day = date.day();

    let partitionned_dir_name =
        format!("history_archive/year={}/month={}/day={}", year, month, day);

    if let Err(e) = std::fs::create_dir_all(PathBuf::from(&partitionned_dir_name)) {
        error!("cannot create history_archive : {}", e);
        return Err(Box::new(e));
    }

    let filename: String = previous_day
        .format(&(partitionned_dir_name + "/events_%Y-%m-%d-%s.parquet"))
        .to_string();

    info!("writing parquet segment {}", &filename);
    if let Err(e) = hist_database.export_to_parquet(
        &filename,
        Some((
            previous_day.timestamp_micros(),
            triggered_date.timestamp_micros(),
        )),
        true,
    ) {
        error!("Error while exporting to parquet {}", e);
        return Err(e);
    }

    Ok(())
}

/// start method
#[allow(unreachable_code)]
async fn start(config: IOTMonitor) -> Result<(), Box<dyn RustError>> {
    // set scheduling

    if let Some(hist) = config.history.clone() {
        let sched = JobScheduler::new().await?;

        // every days at 00:00Z
        let job = Job::new_async("0 0 0 1-31 * *", move |_uuid, _l| {
            let local_hist = hist.clone();
            Box::pin(async move {
                let triggered_date = chrono::Utc::now();
                if let Err(e) = export_history_day_from_datetime(local_hist, triggered_date) {
                    error!("{}", e);
                }
            })
        })
        .expect("error in scheduling job");

        sched.add(job).await?;
        sched.start().await?;
    }

    // search for existing processes, and wrap their declaration

    let histo_topic = config.history_topic.clone();

    let mut populated_config = wrap_already_exists_processes(config);

    let base_topic = populated_config.mqtt_config.base_topic.clone();

    let config_mqtt = populated_config.mqtt_config.clone();

    let mut main_client_connection = match client_from_args(&config_mqtt) {
        Ok(client) => client,
        Err(e) => panic!("{}", e),
    };

    info!("opening state storage");
    populated_config.state_connection = Some(Arc::new(state::init()?));

    let mut config_mqtt_watchdog = populated_config.mqtt_config.clone();

    let rw: tokio::sync::RwLock<IOTMonitor> = tokio::sync::RwLock::new(populated_config);
    let config_ref = Arc::new(rw);

    let mut history_handle: Option<tokio::task::JoinHandle<()>> = None;

    //////////////////////////////////////////////////////////////////////////////////// history

    if history_handle.is_none() {
        if let Some(histo_topic_string) = histo_topic.clone() {
            debug!("there is an history configuration, launch the history functions");
            // prepare history run
            let mut config_ref_history = config_mqtt_watchdog.clone();

            assert!(&config_mqtt_watchdog.client_id.is_some());
            config_ref_history.client_id =
                Some(config_mqtt_watchdog.client_id.clone().unwrap() + "_history");
            let history_config_ref = config_ref.clone();

            history_handle = Some(tokio::spawn(async move {
                debug!("launch history");

                let mut history_subscription_client = match client_from_args(&config_ref_history) {
                    Ok(client) => client,
                    Err(e) => {
                        error!("error in connecting to mqtt broker : {}", e);
                        return;
                    } // cannot connect twice
                };

                loop {
                    if history_subscription_client.connect().await.is_ok() {
                        debug!("connected");

                        if let Err(e) = history_and_run(
                            histo_topic_string.clone(),
                            &history_config_ref,
                            &mut history_subscription_client,
                        )
                        .await
                        {
                            error!("error from subscribe history, {}", e);
                            // try reconnecting
                            // break;
                            let _r = history_subscription_client.disconnect().await;
                        };
                    }
                    let _ = wait_2s().await; // ignore the result
                }
            }));
            debug!("current handle : {:?}", history_handle);
        }
    }

    // main loop, reconnect
    loop {
        let conn_result = main_client_connection.connect().await;

        if let Ok(ok_result) = conn_result {
            info!("connected : {:?}\n", ok_result);

            if let Some(clientid) = config_mqtt_watchdog.client_id {
                config_mqtt_watchdog.client_id = Some(clientid.clone() + "_outbounds");
                let mut watch_dog_client = match client_from_args(&config_mqtt_watchdog) {
                    Ok(client) => client,
                    Err(e) => panic!("{}", e), // cannot connect twice
                };

                let config_ref_check = config_ref.clone();
                // we must test the main connexion first

                watch_dog_client
                    .connect()
                    .await
                    .expect("fail to connect to mqtt for watchdog purpose");

                debug!("connected");

                let l_base_topic = base_topic.clone();

                tokio::spawn(async move {
                    debug!("start watchdog");
                    let mut cnx_mqtt = watch_dog_client;

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
                            break; // end of the history process
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
                                    let process_launch_result =
                                        process::run_process_with_fork(name, additional_infos);
                                    if let Err(processerr) = process_launch_result {
                                        warn!("error launching process: {:?}", processerr);
                                    }
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
                                    format!("{}/expired/{}", l_base_topic, name),
                                    format!("{}", datetime.format("%d/%m/%Y %T"))
                                        .as_bytes()
                                        .to_vec(),
                                );
                                pexpired.set_qos(int_to_qos(1));
                                pexpired.set_retain(false);

                                let publish_result = cnx_mqtt.publish(&pexpired).await;
                                if let Err(_e) = publish_result {
                                    warn!("error in publishing the health check");
                                    break;
                                }
                            }
                        }

                        let _r = wait_2s().await;
                    } // loop

                    let _r = cnx_mqtt.disconnect().await;
                });

                // this is blocking
                if let Err(e) = subscribe_and_run(&config_ref, &mut main_client_connection).await {
                    error!("error from subscribe and run, {}", e);
                    // try reconnecting
                    let _r = main_client_connection.disconnect().await;
                }

                // we continue if the connection is lost
            } else {
                info!("no client id");
                let disconnect_result = main_client_connection.disconnect().await;
                if let Err(e) = disconnect_result {
                    warn!("error in disconnecting : {} , continue", e);
                }
            }
        } else {
            // main connection failed
            error!("error in connection : {:?}\n", conn_result);
        }

        // wait 1s before retry to reconnect

        let _ = wait_2s().await;
    } // loop to reconnect

    Ok(())
}

#[allow(dead_code)]
#[derive(Debug, StructOpt, Clone)]
#[structopt(name = "rsiotmonitor", about = "Command line arguments")]
struct Opt {
    /// Activate debug mode
    // short and long flags (-d, --debug) will be deduced from the field's name
    #[structopt(long)]
    debug: bool,

    #[structopt(long, name = "embeddedMqtt")]
    embedded_mqtt: bool,

    #[structopt(long, default_value = "0.0.0.0:1884", name = "embeddedMqttBindOptions")]
    embedded_mqtt_bind_options: String,

    #[structopt(long)]
    disable: Option<String>,

    #[structopt(long)]
    enable: Option<String>,

    #[structopt(long)]
    command_archive_history_date: Option<String>,

    #[structopt(long)]
    command_create_snapshot: Option<String>,
}

async fn tcp_server_loop(
    broker_tx: tokio::sync::mpsc::Sender<BrokerMessage>,
    bind_options: String,
) -> io::Result<()> {
    info!("MQTTServer Listening on {}", bind_options);

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
    info!(
        "MQTTServer Listening on {} for websocket",
        WEBSOCKET_TCP_LISTENER_ADDR
    );

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

    let local_bind_option = opt.embedded_mqtt_bind_options.clone();
    let local_broker_tx = broker_tx.clone();

    // tcp_listener,
    let tcp_listener = tokio::task::spawn(async move {
        let _r = tcp_server_loop(local_broker_tx.clone(), local_bind_option).await;
    });

    // websocket_listener
    let websocket_listener = tokio::task::spawn(async move {
        let _r = websocket_server_loop(broker_tx).await;
    });

    try_join_all([broker, tcp_listener, websocket_listener]).await?;

    Ok(())
}

/// main procedure
#[tokio::main]
async fn main() {
    env_logger::init();

    let opt = Opt::from_args();

    // handling commands
    if let Some(export_history_to_parse) = &opt.command_archive_history_date {
        println!(
            "handling history export on date : {}",
            export_history_to_parse
        );

        let config = crate::config::read_configuration().await.unwrap();
        if let Some(history) = config.history {
            let day: DateTime<Utc> = DateTime::parse_from_rfc3339(export_history_to_parse)
                .expect("error while parsing the date, date must be passed as iso860, eg: 2023-09-17T00:00:00Z")
                .into();
            export_history_day_from_datetime(history, day).unwrap();

            println!("export done");
            return;
        } else {
            panic!("no history configured, cannot export history on date");
        }
    }

    if let Some(export_snapshot) = &opt.command_create_snapshot {
        let config = crate::config::read_configuration().await.unwrap();

        if let Some(history) = config.history {
            if let Err(e) = history.export_to_parquet(export_snapshot, None, false) {
                error!("Error while exporting to parquet {}", e);
                panic!("error while exporting : {:?}", e);
            }
            println!("snapshot done");
            return;
        } else {
            panic!("no history configured, cannot create snapshot");
        }
    }

    if opt.embedded_mqtt {
        info!("Starting embedded mqtt {}", opt.embedded_mqtt_bind_options);
        let cloned_opt = opt.clone();

        let _broker_spawn = tokio::spawn(async move {
            let _r = launch_mqtt_server(cloned_opt).await;
            // ignore result
        });
    }

    let config = crate::config::read_configuration().await.unwrap();

    debug!("Starting with config : {:?}\n", &config);

    let _http_server = tokio::task::spawn(async move {
        httpserver::server_start(([0, 0, 0, 0], 3000)).await;
    });

    start(config).await.unwrap();
}
