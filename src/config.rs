use log::{debug, info};

use std::{collections::HashMap, sync::Arc};

use toml_parse::{Toml, Value};

use crate::{history::History, AdditionalProcessInformation, IOTMonitor, MonitoringInfo};

use std::time::Duration;

use std::iter::FromIterator;

/// Mqtt connection properties and configuration
#[derive(Debug, Clone)]
pub struct MqttConfig {
    /// Username to authenticate with, optional.
    pub username: Option<String>,

    /// Password to authenticate with, optional.
    pub password: Option<String>,

    /// Host to connect to, REQUIRED.
    pub url: String,

    /// monitoring base topic
    pub base_topic: String,

    /// Client ID to identify as, optional.
    pub client_id: Option<String>,

    /// Enable TLS and set the path to a PEM file containing the
    /// CA certificate that signs the remote server's certificate.
    pub tls_server_ca_file: Option<String>,

    /// Enable TLS and trust the CA certificates in the webpki-roots
    /// crate, ultimately Mozilla's root certificates.
    pub tls_mozilla_root_cas: bool,

    /// Enable TLS and set the path to a PEM file containing the
    /// client certificate for client authentication.
    pub tls_client_crt_file: Option<String>,

    /// Enable TLS and set the path to a PEM file containing the
    /// client rsa key for client authentication.
    pub tls_client_rsa_key_file: Option<String>,

    /// Keepalive interval in seconds
    pub keep_alive: u16,

    /// Operation timeout in seconds
    pub op_timeout: u16,
}

impl Default for MqttConfig {
    fn default() -> Self {
        MqttConfig {
            username: None,
            password: None,
            url: "".into(),
            base_topic: "iotmonitor/monitoring".into(), // default
            client_id: None,
            tls_server_ca_file: None,
            tls_mozilla_root_cas: false,
            tls_client_crt_file: None,
            tls_client_rsa_key_file: None,
            keep_alive: 10,
            op_timeout: 10,
        }
    }
}

/// read configuration from config.toml
pub async fn read_configuration() -> mqtt_async_client::Result<IOTMonitor> {
    let mut mqtt_config = MqttConfig::default();

    use std::fs;
    log::info!("Reading configuration from config.toml");
    let contents = fs::read_to_string("config.toml").expect("cannot read config.toml");

    let t = Toml::new(&contents);

    let mut history_topic: Option<String> = None;
    let mut flight_sql_bind: Option<String> = None;
    let mut http_bind: Option<String> = None;
    let mut http_port: Option<u16> = None;
    let mut analytic_timeout_execute_secs: Option<u64> = None;
    let mut analytic_timeout_stream_secs: Option<u64> = None;
    let mut analytic_max_simultaneous_queries: Option<usize> = None;
    let mut analytic_small_profile: Option<bool> = None;

    let devices: Vec<Box<MonitoringInfo>> = t.iter().fold(Vec::new(), |acc, i| {
        let mut m = acc;
        if let toml_parse::Value::Table(table) = i {
            if table.header() == "mqtt" {
                log::debug!("Reading mqtt configuration");
                crate::config::read_mqtt_config_table(&mut mqtt_config, table);
            } else if table.header() == "http" {
                log::debug!("Reading http configuration");
                read_http_config_table(&mut http_bind, &mut http_port, table);
            } else if table.header() == "analytic" {
                log::debug!("Reading analytic configuration");
                read_analytic_config_table(
                    &mut flight_sql_bind,
                    &mut analytic_timeout_execute_secs,
                    &mut analytic_timeout_stream_secs,
                    &mut analytic_max_simultaneous_queries,
                    &mut analytic_small_profile,
                    table,
                );
            } else if table.header() == "history" {
                log::debug!("Reading history configuration");
                for kv in table.items() {
                    if let Some(keyname) = kv.key() {
                        if let Value::StrLit(s) = kv.value() {
                            if keyname == "storageTopic" {
                                history_topic = Some(s.clone());
                            }
                        }
                    }
                }
            } else {
                // create MonitorInfo
                log::debug!("Creating MonitorInfo for {}", table.header());
                let mut name: String = table.header().into();
                let mut isagent: bool = false;

                if table.header().starts_with("agent_") {
                    if let Some(without_suffix) = name.strip_prefix("agent_") {
                        name = without_suffix.into();
                        isagent = true;
                    }
                }

                let mut monitor_info = MonitoringInfo::create(name);

                crate::config::update_monitorinfo_from_config_table(&mut monitor_info, table);

                // only agent have process informations
                if isagent {
                    debug!("reading process informations");
                    crate::config::read_process_informations_from_config_table(
                        &mut monitor_info,
                        table,
                    )
                }

                // if this is an agent, add the additional elements
                m.push(monitor_info);
            }
        };
        m
    });

    // monitoring info by device name
    let hash: HashMap<String, Box<MonitoringInfo>> =
        HashMap::from_iter(devices.into_iter().map(|e| (e.name.clone(), e)));

    let mut opt_history: Option<Arc<History>> = None;
    if let Some(_topics_history) = history_topic.clone() {
        info!("history initialization");
        opt_history = Some(History::init().unwrap_or_else(|e| {
            log::error!("Error initializing history: {}", e);
            panic!("error while initializing history: {:?}", e);
        }));
    }

    let iotmonitor = IOTMonitor::new(
        mqtt_config,
        hash,
        history_topic,
        opt_history,
        flight_sql_bind,
        http_bind,
        http_port,
        analytic_timeout_execute_secs,
        analytic_timeout_stream_secs,
        analytic_max_simultaneous_queries,
        analytic_small_profile,
    );

    log::debug!("IOTMonitor created: {:?}", iotmonitor);

    Ok(iotmonitor)
}

/// in parsing, update the monitoinfo
pub fn update_monitorinfo_from_config_table(
    monitor_info: &mut MonitoringInfo,
    table: &toml_parse::Table,
) {
    for kv in table.items() {
        if let Some(keyname) = kv.key() {
            if let Value::StrLit(s) = kv.value() {
                match keyname {
                    // read keys from configuration : watchTimeOut, helloTopic, watchTopics, stateTopics
                    // watchTimeOut : watch dog for alive state, when the timeout is reached without and interactions on watchTopics, then iotmonitor trigger an expire message for the device
                    // helloTopic : the topic to observe to welcome the device. This topic trigger the state recovering for the device and agents. IotMonitor, resend the previous stored stateTopics
                    // watchTopics : the topic pattern to observe to know the device is alive
                    // stateTopics : list of topics for recording the states and reset them as they are welcomed
                    "watchTopics" => {
                        let topic_list = vec![s.clone()];
                        monitor_info.watch_topics = topic_list;
                    }
                    "stateTopics" => {
                        monitor_info.state_topic = Some(s.clone());
                    }
                    "helloTopic" => {
                        monitor_info.hello_topic = Some(s.clone());
                    }
                    "watchTimeOut" => {
                        let d = s.parse::<u64>().unwrap();
                        let duration = Duration::from_secs(d);
                        monitor_info.timeout_value = duration;
                    }
                    _ => debug!("unknown key {}", &keyname),
                }
            }
        }
    }
}

pub fn read_process_informations_from_config_table(
    monitor_info: &mut MonitoringInfo,
    table: &toml_parse::Table,
) {
    let mut additional_process_info = AdditionalProcessInformation {
        exec: "".into(),
        last_restarted: None,
        pid: None,
        restarted_count: 0,
    };

    for kv in table.items() {
        if let Some(keyname) = kv.key() {
            if let Value::StrLit(s) = kv.value() {
                if keyname == "exec" {
                    additional_process_info.exec = s.clone();
                }
            }
        }
    }

    monitor_info.associated_process_information = Some(Box::new(additional_process_info));
}

/// `[http]` bind address and port for the analytic HTTP server (CLI overrides when flags are set).
pub fn read_http_config_table(
    bind: &mut Option<String>,
    port: &mut Option<u16>,
    table: &toml_parse::Table,
) {
    assert!(table.header() == "http");
    for kv in table.items() {
        if let Some(keyname) = kv.key() {
            match keyname {
                "bind" => {
                    if let Value::StrLit(s) = kv.value() {
                        *bind = Some(s.clone());
                    }
                }
                "port" => {
                    if let Some(p) = http_port_from_value(kv.value()) {
                        *port = Some(p);
                    }
                }
                _ => debug!("unknown key in [http] section: {}", keyname),
            }
        }
    }
}

fn http_port_from_value(v: &Value) -> Option<u16> {
    match v {
        Value::Int(i) => {
            if *i >= 0 && *i <= u16::MAX as i64 {
                Some(*i as u16)
            } else {
                None
            }
        }
        Value::StrLit(s) => s.parse().ok(),
        _ => None,
    }
}

/// Optional `[analytic]` settings. Command-line flags override these when provided (see `main`).
pub fn read_analytic_config_table(
    flight_sql_bind: &mut Option<String>,
    timeout_execute_secs: &mut Option<u64>,
    timeout_stream_secs: &mut Option<u64>,
    max_simultaneous_queries: &mut Option<usize>,
    small_profile: &mut Option<bool>,
    table: &toml_parse::Table,
) {
    assert!(table.header() == "analytic");
    for kv in table.items() {
        if let Some(keyname) = kv.key() {
            let v = kv.value();
            match keyname {
                "flightSqlBind" => {
                    if let Value::StrLit(s) = v {
                        *flight_sql_bind = Some(s.clone());
                    }
                }
                "timeoutToExecuteQuery" => {
                    if let Some(n) = positive_u64(v) {
                        *timeout_execute_secs = Some(n);
                    }
                }
                "timeoutToStream" => {
                    if let Some(n) = positive_u64(v) {
                        *timeout_stream_secs = Some(n);
                    }
                }
                "maxSimultaneousQueries" => {
                    if let Some(n) = positive_usize(v) {
                        *max_simultaneous_queries = Some(n);
                    }
                }
                "smallProfile" => {
                    if let Value::Bool(b) = v {
                        *small_profile = Some(*b);
                    }
                }
                _ => debug!("unknown key in [analytic] section: {}", keyname),
            }
        }
    }
}

fn positive_u64(v: &Value) -> Option<u64> {
    match v {
        Value::Int(i) if *i > 0 => Some(*i as u64),
        Value::StrLit(s) => s.parse().ok(),
        _ => None,
    }
}

fn positive_usize(v: &Value) -> Option<usize> {
    match v {
        Value::Int(i) if *i > 0 && *i <= usize::MAX as i64 => Some(*i as usize),
        Value::StrLit(s) => s.parse().ok(),
        _ => None,
    }
}

pub fn read_mqtt_config_table(config: &mut MqttConfig, table: &toml_parse::Table) {
    assert!(table.header() == "mqtt");
    for kv in table.items() {
        if let Some(keyname) = kv.key() {
            if let Value::StrLit(s) = kv.value() {
                match keyname {
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
                        panic!("unknown mqtt section property : {}, only serverAddress, baseTopic, password, clientid, user are allowed", s);
                    }
                }
            }
        }
    }
}
