///
/// module for browing process and command line, and launch registered agents
///
pub mod process;

pub mod state;

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use derivative::Derivative;
use log::debug;
use toml_parse::Value;

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

/// General iotmonitor configuration, with mqtt configuration and monitored device or agents
#[derive(Derivative)]
#[derivative(Debug)]
pub struct Config {
    pub mqtt_config: MqttConfig,

    #[derivative(Debug = "ignore")]
    pub state_connection: Option<Arc<sqlite::ConnectionWithFullMutex>>,

    /// monitored elements
    pub monitored_devices: HashMap<String, Box<MonitoringInfo>>,
}

#[derive(Debug)]
pub struct AdditionalProcessInformation {
    // pid is to track the process while running
    pub pid: Option<u32>,
    // process identifier attributed by IOTMonitor, to track existing processes
    // processIdentifier: []const u8 = "",
    pub exec: String,

    // last time the process is restarted
    pub lastRestarted: Option<SystemTime>,

    // number of time, the process is restarted
    pub restartedCount: u64,
}

/// information about the monitored item (process or device)
#[derive(Debug)]
pub struct MonitoringInfo {
    /// device or agent name
    pub name: String,
    /// watched mqtt topics, to check if the device is alive
    pub watch_topics: Vec<String>,

    /// next contact time
    pub next_contact: Option<SystemTime>,
    /// timeout after the device is considered as non responding
    pub timeout_value: Duration,
    /// associated mqtt state topic, to record the device state
    pub state_topic: Option<String>,
    /// hello topic, signaling the device is newly activated
    pub hello_topic: Option<String>,
    /// is the monitoring activated ?
    pub enabled: bool,
    /// for agent this property hold the additional informations
    /// used for process launched or relaunched
    pub associated_process_information: Option<Box<AdditionalProcessInformation>>,

    /// count of the hello topic watched
    pub hello_count: u32,
}

/// monitoring information functions
impl MonitoringInfo {
    pub fn create(name: String) -> Box<MonitoringInfo> {
        let b: MonitoringInfo = MonitoringInfo {
            name: name.clone(),
            watch_topics: Vec::new(),
            associated_process_information: None,
            enabled: true,
            hello_topic: None,
            timeout_value: Duration::from_secs(30),
            next_contact: None,
            state_topic: None,
            hello_count: 0,
        };

        Box::new(b)
    }

    pub fn update_next_contact(self: &mut Self) {
        self.next_contact = Some(SystemTime::now());
    }

    pub fn has_expired(self: &Self) -> bool {
        if let Some(next_time) = self.next_contact {
            let current_time = SystemTime::now();
            let expired: bool = current_time > next_time + self.timeout_value;
            expired
        } else {
            false
        }
    }
}

/// in parsing, update the monitoinfo
pub fn update_monitorinfo_from_config_table(
    monitor_info: &mut MonitoringInfo,
    table: &toml_parse::Table,
) {
    for kv in table.items() {
        if let Some(keyname) = kv.key() {
            match kv.value() {
                Value::StrLit(s) => {
                    // watchTimeOut : watch dog for alive state, when the timeout is reached without and interactions on watchTopics, then iotmonitor trigger an expire message for the device
                    // helloTopic : the topic to observe to welcome the device. This topic trigger the state recovering for the device and agents. IotMonitor, resend the previous stored stateTopics
                    // watchTopics : the topic pattern to observe to know the device is alive
                    // stateTopics : list of topics for recording the states and reset them as they are welcomed

                    if keyname == "watchTopics" {
                        let mut topicList = Vec::new();
                        topicList.push(s.clone());
                        monitor_info.watch_topics = topicList;
                    } else if keyname == "stateTopics" {
                        monitor_info.state_topic = Some(s.clone());
                    } else if keyname == "helloTopic" {
                        monitor_info.hello_topic = Some(s.clone());
                    } else if keyname == "watchTimeOut" {
                        let d = s.parse::<u64>().unwrap();
                        let duration = Duration::from_secs(d);
                        monitor_info.timeout_value = duration;
                    } else {
                        debug!("unknown key {}", &keyname);
                    }
                }
                _ => (),
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
        lastRestarted: None,
        pid: None,
        restartedCount: 0,
    };

    for kv in table.items() {
        if let Some(keyname) = kv.key() {
            match kv.value() {
                Value::StrLit(s) => {
                    if keyname == "exec" {
                        additional_process_info.exec = s.clone();
                    }
                }
                _ => (),
            }
        }
    }

    monitor_info.associated_process_information = Some(Box::new(additional_process_info));
}

pub fn read_mqtt_config_table(config: &mut MqttConfig, table: &toml_parse::Table) {
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
