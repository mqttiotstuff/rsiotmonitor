///
/// module for browing process and command line, and launch registered agents
///
pub mod process;

pub mod state;

pub mod httpserver;

pub mod mqtt_utils;

pub mod history;

pub mod config;

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use derivative::Derivative;
use history::History;



use log::debug;
use toml_parse::Value;

/// General iotmonitor configuration, with mqtt configuration and monitored device or agents
#[derive(Derivative)]
#[derivative(Debug)]
pub struct IOTMonitor {
    pub mqtt_config: crate::config::MqttConfig,

    pub history_topic: Option<String>,

    #[derivative(Debug = "ignore")]
    pub history: Option<Box<History>>,

    #[derivative(Debug = "ignore")]
    pub state_connection: Option<Arc<sqlite::ConnectionWithFullMutex>>,

    /// monitored elements
    monitored_devices: HashMap<String, Box<MonitoringInfo>>,
}

impl IOTMonitor {
    pub fn new(
        mqtt_config: crate::config::MqttConfig,
        state_connection: Option<Arc<sqlite::ConnectionWithFullMutex>>,
        monitored_devices: HashMap<String, Box<MonitoringInfo>>,
        history_topic: Option<String>,
        history: Option<Box<History>>,
    ) -> Self {
        let config = IOTMonitor {
            mqtt_config: mqtt_config,
            monitored_devices: monitored_devices,
            state_connection: None,
            history_topic,
            history,
        };
        config
    }

    pub fn monitored_devices(&self) -> &HashMap<String, Box<MonitoringInfo>> {
        &self.monitored_devices
    }

    pub fn monitored_devices_mut(&mut self) -> &mut HashMap<String, Box<MonitoringInfo>> {
        &mut self.monitored_devices
    }
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


#[derive(Debug)]
pub struct AdditionalProcessInformation {
    // pid is to track the process while running
    pub pid: Option<u32>,
    // process identifier attributed by IOTMonitor, to track existing processes
    // processIdentifier: []const u8 = "",
    pub exec: String,

    // last time the process is restarted
    pub last_restarted: Option<SystemTime>,

    // number of time, the process is restarted
    pub restarted_count: u64,
}

const INITIAL_DELAY_FOR_CHECKING: u64 = 30;

/// monitoring information functions
impl MonitoringInfo {
    pub fn create(name: String) -> Box<MonitoringInfo> {
        let b: MonitoringInfo = MonitoringInfo {
            name: name.clone(),
            watch_topics: Vec::new(),
            associated_process_information: None,
            enabled: true,
            hello_topic: None,
            timeout_value: Duration::from_secs(INITIAL_DELAY_FOR_CHECKING),
            next_contact: Some(SystemTime::now() + Duration::from_secs(INITIAL_DELAY_FOR_CHECKING)),
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
