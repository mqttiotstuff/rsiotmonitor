


pub mod process;

use std::time::{SystemTime, Duration};


#[derive(Debug)]
pub struct AdditionalProcessInformation {
    
        // pid is to track the process while running
        pid: Option<i64>,
        // process identifier attributed by IOTMonitor, to track existing processes
        // processIdentifier: []const u8 = "",
        
        exec: String,
    
        // last time the process is restarted
        lastRestarted: Option<SystemTime>,
    
        // number of time, the process is restarted
        restartedCount: u64,    
}


#[derive(Debug)]
pub struct MonitoringInfo {

        pub name: String,
        pub watch_topics: Vec<String>,
        pub next_contact: Option<SystemTime>,
        pub timeout_value: u32,
        pub state_topic: Option<String>,
        pub hello_topic: Option<String>,
        pub enabled: bool,

        pub associated_process_information: Option<Box<AdditionalProcessInformation>>


}

impl MonitoringInfo {

    pub fn create(name: String) -> Box<MonitoringInfo> {
        let mut b : MonitoringInfo = MonitoringInfo {
            name:name.clone(),
            watch_topics: Vec::new(),
            associated_process_information: None,
            enabled: true,
            hello_topic: None,
            timeout_value: 30,
            next_contact: None,
            state_topic: None
        };

        Box::new(b)
    }

    pub fn update_next_contact(self: &mut Self) {

    }

    pub fn has_expired(self: &Self) -> bool {
        if let Some(next_time) = self.next_contact {
            let current_time = SystemTime::now();
            let expired : bool = current_time > next_time + Duration::from_secs(self.timeout_value.into());
            expired
        } else {
            false
        }
    }

}




