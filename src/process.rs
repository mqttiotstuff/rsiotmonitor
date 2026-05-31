use std::ffi::CString;
use std::fmt;
use std::fs::{read_dir, DirEntry, File, FileType, ReadDir};
use std::path::Path;
use std::process::Stdio;
use std::time::{SystemTime, Instant};

use log::{debug, info};
use nix::unistd::setsid;
use std::error::Error;
use std::result::Result;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

use crate::AdditionalProcessInformation;

use std::convert::TryInto;
use std::collections::HashMap;
use std::sync::Mutex;

pub const MAGIC: &str = "IOTMONITORMAGIC";

// Cache for storing previous process statistics samples for CPU percentage calculation
use std::sync::OnceLock;

static PROCESS_STATS_CACHE: OnceLock<Mutex<HashMap<u32, (ProcessStatistics, Instant)>>> = OnceLock::new();

fn get_cache() -> &'static Mutex<HashMap<u32, (ProcessStatistics, Instant)>> {
    PROCESS_STATS_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

#[derive(Debug)]
pub struct ProcessInformation {
    pub pid: u32,
    pub commmand_line_elements: Vec<String>,
}

/// get the process information structure from the pid
pub fn get_process_information(
    pid: u32,
) -> Result<ProcessInformation, Box<dyn Error + Send + Sync>> {
    // read elements in process
    use std::io::{BufRead, BufReader};

    let cmdpath = format!("/proc/{}/cmdline", pid);
    let p = Path::new(&cmdpath);
    let f = File::open(p)?;
    let reader = BufReader::new(f);
    let mut v = Vec::new();

    for line in reader.lines() {
        for word in line.unwrap().split('\0') {
            v.push(String::from(word));
        }
    }

    Ok(ProcessInformation {
        pid,
        commmand_line_elements: v,
    })
}

/// Process statistics aligned with /proc/pid/status fields
///
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct ProcessStatistics {
    // Basic process information from /proc/pid/status
    pub name: String,         // Name: process name
    pub state: String,        // State: process state (R, S, D, Z, etc.)
    pub pid: u32,             // Pid: process ID
    pub threads: Option<u32>, // Threads: number of threads

    // Memory information from /proc/pid/status (values in kB)
    pub vm_peak_in_kb: Option<u64>, // VmPeak: peak virtual memory size
    pub vm_size_in_kb: Option<u64>, // VmSize: virtual memory size
    pub vm_rss_in_kb: Option<u64>,  // VmRSS: resident set size (physical memory)
    pub vm_hwm_in_kb: Option<u64>,  // VmHWM: peak resident set size
    pub vm_data_in_kb: Option<u64>, // VmData: data segment size
    pub vm_stk_in_kb: Option<u64>,  // VmStk: stack size
    pub vm_exe_in_kb: Option<u64>,  // VmExe: executable code size
    pub vm_lib_in_kb: Option<u64>,  // VmLib: library code size
    pub vm_swap_in_kb: Option<u64>, // VmSwap: swapped virtual memory

    pub rss_anon_in_kb: Option<u64>, // RssAnon: anonymous resident set size
    pub rss_file_in_kb: Option<u64>, // RssFile: file resident set size
    pub rss_shmem_in_kb: Option<u64>, // RssShmem: shared memory resident set size

    // Additional fields (not directly from /proc/pid/status)
    pub process_start_time: Option<SystemTime>, // Calculated from /proc/pid/stat

    pub process_current_cpu_ticks: Option<u64>, // Calculated from /proc/pid/stat
    pub process_total_system_cpu_usage_in_seconds: Option<f64>, // Calculated from /proc/pid/stat
    pub process_total_user_cpu_usage_in_seconds: Option<f64>, // Calculated from /proc/pid/stat
    pub cpu_percentage: Option<f64>, // CPU usage percentage (calculated from two samples)

}

impl ProcessStatistics {
    /// Create a new ProcessStatistics with default values
    pub fn new(pid: u32) -> ProcessStatistics {
        ProcessStatistics {
            name: String::new(),
            state: String::new(),
            pid,
            threads: None,
            vm_peak_in_kb: None,
            vm_size_in_kb: None,
            vm_rss_in_kb: None,
            vm_hwm_in_kb: None,
            vm_data_in_kb: None,
            vm_stk_in_kb: None,
            vm_exe_in_kb: None,
            vm_lib_in_kb: None,
            vm_swap_in_kb: None,

            rss_anon_in_kb: None,
            rss_file_in_kb: None,
            rss_shmem_in_kb: None,
            
            process_start_time: None,

            process_current_cpu_ticks: None,

            process_total_system_cpu_usage_in_seconds: None,
            process_total_user_cpu_usage_in_seconds: None,
            cpu_percentage: None,

        }
    }

    /// Calculate and set CPU percentage from a previous sample
    /// 
    /// This method calculates the CPU usage percentage by comparing
    /// this sample with a previous sample.
    /// 
    /// # Arguments
    /// * `previous` - Previous ProcessStatistics sample
    /// * `elapsed_seconds` - Time elapsed between samples in seconds
    /// 
    /// # Returns
    /// The calculated CPU percentage, or None if calculation is not possible
    pub fn calculate_cpu_percentage(
        &mut self,
        previous: &ProcessStatistics,
        elapsed_seconds: f64,
    ) -> Option<f64> {
        if elapsed_seconds <= 0.0 {
            return None;
        }

        let prev_ticks = previous.process_current_cpu_ticks?;
        let curr_ticks = self.process_current_cpu_ticks?;
        
        if curr_ticks < prev_ticks {
            // Process might have been restarted, can't calculate
            return None;
        }
        
        let ticks_diff = curr_ticks - prev_ticks;
        
        // Convert elapsed seconds to clock ticks
        const CLK_TCK: f64 = 100.0;
        let elapsed_ticks = elapsed_seconds * CLK_TCK;
        
        if elapsed_ticks <= 0.0 {
            return None;
        }
        
        // CPU usage = (cpu_ticks_used / elapsed_clock_ticks) * 100
        // This gives percentage of one CPU core (can exceed 100% for multi-threaded processes)
        let cpu_percentage = (ticks_diff as f64 / elapsed_ticks) * 100.0;
        self.cpu_percentage = Some(cpu_percentage);
        Some(cpu_percentage)
    }
}

/// this function read the process statistics, from the proc status file
pub fn get_process_statistics(pid: u32) -> Result<ProcessStatistics, Box<dyn Error + Send + Sync>> {
    /// parse the specific values with units
    fn parse_value_with_units(value: &str) -> Result<u64, Box<dyn Error + Send + Sync>> {
        let parts = value.split_whitespace().collect::<Vec<&str>>();
        if parts.len() >= 2 {
            log::debug!("in parse_value_with_units, parts: {:?}", parts);
            let value = parts[0].parse::<u64>()?;
            let units = parts[1].to_string();
            // unit is kB, MB, GB, TB, PB, EB, ZB, YB
            let unit_factor = match units.to_lowercase().as_str() {
                "kb" => 1024,
                "mb" => 1024 * 1024,
                "gb" => 1024 * 1024 * 1024,
                "tb" => 1024 * 1024 * 1024 * 1024,
                "pb" => 1024 * 1024 * 1024 * 1024 * 1024,
                _ => {
                    log::debug!("in parse_value_with_units, unrecognized units: {}", units);
                    return Err(Box::new(ProcessError {
                        message: format!("invalid units: {}", units),
                    }));
                }
            };
            return Ok(value * unit_factor / 1024); // normalize to kB
        }
        log::debug!("in parse_value_with_units, unrecognized value: {}", value);
        Err(Box::new(ProcessError {
            message: format!("invalid value: {}", value),
        }))
    }

    let mut process_statistics = ProcessStatistics::new(pid);

    // read elements in process
    use std::io::{BufRead, BufReader};

    // reading the /proc/{}/stat file
    // Format: pid (comm) state ppid ... utime stime ... num_threads ... starttime ...
    // Note: comm field (field 2) is in parentheses and may contain spaces
    let cmdpath = format!("/proc/{}/stat", pid);
    let p = Path::new(&cmdpath);
    let f = File::open(p)?;
    let reader = BufReader::new(f);
    let line = reader.lines().next().ok_or(Box::new(ProcessError {
        message: format!("failed to read stat file for pid: {}", pid),
    }))??;
    log::debug!("in get_process_statistics, stat: {:?}", line);
    
    // Parse stat file: find the closing parenthesis of comm field, then split the rest
    // Format: pid (comm) state ppid ...
    let opening_paren = line.find('(').ok_or(Box::new(ProcessError {
        message: format!("invalid stat file format for pid: {}", pid),
    }))?;
    let closing_paren = line.rfind(')').ok_or(Box::new(ProcessError {
        message: format!("invalid stat file format for pid: {}", pid),
    }))?;
    
    // Extract process name from comm field (remove parentheses)
    let comm_field = &line[opening_paren + 1..closing_paren];
    if !comm_field.is_empty() && process_statistics.name.is_empty() {
        process_statistics.name = comm_field.to_string();
    }
    
    // Extract fields after the comm field
    let after_comm = &line[closing_paren + 1..];
    let stat_fields: Vec<&str> = after_comm.split_whitespace().collect();
    
    // Field indices (0-indexed after comm field):
    // 0: state, 1: ppid, 2: pgrp, 3: session, 4: tty_nr, 5: tpgid, 6: flags,
    // 7: minflt, 8: cminflt, 9: majflt, 10: cmajflt,
    // 11: utime (field 14), 12: stime (field 15),
    // 13: cutime, 14: cstime, 15: priority, 16: nice,
    // 17: num_threads (field 20),
    // 18: itrealvalue, 19: starttime (field 22)
    
    const CLK_TCK: f64 = 100.0; // Clock ticks per second (standard on Linux)
    
    // Parse state (field 3) - index 0 after comm
    if !stat_fields.is_empty() && process_statistics.state.is_empty() {
        process_statistics.state = stat_fields[0].to_string();
    }
    
    // Parse utime (user CPU time in clock ticks) - field 14, index 11
    let mut utime_ticks: Option<u64> = None;
    if stat_fields.len() > 11 {
        if let Ok(ticks) = stat_fields[11].parse::<u64>() {
            utime_ticks = Some(ticks);
            let utime_seconds = ticks as f64 / CLK_TCK;
            process_statistics.process_total_user_cpu_usage_in_seconds = Some(utime_seconds);
        }
    }
    
    // Parse stime (system CPU time in clock ticks) - field 15, index 12
    let mut stime_ticks: Option<u64> = None;
    if stat_fields.len() > 12 {
        if let Ok(ticks) = stat_fields[12].parse::<u64>() {
            stime_ticks = Some(ticks);
            let stime_seconds = ticks as f64 / CLK_TCK;
            process_statistics.process_total_system_cpu_usage_in_seconds = Some(stime_seconds);
        }
    }
    
    // Calculate total CPU ticks (utime + stime)
    match (utime_ticks, stime_ticks) {
        (Some(utime), Some(stime)) => {
            process_statistics.process_current_cpu_ticks = Some(utime + stime);
        }
        (Some(utime), None) => {
            process_statistics.process_current_cpu_ticks = Some(utime);
        }
        (None, Some(stime)) => {
            process_statistics.process_current_cpu_ticks = Some(stime);
        }
        (None, None) => {
            // Leave as None
        }
    }
    
    // Parse num_threads - field 20, index 17
    if stat_fields.len() > 17 {
        if let Ok(threads) = stat_fields[17].parse::<u32>() {
            process_statistics.threads = Some(threads);
        }
    }
    
    // Parse starttime (field 22, index 19) - time since boot in clock ticks
    if stat_fields.len() > 19 {
        if let Ok(starttime_ticks) = stat_fields[19].parse::<u64>() {
            // Calculate process start time
            // Read /proc/uptime to get system uptime
            let uptime_path = Path::new("/proc/uptime");
            if let Ok(uptime_file) = File::open(uptime_path) {
                let mut uptime_reader = BufReader::new(uptime_file);
                let mut uptime_line = String::new();
                if uptime_reader.read_line(&mut uptime_line).is_ok() {
                    if let Some(uptime_str) = uptime_line.split_whitespace().next() {
                        if let Ok(uptime_seconds) = uptime_str.parse::<f64>() {
                            // Calculate boot time: current_time - uptime
                            let now = SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs() as f64;
                            let boot_time = now - uptime_seconds;
                            
                            // Process start time = boot_time + (starttime_ticks / CLK_TCK)
                            let process_start_seconds = boot_time + (starttime_ticks as f64 / CLK_TCK);
                            
                            // Convert to Duration and add to UNIX_EPOCH
                            let start_duration = std::time::Duration::from_secs_f64(process_start_seconds);
                            if let Some(start_time) = std::time::UNIX_EPOCH.checked_add(start_duration) {
                                process_statistics.process_start_time = Some(start_time);
                            }
                        }
                    }
                }
            } else {
                log::error!("failed to read uptime file for pid: {}", pid);
            }
        }
    }



    let cmdpath = format!("/proc/{}/status", pid);
    let p = Path::new(&cmdpath);
    let f = File::open(p)?;
    let reader = BufReader::new(f);

    for line in reader.lines() {
        let line = line.unwrap();
        let parts = line.split(':').collect::<Vec<&str>>();
        if parts.len() == 2 {
            let key = parts[0].trim();
            let value = parts[1].trim();
            log::debug!("key: {}, value: {}", key, value);
            match key {
                "Name" => process_statistics.name = value.to_string(),
                "State" => process_statistics.state = value.to_string(),
                "Pid" => {
                    if let Ok(pid_val) = value.parse::<u32>() {
                        process_statistics.pid = pid_val;
                    }
                }
                "Threads" => {
                    if let Ok(threads_val) = value.parse::<u32>() {
                        process_statistics.threads = Some(threads_val);
                    } else {
                        log::error!("invalid value for Threads: {}, continue", value);
                    }
                }
                "VmPeak" => {
                    // Format: "1234 kB"

                    if let Ok(kb_val) = parse_value_with_units(value) {
                        process_statistics.vm_peak_in_kb = Some(kb_val);
                    }
                }
                "VmSize" => {
                    if let Ok(kb_val) = parse_value_with_units(value) {
                        process_statistics.vm_size_in_kb = Some(kb_val);
                    }
                }
                "VmRSS" => {
                    if let Ok(kb_val) = parse_value_with_units(value) {
                        process_statistics.vm_rss_in_kb = Some(kb_val);
                    }
                }
                "VmHWM" => {
                    if let Ok(kb_val) = parse_value_with_units(value) {
                        process_statistics.vm_hwm_in_kb = Some(kb_val);
                    }
                }
                "VmData" => {
                    if let Ok(kb_val) = parse_value_with_units(value) {
                        process_statistics.vm_data_in_kb = Some(kb_val);
                    }
                }
                "VmStk" => {
                    if let Ok(kb_val) = parse_value_with_units(value) {
                        process_statistics.vm_stk_in_kb = Some(kb_val);
                    }
                }
                "VmExe" => {
                    if let Ok(kb_val) = parse_value_with_units(value) {
                        process_statistics.vm_exe_in_kb = Some(kb_val);
                    }
                }
                "VmLib" => {
                    if let Ok(kb_val) = parse_value_with_units(value) {
                        process_statistics.vm_lib_in_kb = Some(kb_val);
                    }
                }
                "VmSwap" => {
                    if let Ok(kb_val) = parse_value_with_units(value) {
                        process_statistics.vm_swap_in_kb = Some(kb_val);
                    }
                }, 
                "RssAnon" => {
                    if let Ok(kb_val) = parse_value_with_units(value) {
                        process_statistics.rss_anon_in_kb = Some(kb_val);
                    }
                },
                "RssFile" => {
                    if let Ok(kb_val) = parse_value_with_units(value) {
                        process_statistics.rss_file_in_kb = Some(kb_val);
                    }
                },
                "RssShmem" => {
                    if let Ok(kb_val) = parse_value_with_units(value) {
                        process_statistics.rss_shmem_in_kb = Some(kb_val);
                    }
                }
                _ => {
                    log::debug!("in get_process_statistics, unrecognized key: {}", key);
                }
            }
        }
    }

    // Try to calculate CPU percentage from cached previous sample
    let cache = get_cache();
    if let Ok(mut cache_guard) = cache.lock() {
        let now = Instant::now();
        
        if let Some((previous_stats, previous_time)) = cache_guard.get(&pid) {
            let elapsed = now.duration_since(*previous_time).as_secs_f64();
            
            // Only calculate if we have valid elapsed time (at least 0.1 seconds)
            if elapsed >= 0.1 {
                if let Some(cpu) =
                    process_statistics.calculate_cpu_percentage(previous_stats, elapsed)
                {
                    log::debug!(
                        "Calculated CPU percentage for pid {}: {:?}%",
                        pid,
                        cpu
                    );
                }
            }
        }
        
        // Update cache with current sample
        cache_guard.insert(pid, (process_statistics.clone(), now));
    }

    Ok(process_statistics)
}

/// Get CPU usage percentage for a process by taking two samples
/// 
/// This function takes two samples with a specified delay and calculates
/// the current CPU usage percentage.
/// 
/// # Arguments
/// * `pid` - Process ID
/// * `sample_interval_seconds` - Time to wait between samples (default: 1.0)
/// 
/// # Returns
/// ProcessStatistics with cpu_percentage populated, or error if sampling failed
pub fn get_process_statistics_with_cpu_percentage(
    pid: u32,
    sample_interval_seconds: f64,
) -> Result<ProcessStatistics, Box<dyn Error + Send + Sync>> {
    use std::thread;
    use std::time::Duration;
    
    // Take first sample
    let previous = get_process_statistics(pid)?;
    
    // Wait for the sample interval
    let sleep_duration = Duration::from_secs_f64(sample_interval_seconds);
    thread::sleep(sleep_duration);
    
    // Take second sample
    let mut current = get_process_statistics(pid)?;
    
    // Calculate CPU usage percentage
    current.calculate_cpu_percentage(&previous, sample_interval_seconds);
    
    Ok(current)
}

/// iterator for browsing the processes
pub struct ProcessIterator {
    /// current elements in the iterator browsing
    elements: Box<ReadDir>,
}

/// process iterator to watch the running processes
impl ProcessIterator {
    pub fn new() -> Result<ProcessIterator, Box<dyn Error + Send + Sync>> {
        let dir: &Path = Path::new("/proc");
        let entries = read_dir(dir)?;
        Ok(ProcessIterator {
            elements: Box::new(entries),
        })
    }
    /// return None if some elements could not match the process dir description
    pub fn try_read(result: std::io::Result<DirEntry>) -> Option<ProcessInformation> {
        if let Ok(d) = result {
            let filename = d.file_name();
            let filename_string = filename.to_str()?;
            let filetype = d.file_type();
            if let Ok(ft) = filetype {
                if FileType::is_dir(&ft) {
                    let number_result = String::from(filename_string).parse::<u32>();
                    if let Ok(pid) = number_result {
                        let pi = get_process_information(pid).unwrap();
                        return Some(pi);
                    }
                }
            }
            None
        } else {
            None
        }
    }
}

/// iterator for the process iterator
impl Iterator for ProcessIterator {
    type Item = ProcessInformation;

    // next() is the only required method
    fn next(&mut self) -> Option<Self::Item> {
        // Increment our count. This is why we started at zero.
        let e = self.elements.next();
        match e {
            Some(result) => match ProcessIterator::try_read(result) {
                Some(e) => Some(e),
                None => self.next(),
            },
            None => None,
        }
    }
}

#[test]
fn test_iterator() {
    let pi = ProcessIterator::new().unwrap();
    for entry in pi {
        println!("{:?}", &entry);
    }
}


#[test]
fn test_iterator_and_get_process_statistics() {
    let pi = ProcessIterator::new().unwrap();
    for entry in pi {
        let ps = get_process_statistics(entry.pid).unwrap();
        println!("{:?}", &ps);
    }
}


#[derive(Debug)]
struct ProcessError {
    message: String,
}

impl fmt::Display for ProcessError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Process error {}", self.message)
    }
}

impl Error for ProcessError {}

/// create the process, using fork
pub fn run_process_with_fork(
    name: &str,
    processinfo: &mut AdditionalProcessInformation,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let magicprocessheader: String = String::from(MAGIC) + "_";

    use nix::sys::signal::*;
    use nix::unistd::{execv, fork, ForkResult};
    match unsafe { fork() } {
        Ok(ForkResult::Parent { child, .. }) => {
            let u: i32 = child.into();
            let r: u32 = u.try_into().unwrap();
            processinfo.pid = Some(r);
            info!("pid created for {} : {}", &name, r);
            unsafe {
                // avoid creating zombi when child exit
                if let Err(e) = signal(Signal::SIGCHLD, SigHandler::SigIgn) {
                    return Err(Box::new(ProcessError {
                        message: format!("error in signal {}", e),
                    }));
                }
            }
        }
        Ok(ForkResult::Child) => {
            let exec = format!(
                "echo {};{};echo END",
                magicprocessheader + name,
                processinfo.exec
            );
            debug!("running {} , {}", &name, &exec);
            let args = &[
                &CString::new("/bin/bash").unwrap(),
                &CString::new("-c").unwrap(),
                &CString::new(exec).unwrap(),
            ];

            // dissociate from its parent
            if let Err(e) = setsid() {
                return Err(Box::new(ProcessError {
                    message: format!("error for setuid {}", e),
                }));
            }

            return match execv(&CString::new("/bin/bash").unwrap(), args) {
                Ok(_) => Ok(()),
                Err(e) => Err(Box::new(ProcessError {
                    message: format!("error in exec {}", e),
                })),
            };
        }
        Err(_) => println!("Fork failed"),
    }
    Ok(())
}

/// launch the process with the IOTMONITORING tag
pub fn launch_process(
    name: &str,
    processinfo: &mut AdditionalProcessInformation,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let magicprocessheader: String = String::from(MAGIC) + "_";

    let exec = format!(
        "echo {};{};echo END",
        magicprocessheader + name,
        processinfo.exec
    );

    // construct command line

    let mut cmd = Command::new("bash");
    let all = cmd.args(["-c", &exec]).env("IOTMONITORMAGIC", name);

    // Specify that we want the command's standard output piped back to us.
    // By default, standard input/output/error will be inherited from the
    // current process (for example, this means that standard input will
    // come from the keyboard and standard output/error will go directly to
    // the terminal if this process is invoked from the command line).
    all.stdout(Stdio::piped());

    let mut child = all.spawn().expect("failed to spawn command");

    processinfo.pid = child.id();

    let stdout = child
        .stdout
        .take()
        .expect("child did not have a handle to stdout");

    let mut reader = BufReader::new(stdout);

    // Ensure the child process is spawned in the runtime so it can
    // make progress on its own while we await for any output.
    tokio::spawn(async move {
        let status = child
            .wait()
            .await
            .expect("child process encountered an error");

        info!("child ended, with status was: {}", status);
    });

    let processname = name.to_string();
    tokio::spawn(async move {
        let mut s: String = "".into();
        while let Ok(size) = reader.read_line(&mut s).await {
            if size == 0 {
                break;
            }
            debug!("{} >>> {}", processname, s);
        }
    });

    Ok(())
}
