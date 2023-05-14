use std::ffi::{CStr, CString};
use std::fs::{read_dir, DirEntry, File, FileType, ReadDir};
use std::path::Path;
use std::process::Stdio;

use log::{debug, info};
use mqtt_async_client::Result;
use nix::unistd::setsid;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

use crate::AdditionalProcessInformation;

pub const MAGIC: &str = "IOTMONITORMAGIC";

#[derive(Debug)]
pub struct ProcessInformation {
    pub pid: u32,
    pub commmand_line_elements: Vec<String>,
}

/// get the process information structure from the pid
pub fn get_process_information(pid: u32) -> Result<ProcessInformation> {
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
        pid: pid,
        commmand_line_elements: v,
    })
}

/// iterator for browsing the processes
pub struct ProcessIterator {
    /// current elements in the iterator browsing
    elements: Box<ReadDir>,
}

/// process iterator to watch the running processes
impl ProcessIterator {
    pub fn new() -> Result<ProcessIterator> {
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


/// create the process
pub fn run_process_with_fork(name: &String, processinfo: &mut AdditionalProcessInformation) -> Result<()> {
    
    let MAGICPROCSSHEADER: String = String::from(MAGIC) + "_";

    use nix::sys::signal::*;
    use nix::unistd::{execv, fork, ForkResult};
    match unsafe { fork() } {
        Ok(ForkResult::Parent { child, .. }) => {
            let u: i32 = child.into();
            let r: u32 = u.try_into().unwrap();
            processinfo.pid = Some(r);
            info!("pid created for {} : {}", &name,r);
            unsafe {
                // avoid creating zombi when child exit
                signal(Signal::SIGCHLD, SigHandler::SigIgn);
            }
        }
        Ok(ForkResult::Child) => {
            let exec = format!(
                "echo {};{};echo END",
                MAGICPROCSSHEADER + name,
                processinfo.exec
            );
            debug!("running {} , {}",&name ,&exec);
            let args = &[
                &CString::new("/bin/bash").unwrap(),
                &CString::new("-c").unwrap(),
                &CString::new(exec.clone()).unwrap(),
            ];

            // dissociate from its parent
            setsid();
            // replace the process
            execv(&CString::new("/bin/bash").unwrap(), args);
            return Ok(());
        }
        Err(_) => println!("Fork failed"),
    }
    Ok(())
}

/// launch the process with the IOTMONITORING tag
pub fn launch_process(name: &String, processinfo: &mut AdditionalProcessInformation) -> Result<()> {
    
    let MAGICPROCSSHEADER: String = String::from(MAGIC) + "_";

    let exec = format!(
        "echo {};{};echo END",
        MAGICPROCSSHEADER + name,
        processinfo.exec
    );

    // construct command line

    let mut cmd = Command::new("bash");
    let all = cmd
        .args(&["-c", &exec])
        .env("IOTMONITORMAGIC", name.clone());

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

    let processname = name.clone();
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
