// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use clap::{Parser, ValueEnum};
use memmap2::{Mmap, MmapMut};
use rand::Rng;
use sha2::Digest;
use sha2::Sha256;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::panic::catch_unwind;
use std::thread;
use std::time::Duration;

const UDS_PATH: &str = "/ez/isolate_ipc";

#[derive(Default, Clone, ValueEnum)]
enum Operation {
    #[default]
    SendUds,
    ReadShm,
    WriteShm,
    VerifyReadOnlyShm,
    Sleep,
    CreateThread,
    ReadFile,
    InspectFsRoot,
    PingLoopback,
    EchoAllEnvs,
    ExitSuccess,
    ExitFail,
    TryWriteRoot,
}

#[derive(Parser)]
struct Args {
    #[arg(long, required = false, value_enum, default_value_t = Operation::default())]
    operation: Operation,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    match args.operation {
        Operation::SendUds => {
            // Send a fixed piece of data over UDS.
            let mut stream = UnixStream::connect(UDS_PATH)?;
            stream.write_all(b"EZ is cool")?;
        }
        Operation::ReadShm => {
            // Read data from a `filename` passed over UDS calculate its
            // digest and send the digest over UDS.
            let mut stream = UnixStream::connect(UDS_PATH)?;
            // Get filename from UDS.
            let mut buf = vec![0; 100];
            let n = stream.read(&mut buf)?;
            let filename = std::str::from_utf8(&buf[..n])?;
            let mut file = File::open(filename)?;
            let mut contents = Vec::new();
            file.read_to_end(&mut contents)?;
            let mmap = unsafe { Mmap::map(&file)? };
            stream.write_all(&Sha256::digest(&mmap[..]))?;
        }
        Operation::WriteShm => {
            // Write rand data to `filename` passed over UDS, calculate the
            // digest and send it over UDS.
            let mut stream = UnixStream::connect(UDS_PATH)?;
            // Get filename from UDS.
            let mut buf = vec![0; 100];
            let n = stream.read(&mut buf)?;
            let filename = std::str::from_utf8(&buf[..n])?;
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(filename)?;
            file.set_len(1000)?;
            let mut mmap = unsafe { MmapMut::map_mut(&file)? };
            let mut rng = rand::rng();
            for i in 0..1000 {
                mmap[i] = rng.random();
            }
            stream.write_all(&Sha256::digest(&mmap[..]))?;
        }
        Operation::VerifyReadOnlyShm => {
            // Try to write to a supposedly read-only file. The file name is
            // passed over UDS.
            // The goal is to verify that the file is non-mutable.
            // Report back over UDS whether the file is readable or writable.
            let mut stream = UnixStream::connect(UDS_PATH)?;
            // Get filename from UDS.
            let mut buf = vec![0; 100];
            let n = stream.read(&mut buf)?;
            let filename = std::str::from_utf8(&buf[..n])?;
            match OpenOptions::new().read(true).write(true).open(filename) {
                Ok(_) => stream.write_all(b"writable")?,
                Err(err) => stream.write_all(format!("{err}").as_bytes())?,
            };
        }
        Operation::Sleep => {
            std::thread::sleep(Duration::from_secs(1));
        }
        Operation::CreateThread => {
            let mut stream = UnixStream::connect(UDS_PATH)?;
            match catch_unwind(|| {
                let handle = thread::spawn(|| {});
                handle.join().unwrap();
            }) {
                Ok(_) => stream.write_all(b"thread created")?,
                Err(_) => stream.write_all(b"thread create failed")?,
            };
        }
        Operation::ReadFile => {
            let mut f = File::open("/message")?;
            let mut buf = Vec::new();
            f.read_to_end(&mut buf)?;
            let mut stream = UnixStream::connect(UDS_PATH)?;
            stream.write_all(&buf)?;
        }
        Operation::InspectFsRoot => {
            let mut buf = Vec::<u8>::new();
            for path in ["/a", "/ez", "/dev", "/proc", "/sys"].iter() {
                let entries = fs::read_dir(path)?;
                for entry in entries {
                    if let Some(name) = entry?.path().to_str() {
                        buf.extend_from_slice(name.as_bytes());
                        buf.push(b',');
                    }
                }
            }
            let mut stream = UnixStream::connect(UDS_PATH)?;
            stream.write_all(&buf)?;
        }
        Operation::PingLoopback => {
            let mut stream = UnixStream::connect(UDS_PATH)?;
            for target in ["::1", "127.0.0.1"] {
                match ping::ping(
                    target.parse()?,
                    Some(Duration::from_millis(500)),
                    None,
                    None,
                    None,
                    None,
                ) {
                    Ok(_) => {}
                    Err(_e) => {
                        stream.write_all(b"loopback ping failed")?;
                        return Ok(());
                    }
                }
            }
            stream.write_all(b"loopback addresses reachable")?;
        }
        Operation::EchoAllEnvs => {
            let mut stream = UnixStream::connect(UDS_PATH)?;
            let mut vars: Vec<String> = env::vars().map(|(k, v)| format!("{}={}", k, v)).collect();
            // Sort for deterministic output in tests.
            vars.sort();
            stream.write_all(vars.join(";").as_bytes())?;
        }
        Operation::ExitSuccess => {
            std::process::exit(0);
        }
        Operation::ExitFail => {
            std::process::exit(1);
        }
        Operation::TryWriteRoot => {
            let mut stream = UnixStream::connect(UDS_PATH)?;
            match File::create("/test_write") {
                Ok(_) => stream.write_all(b"writable")?,
                Err(e) => stream.write_all(e.to_string().as_bytes())?,
            }
        }
    }
    Ok(())
}
