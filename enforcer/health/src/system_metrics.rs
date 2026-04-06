// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// Retrieves the current number of open file descriptors for the Enforcer process.
/// This reads from /proc/self/fd through the procfs crate.
/// Cost: Relatively cheap but involves reading the procfs directory self/fd.
pub fn get_system_fd_count() -> Option<i64> {
    procfs::process::Process::myself().ok().and_then(|p| p.fd_count().ok().map(|c| c as i64))
}

/// Retrieves the Resident Set Size (RSS) memory usage of the Enforcer process in bytes.
/// This reads from /proc/self/statm to get the number of resident pages and converts to bytes.
/// Cost: Very cheap as it reads a single, small virtual file containing basic numeric data.
pub fn get_system_memory_rss_bytes() -> Option<i64> {
    if let Ok(proc) = procfs::process::Process::myself() {
        if let Ok(statm) = proc.statm() {
            return Some(statm.resident as i64 * procfs::page_size() as i64);
        }
    }
    None
}

/// Retrieves a tuple containing (process_cpu_ticks, total_system_cpu_ticks).
/// This is used to compute the CPU utilization percentage between two samples.
///
/// Cost: Low (tens of microseconds), but requires string parsing of /proc/self/stat and /proc/stat. Best for background scraping, not hot paths.
pub fn get_system_cpu_ticks() -> Option<(u64, u64)> {
    use procfs::CurrentSI;
    let stat = procfs::process::Process::myself().ok()?.stat().ok()?;
    let process_ticks = stat.utime + stat.stime;

    let host_stat = procfs::KernelStats::current().ok()?;
    let total = host_stat.total;

    let total_ticks = total.user
        + total.nice
        + total.system
        + total.idle
        + total.iowait.unwrap_or(0)
        + total.irq.unwrap_or(0)
        + total.softirq.unwrap_or(0)
        + total.steal.unwrap_or(0)
        + total.guest.unwrap_or(0)
        + total.guest_nice.unwrap_or(0);

    if process_ticks > 0 && total_ticks > 0 {
        Some((process_ticks, total_ticks))
    } else {
        None
    }
}

pub fn calculate_cpu_usage(
    current_ticks: (u64, u64),
    last_sample: &mut Option<(u64, u64)>,
) -> Option<f64> {
    let (process_ticks, total_ticks) = current_ticks;
    let cpu_usage = if let Some((last_process, last_total)) = *last_sample {
        let process_diff = process_ticks.saturating_sub(last_process);
        let total_diff = total_ticks.saturating_sub(last_total);
        if total_diff > 0 {
            Some((process_diff as f64 / total_diff as f64) * 100.0)
        } else {
            None
        }
    } else {
        None
    };
    *last_sample = Some((process_ticks, total_ticks));
    cpu_usage
}
