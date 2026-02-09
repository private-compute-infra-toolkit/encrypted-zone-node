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
pub mod logger {
    #[cfg(feature = "debug")]
    // Sets up logging to console.
    pub fn setup_logging() -> Result<(), std::io::Error> {
        // See formatting details here:
        // https://docs.rs/log4rs/latest/log4rs/encode/pattern/index.html#formatters
        const DEFAULT_LOG_FMT: &str = "{d(%Y-%m-%d %H:%M:%S%.6f)} {l} {M}::{f}:{L} - {m}{n}";
        let console_appender = log4rs::append::console::ConsoleAppender::builder()
            .encoder(Box::new(log4rs::encode::pattern::PatternEncoder::new(DEFAULT_LOG_FMT)))
            .build();
        let config = log4rs::Config::builder()
            .appender(
                log4rs::config::Appender::builder()
                    .build("console_logs", Box::new(console_appender)),
            )
            .build(
                log4rs::config::Root::builder()
                    .appender("console_logs")
                    .build(log::LevelFilter::Info),
            )
            .unwrap();
        log4rs::init_config(config).unwrap();
        Ok(())
    }
    #[cfg(not(feature = "debug"))]
    pub fn setup_logging() -> Result<(), std::io::Error> {
        println!("Logging will be disabled in prod mode...");
        Ok(())
    }
}
