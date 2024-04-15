#![feature(int_roundings)]
mod command_options;
mod croner;
mod job;
mod task;
mod notifier;

use command_options::CommandOptions;
use failure::Error;
use log4rs::{
    append::{
        console::ConsoleAppender,
        rolling_file::{
            policy::compound::{
                roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger, CompoundPolicy,
            },
            RollingFileAppender,
        },
    },
    config::Appender,
    encode::pattern::PatternEncoder,
    Config,
};
use structopt::StructOpt;

fn setup_loggin(log_folder: &str, log_level: &str) -> Result<(), Error> {
    let log_file = format!("{}/croner.log", log_folder);

    let console_appender = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new(
            "{d(%Y-%m-%d %H:%M:%S)} - {l:<5.10} - {t:<30.30} - {m}{n}",
        )))
        .build();

    let trigger = SizeTrigger::new(1024 * 1024 * 1024);
    let roller = FixedWindowRoller::builder()
        .build(&(log_file.clone() + "-{}"), 10)
        .unwrap();
    let policy = CompoundPolicy::new(Box::new(trigger), Box::new(roller));

    let file_appender = RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(
            "{d(%Y-%m-%d %H:%M:%S)} - {l:<5.10} - {t:<30.30} - {m}{n}",
        )))
        .build(log_file, Box::new(policy))
        .unwrap();

    let config = Config::builder()
        .appender(Appender::builder().build("console", Box::new(console_appender)))
        .appender(Appender::builder().build("file", Box::new(file_appender)))
        .build(
            log4rs::config::Root::builder()
                .appender("console")
                .appender("file")
                .build(log_level.parse().unwrap()),
        )
        .unwrap();

    log4rs::init_config(config).unwrap();
    Ok(())
}

#[tokio::main]
async fn main() {
    let command_options = CommandOptions::from_args();
    match command_options {
        CommandOptions::Start(start_options) => {
            let config_path = start_options.config_file;
            let croner = croner::Croner::load_from_toml(&config_path);

            let log_folder = croner.log_folder.clone();
            let log_level = croner.log_level.clone();
            setup_loggin(&log_folder, &log_level).unwrap();
            croner.run().await.unwrap();
        }
    }
}
