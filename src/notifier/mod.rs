use failure::{format_err, Error};
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::{os::unix::process::CommandExt, process::Command};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZaloNotifier {
    pub group_id: u64,
    pub sender_id: u64,
    pub owner_id: u64,
}

impl ZaloNotifier {
    pub fn new(group_id: u64, sender_id: u64, owner_id: u64) -> ZaloNotifier {
        ZaloNotifier {
            group_id,
            sender_id,
            owner_id,
        }
    }

    pub async fn send_msg(&self, msg: &str) -> Result<(), Error> {
        println!("Sending message to Zalo group: {}", msg);

        let mut command = Command::new("python");
        command
            .arg("pythons/send_notif.py")
            .arg(self.sender_id.to_string())
            .arg(msg)
            .arg(self.owner_id.to_string());

        command.spawn()?.wait()?;

        let output = command.output()?;


        if output.status.success() {
            info!("Sent message to Zalo group successfully");
            Ok(())
        } else {
            let error_msg = String::from_utf8(output.stderr)?;
            error!("Failed to send message to Zalo group: {}", error_msg);
            Err(format_err!("Failed to send message to Zalo group"))
        }
    }
}
