use failure::Error;
use log::{error, info};
use serde::{Deserialize, Serialize};

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

        let url = "http://10.30.58.19:8080/iapi/groupmsg/text";

        let client = reqwest::Client::new();
        let json_body = format!(
            r#"{{
                "group_id": {},
                "user_id": {},
                "tag_uids": {},
                "msg": "{}"
            }}"#,
            self.group_id, self.sender_id, self.owner_id, msg
        );

        let res = client.post(url).body(json_body).send().await?;

        if res.status().is_success() {
            info!("Message sent successfully");
        } else {
            error!("Failed to send message");
        }

        Ok(())
    }
}
