use failure::{format_err, Error};
use log::{error, info};
use reqwest::header::CONTENT_TYPE;
use serde::{Deserialize, Serialize};
use serde_json::json;

pub fn default_notifier() -> Option<Notifier> {
    return None;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Notifier {
    Slack(SlackNotifier),
}

pub trait Notify {
    async fn send_msg(&self, msg: &str) -> Result<(), Error>;
}

impl Notify for Notifier {
    async fn send_msg(&self, msg: &str) -> Result<(), Error> {
        match self {
            Notifier::Slack(notifier) => notifier.send_msg(msg).await,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SlackNotifier {
    pub webhook_url: String,
    pub owner_id: String,
}

impl SlackNotifier {
    pub fn new(webhook_url: String, owner_id: String) -> SlackNotifier {
        SlackNotifier {
            webhook_url,
            owner_id,
        }
    }
}

impl Notify for SlackNotifier {
    async fn send_msg(&self, msg: &str) -> Result<(), Error> {
        println!("Sending message to Slack: {}", msg);

        let client = reqwest::Client::new();

        let res = client
            .post(&self.webhook_url)
            .header(CONTENT_TYPE, "application/json")
            .json(&json!({ "text": format!("{}\ncc: <@{}>", msg, self.owner_id) }))
            .send()
            .await?;

        if res.status().is_success() {
            info!("Sent message to Slack successfully");
            Ok(())
        } else {
            let error_msg = res.text().await?;
            error!("Failed to send message to Slack: {}", error_msg);
            Err(format_err!("Failed to send message to Slack"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_slack_notifier_send_msg() {
        let notifier = SlackNotifier::new(
            "https://hooks.slack.com/services/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"
                .to_string(),
            "U06V024CRJL".to_string(),
        );
        let res = notifier.send_msg("Hello, world!").await;
        assert!(res.is_ok());
    }
}
