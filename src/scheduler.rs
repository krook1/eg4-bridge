use crate::prelude::*;


pub struct Scheduler {
    config: ConfigWrapper,
    channels: Channels,
}

impl Scheduler {
    pub fn new(config: ConfigWrapper, channels: Channels) -> Self {
        Self { config, channels }
    }

    pub async fn start(&self) -> Result<()> {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));

        loop {
            interval.tick().await;

            for inverter in self.config.enabled_inverters() {
                crate::coordinator::commands::timesync::TimeSync::new(self.channels.clone(), inverter)
                    .run()
                    .await?;
            }
        }
    }
}
