use crate::prelude::*;
use crate::coordinator::commands::time_register_ops;
use crate::coordinator::commands::set_hold::SetHold;
use crate::coordinator::commands::write_param::WriteParam;
use crate::coordinator::commands::time_register_ops::SetTimeRegister;

pub struct WriteInverter {
    channels: Channels,
    inverter: config::Inverter,
}

impl WriteInverter {
    pub fn new(channels: Channels, inverter: config::Inverter) -> Self {
        Self {
            channels,
            inverter,
        }
    }

    pub async fn set_ac_charge_rate(&self, value: u16) -> Result<()> {
        self.set_hold(0x0102_u16, value).await
    }

    pub async fn set_ac_charge_soc_limit(&self, value: u16) -> Result<()> {
        self.set_hold(0x0103_u16, value).await
    }

    pub async fn set_ac_charge_time(&self, config: ConfigWrapper, values: [u8; 4]) -> Result<()> {
        self.set_time_register(config, time_register_ops::Action::AcCharge(0), values).await
    }

    pub async fn set_ac_first_time(&self, config: ConfigWrapper, values: [u8; 4]) -> Result<()> {
        self.set_time_register(config, time_register_ops::Action::AcFirst(0), values).await
    }

    pub async fn set_charge_priority_time(&self, config: ConfigWrapper, values: [u8; 4]) -> Result<()> {
        self.set_time_register(config, time_register_ops::Action::ChargePriority(0), values).await
    }

    pub async fn set_charge_rate(&self, value: u16) -> Result<()> {
        self.set_hold(0x0100_u16, value).await
    }

    pub async fn set_discharge_cutoff_soc_limit(&self, value: u16) -> Result<()> {
        self.set_hold(0x0104_u16, value).await
    }

    pub async fn set_discharge_rate(&self, value: u16) -> Result<()> {
        self.set_hold(0x0101_u16, value).await
    }

    pub async fn set_forced_discharge_time(&self, config: ConfigWrapper, values: [u8; 4]) -> Result<()> {
        self.set_time_register(config, time_register_ops::Action::ForcedDischarge(0), values).await
    }

    pub async fn set_hold<U>(&self, register: U, value: u16) -> Result<()>
    where
        U: Into<u16>,
    {
        SetHold::new(
            self.channels.clone(),
            self.inverter.clone(),
            register,
            value,
        )
        .run()
        .await?;
        Ok(())
    }

    pub async fn set_param<U>(&self, register: U, value: u16) -> Result<()>
    where
        U: Into<u16>,
    {
        WriteParam::new(
            self.channels.clone(),
            self.inverter.clone(),
            register,
            value,
        )
        .run()
        .await?;
        Ok(())
    }

    pub async fn set_time_register(
        &self,
        config: ConfigWrapper,
        action: time_register_ops::Action,
        values: [u8; 4],
    ) -> Result<()> {
        SetTimeRegister::new(
            self.channels.clone(),
            self.inverter.clone(),
            config,
            action,
            values,
        )
        .run()
        .await
    }
} 