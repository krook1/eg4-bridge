use crate::prelude::*;
use crate::coordinator::commands::time_register_ops;
use crate::coordinator::commands::set_hold::SetHold;
use crate::coordinator::commands::write_param::WriteParam;
use crate::coordinator::commands::time_register_ops::SetTimeRegister;

/// WriteInverter handles all direct inverter operations.
/// The read_only check only applies to write operations (set_* functions).
/// Read operations are always allowed regardless of read_only setting.
pub struct WriteInverter {
    channels: Channels,
    inverter: config::Inverter,
    config: ConfigWrapper,
}

impl WriteInverter {
    pub fn new(channels: Channels, inverter: config::Inverter, config: ConfigWrapper) -> Self {
        Self {
            channels,
            inverter,
            config,
        }
    }

    /// Checks if write operations are allowed based on read_only settings.
    /// This check is only used for write operations (set_* functions).
    /// Read operations should not use this check.
    fn check_read_only(&self) -> Result<()> {
        if self.config.read_only() || self.inverter.read_only.unwrap_or(false) {
            Err(anyhow::anyhow!("Write operations are disabled in read-only mode"))
        } else {
            Ok(())
        }
    }

    /// Write operation: Sets AC charge rate
    /// Blocked by read_only setting
    pub async fn set_ac_charge_rate(&self, value: u16) -> Result<()> {
        self.check_read_only()?;
        self.set_hold(0x0102_u16, value).await
    }

    /// Write operation: Sets AC charge SOC limit
    /// Blocked by read_only setting
    pub async fn set_ac_charge_soc_limit(&self, value: u16) -> Result<()> {
        self.check_read_only()?;
        self.set_hold(0x0103_u16, value).await
    }

    /// Write operation: Sets AC charge time
    /// Blocked by read_only setting
    pub async fn set_ac_charge_time(&self, values: [u8; 4]) -> Result<()> {
        self.check_read_only()?;
        self.set_time_register(time_register_ops::Action::AcCharge(0), values).await
    }

    /// Write operation: Sets AC first time
    /// Blocked by read_only setting
    pub async fn set_ac_first_time(&self, values: [u8; 4]) -> Result<()> {
        self.check_read_only()?;
        self.set_time_register(time_register_ops::Action::AcFirst(0), values).await
    }

    /// Write operation: Sets charge priority time
    /// Blocked by read_only setting
    pub async fn set_charge_priority_time(&self, values: [u8; 4]) -> Result<()> {
        self.check_read_only()?;
        self.set_time_register(time_register_ops::Action::ChargePriority(0), values).await
    }

    /// Write operation: Sets charge rate
    /// Blocked by read_only setting
    pub async fn set_charge_rate(&self, value: u16) -> Result<()> {
        self.check_read_only()?;
        self.set_hold(0x0100_u16, value).await
    }

    /// Write operation: Sets discharge cutoff SOC limit
    /// Blocked by read_only setting
    pub async fn set_discharge_cutoff_soc_limit(&self, value: u16) -> Result<()> {
        self.check_read_only()?;
        self.set_hold(0x0104_u16, value).await
    }

    /// Write operation: Sets discharge rate
    /// Blocked by read_only setting
    pub async fn set_discharge_rate(&self, value: u16) -> Result<()> {
        self.check_read_only()?;
        self.set_hold(0x0101_u16, value).await
    }

    /// Write operation: Sets forced discharge time
    /// Blocked by read_only setting
    pub async fn set_forced_discharge_time(&self, values: [u8; 4]) -> Result<()> {
        self.check_read_only()?;
        self.set_time_register(time_register_ops::Action::ForcedDischarge(0), values).await
    }

    /// Write operation: Sets a holding register value
    /// Blocked by read_only setting
    pub async fn set_hold<U>(&self, register: U, value: u16) -> Result<()>
    where
        U: Into<u16>,
    {
        self.check_read_only()?;
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

    /// Write operation: Sets a parameter value
    /// Blocked by read_only setting
    pub async fn set_param<U>(&self, register: U, value: u16) -> Result<()>
    where
        U: Into<u16>,
    {
        self.check_read_only()?;
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

    /// Write operation: Sets a time register value
    /// Blocked by read_only setting
    pub async fn set_time_register(
        &self,
        action: time_register_ops::Action,
        values: [u8; 4],
    ) -> Result<()> {
        self.check_read_only()?;
        SetTimeRegister::new(
            self.channels.clone(),
            self.inverter.clone(),
            self.config.clone(),
            action,
            values,
        )
        .run()
        .await
    }
} 