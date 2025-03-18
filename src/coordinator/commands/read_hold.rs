use crate::prelude::*;
use crate::coordinator::Channels;
use crate::config;
use lxp::packet::{DeviceFunction, TranslatedData, Packet};
use lxp::inverter::WaitForReply;

use super::validation::validate_register_block_boundary;

// Define Table 8 register ranges
const SYSTEM_INFO_RANGE: (u16, u16) = (0, 24);
const GRID_LIMITS_RANGE: (u16, u16) = (25, 28);
const GRID_PROTECTION_RANGE: (u16, u16) = (29, 53);
const POWER_QUALITY_RANGE: (u16, u16) = (54, 63);
const SYSTEM_CONTROL_RANGE: (u16, u16) = (64, 67);
const AC_CHARGE_RANGE: (u16, u16) = (160, 161);
const BATTERY_WARNING_RANGE: (u16, u16) = (162, 169);
const AUTOTEST_RANGE: (u16, u16) = (170, 175);

pub struct ReadHold {
    channels: Channels,
    inverter: config::Inverter,
    register: u16,
    count: u16,
}

impl ReadHold {
    pub fn new<U>(channels: Channels, inverter: config::Inverter, register: U, count: u16) -> Self
    where
        U: Into<u16>,
    {
        Self {
            channels,
            inverter,
            register: register.into(),
            count,
        }
    }

    fn is_valid_hold_register_range(start: u16, count: u16) -> bool {
        let end = start + count - 1;
        
        // Check if the range falls entirely within any of the valid Table 8 ranges
        let ranges = [
            SYSTEM_INFO_RANGE,
            GRID_LIMITS_RANGE,
            GRID_PROTECTION_RANGE,
            POWER_QUALITY_RANGE,
            SYSTEM_CONTROL_RANGE,
            AC_CHARGE_RANGE,
            BATTERY_WARNING_RANGE,
            AUTOTEST_RANGE,
        ];

        ranges.iter().any(|&(range_start, range_end)| {
            start >= range_start && end <= range_end
        })
    }

    pub async fn run(&self) -> Result<Packet> {
        // Validate block boundaries before proceeding
        validate_register_block_boundary(self.register, self.count)?;

        // Validate that we're only reading Table 8 registers
        if !Self::is_valid_hold_register_range(self.register, self.count) {
            bail!(
                "Invalid hold register range: registers {}-{} are not in Table 8. \
                Only Table 8 registers can be accessed using ReadHold.",
                self.register,
                self.register + self.count - 1
            );
        }

        let packet = Packet::TranslatedData(TranslatedData {
            datalog: self.inverter.datalog(),
            device_function: DeviceFunction::ReadHold,
            inverter: self.inverter.serial(),
            register: self.register,
            values: self.count.to_le_bytes().to_vec(),
        });

        let mut receiver = self.channels.from_inverter.subscribe();

        if self
            .channels
            .to_inverter
            .send(lxp::inverter::ChannelData::Packet(packet.clone()))
            .is_err()
        {
            bail!("send(to_inverter) failed - channel closed?");
        }

        receiver.wait_for_reply(&packet).await
    }
}
