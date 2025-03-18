use crate::prelude::*;

use lxp::{
    inverter::WaitForReply,
    packet::{DeviceFunction, TranslatedData},
};

use super::validation::validate_register_block_boundary;
use super::read_hold::ReadHold;
use tokio::time::sleep;
use std::time::Duration;

pub struct ReadInputs {
    channels: Channels,
    inverter: config::Inverter,
    register: u16,
    count: u16,
}

impl ReadInputs {
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

    pub async fn run(&self) -> Result<Packet> {
        // Validate block boundaries before proceeding
        validate_register_block_boundary(self.register, self.count)?;

        let packet = Packet::TranslatedData(TranslatedData {
            datalog: self.inverter.datalog(),
            device_function: DeviceFunction::ReadInput,
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

        let result = receiver.wait_for_reply(&packet).await;

        // If read was successful, mark input registers as read
        if result.is_ok() {
            ReadHold::mark_input_registers_read();
            debug!("Input registers marked as read");
        }

        // Add delay after read operation
        let delay_ms = self.inverter.delay_ms();
        info!("Sleeping for {}ms after read input operation", delay_ms);
        sleep(Duration::from_millis(delay_ms)).await;

        result
    }
}
