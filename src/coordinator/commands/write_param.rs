use crate::prelude::*;

use eg4::{
    inverter::WaitForReply,
    packet::{WriteParam as WriteParamPacket, Packet},
};

use crate::coordinator::Channels;
use crate::config;

pub struct WriteParam {
    channels: Channels,
    inverter: config::Inverter,
    register: u16,
    value: u16,
}

impl WriteParam {
    pub fn new<U>(channels: Channels, inverter: config::Inverter, register: U, value: u16) -> Self
    where
        U: Into<u16>,
    {
        Self {
            channels,
            inverter,
            register: register.into(),
            value,
        }
    }

    pub async fn run(&self) -> Result<Packet> {
        let packet = Packet::WriteParam(WriteParamPacket {
            datalog: self.inverter.datalog().expect("datalog must be set for write_param command"),
            register: self.register,
            values: self.value.to_le_bytes().to_vec(),
        });

        let mut receiver = self.channels.from_inverter.subscribe();

        if let Err(e) = self.channels.to_coordinator.send(crate::coordinator::ChannelData::SendPacket(packet.clone())) {
            bail!("Failed to send packet to coordinator: {}", e);
        }

        let packet = receiver.wait_for_reply(&packet).await?;
        // WriteParam packets seem to reply with 0 on success, very odd
        if packet.value() != 0 {
            bail!("failed to set register {}", self.register);
        }

        Ok(packet)
    }
}
