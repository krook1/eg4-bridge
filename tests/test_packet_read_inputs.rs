mod common;
use common::*;
use eg4_bridge::prelude::*;
use eg4_bridge::eg4;
use eg4_bridge::eg4::packet::ReadInputs as PacketReadInputs;
use eg4_bridge::eg4::inverter::Serial;
use std::str::FromStr;
use eg4_bridge::eg4::packet::DeviceFunction;
use eg4_bridge::eg4::packet::Packet;
use eg4_bridge::eg4::packet::TranslatedData;
use eg4_bridge::eg4::inverter::ChannelData;
use eg4_bridge::coordinator::commands::read_inputs::ReadInputs;
use eg4_bridge::prelude::Channels;

#[test]
fn read_inputs_default() {
    let read_inputs = ReadInputs::default();
    assert_eq!(read_inputs.to_input_all(), None);
}

#[test]
fn read_inputs_set() {
    let mut read_inputs = ReadInputs::default();
    read_inputs.set_read_input_1(Factory::read_input_1());
    assert_eq!(read_inputs.to_input_all(), None);
}

#[tokio::test]
#[cfg_attr(not(feature = "mocks"), ignore)]
async fn handles_missing_read_input() {
    let mut read_inputs = ReadInputs::default();
    read_inputs.set_read_input_1(Factory::read_input_1());
    assert_eq!(read_inputs.to_input_all(), None);

    read_inputs.set_read_input_2(Factory::read_input_2());
    assert_eq!(read_inputs.to_input_all(), None);

    read_inputs.set_read_input_3(Factory::read_input_3());
    assert_eq!(read_inputs.to_input_all(), Some(Factory::read_input_all()));

    let mut read_inputs = ReadInputs::default();
    read_inputs.set_read_input_3(Factory::read_input_3());
    assert_eq!(read_inputs.to_input_all(), None);
}
