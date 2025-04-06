mod common;
use common::*;
use eg4_bridge::prelude::*;
use eg4_bridge::eg4;
use eg4_bridge::eg4::packet::{Packet, TranslatedData};
use eg4_bridge::eg4::inverter::Serial;
use eg4_bridge::coordinator::commands::update_hold::UpdateHold;
use eg4_bridge::eg4::inverter::ChannelData;
use eg4_bridge::prelude::Channels;

#[tokio::test]
async fn happy_path() {
    common_setup();

    let inverter = Factory::inverter();
    let channels = Channels::new();

    let register = eg4::packet::Register::Register21 as u16;
    let bit = eg4::packet::RegisterBit::AcChargeEnable;
    let enable = true;

    let subject = coordinator::commands::update_hold::UpdateHold::new(
        channels.clone(),
        inverter.clone(),
        register,
        bit,
        enable,
    );

    let sf = async {
        let result = subject.run().await;
        assert_eq!(
            result?,
            Packet::TranslatedData(eg4::packet::TranslatedData {
                datalog: inverter.datalog(),
                device_function: eg4::packet::DeviceFunction::WriteSingle,
                inverter: inverter.serial(),
                register: 21,
                values: vec![130, 0],
            })
        );

        Ok(())
    };

    let tf = async {
        let mut to_inverter = channels.to_inverter.subscribe();

        // wait for packet requesting current values
        assert_eq!(
            unwrap_inverter_channeldata_packet(to_inverter.recv().await?),
            Packet::TranslatedData(eg4::packet::TranslatedData {
                datalog: inverter.datalog(),
                device_function: eg4::packet::DeviceFunction::ReadHold,
                inverter: inverter.serial(),
                register: 21,
                values: vec![1, 0]
            })
        );

        // send reply with current values
        let reply = Packet::TranslatedData(eg4::packet::TranslatedData {
            datalog: inverter.datalog(),
            device_function: eg4::packet::DeviceFunction::ReadHold,
            inverter: inverter.serial(),
            register: 21,
            values: vec![2, 0],
        });
        channels
            .from_inverter
            .send(eg4::inverter::ChannelData::Packet(reply))?;

        // wait for packet setting new value
        assert_eq!(
            unwrap_inverter_channeldata_packet(to_inverter.recv().await?),
            Packet::TranslatedData(eg4::packet::TranslatedData {
                datalog: inverter.datalog(),
                device_function: eg4::packet::DeviceFunction::WriteSingle,
                inverter: inverter.serial(),
                register: 21,
                values: vec![130, 0] // 128 + 2
            })
        );

        // send reply with new value
        let reply = Packet::TranslatedData(eg4::packet::TranslatedData {
            datalog: inverter.datalog(),
            device_function: eg4::packet::DeviceFunction::WriteSingle,
            inverter: inverter.serial(),
            register: 21,
            values: vec![130, 0],
        });
        channels
            .from_inverter
            .send(eg4::inverter::ChannelData::Packet(reply))?;

        Ok::<(), anyhow::Error>(())
    };

    futures::try_join!(tf, sf).unwrap();
}

#[tokio::test]
async fn no_reply() {
    common_setup();

    let inverter = Factory::inverter();
    let channels = Channels::new();

    let register = eg4::packet::Register::Register21 as u16;
    let bit = eg4::packet::RegisterBit::AcChargeEnable;
    let enable = true;

    let subject = coordinator::commands::update_hold::UpdateHold::new(
        channels.clone(),
        inverter.clone(),
        register,
        bit,
        enable,
    );

    let sf = async {
        let result = subject.run().await;
        assert_eq!(
            result.unwrap_err().to_string(),
            "wait_for_reply TranslatedData(TranslatedData { datalog: 2222222222, device_function: ReadHold, inverter: 5555555555, register: 21, values: [1, 0] }) - timeout"
        );
        Ok::<(), anyhow::Error>(())
    };

    let tf = async {
        // wait for packet requesting current values
        assert_eq!(
            unwrap_inverter_channeldata_packet(channels.to_inverter.subscribe().recv().await?),
            Packet::TranslatedData(eg4::packet::TranslatedData {
                datalog: inverter.datalog(),
                device_function: eg4::packet::DeviceFunction::ReadHold,
                inverter: inverter.serial(),
                register: 21,
                values: vec![1, 0]
            })
        );

        // send no reply
        Ok::<(), anyhow::Error>(())
    };

    futures::try_join!(tf, sf).unwrap();
}
