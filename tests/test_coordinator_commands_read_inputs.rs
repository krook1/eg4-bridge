mod common;
use common::*;
use eg4_bridge::prelude::*;
use eg4_bridge::eg4;
use eg4_bridge::eg4::packet::{DeviceFunction, Packet, TranslatedData};
use eg4_bridge::eg4::inverter::Serial;
use eg4_bridge::coordinator::commands::read_inputs::ReadInputs;
use eg4_bridge::eg4::inverter::ChannelData;
use eg4_bridge::prelude::Channels;

#[tokio::test]
async fn happy_path() {
    common_setup();

    let inverter = Factory::inverter();
    let channels = Channels::new();

    let register = 0 as u16;
    let count = 40 as u16;

    let subject = coordinator::commands::read_inputs::ReadInputs::new(
        channels.clone(),
        inverter.clone(),
        register,
        count,
    );

    let reply = Packet::TranslatedData(eg4::packet::TranslatedData {
        datalog: inverter.datalog(),
        device_function: eg4::packet::DeviceFunction::ReadInput,
        inverter: inverter.serial(),
        register: 0,
        values: vec![0, 0],
    });

    let sf = async {
        let result = subject.run().await;
        assert_eq!(result?, reply.clone());
        Ok(())
    };

    let tf = async {
        channels.to_inverter.subscribe().recv().await?;
        channels
            .from_inverter
            .send(eg4::inverter::ChannelData::Packet(reply.clone()))?;
        Ok::<(), anyhow::Error>(())
    };

    futures::try_join!(tf, sf).unwrap();
}
