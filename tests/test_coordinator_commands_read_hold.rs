mod common;
use common::*;
use eg4_bridge::prelude::*;
use eg4_bridge::eg4;
use eg4_bridge::eg4::packet::{Packet, TranslatedData};
use eg4_bridge::eg4::inverter::Serial;
use eg4_bridge::coordinator::commands::read_hold::ReadHold;
use eg4_bridge::eg4::inverter::ChannelData;
use eg4_bridge::prelude::Channels;

#[tokio::test]
async fn test_read_hold() -> Result<()> {
    let channels = Channels::new();
    let inverter = Inverter::new(1, "test".to_string(), "test".to_string());
    let serial = inverter.serial().unwrap();

    let register = 0;
    let count = 1;

    let mut command = ReadHold::new(channels.clone(), serial, register, count);
    let mut to_inverter = channels.to_inverter.subscribe();

    // Start the command
    command.start().await?;

    // Verify the command sent a ReadHold packet
    assert_eq!(
        unwrap_inverter_channeldata_packet(to_inverter.recv().await?),
        Packet::TranslatedData(eg4::packet::TranslatedData {
            datalog: inverter.datalog(),
            device_function: eg4::packet::DeviceFunction::ReadHold,
            inverter: serial,
            register,
            values: vec![0; count as usize],
        })
    );

    // Send a reply
    let reply = Packet::TranslatedData(eg4::packet::TranslatedData {
        datalog: inverter.datalog(),
        device_function: eg4::packet::DeviceFunction::ReadHold,
        inverter: serial,
        register: 0,
        values: vec![42],
    });

    channels
        .from_inverter
        .send(eg4::inverter::ChannelData::Packet(reply.clone()))?;

    // Verify the command completed successfully
    assert_eq!(command.run().await?, reply);

    Ok(())
}

#[tokio::test]
async fn happy_path() {
    common_setup();

    let inverter = Factory::inverter();
    let channels = Channels::new();

    let register = 0 as u16;
    let count = 40 as u16;

    let subject = coordinator::commands::read_hold::ReadHold::new(
        channels.clone(),
        inverter.clone(),
        register,
        count,
    );

    let reply = Packet::TranslatedData(eg4::packet::TranslatedData {
        datalog: inverter.datalog(),
        device_function: eg4::packet::DeviceFunction::ReadHold,
        inverter: inverter.serial(),
        register: 0,
        values: vec![0, 0],
    });

    let sf = async {
        let result = subject.run().await;
        assert_eq!(result?, reply.clone());
        Ok::<(), anyhow::Error>(())
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

#[tokio::test]
async fn no_reply() {
    common_setup();

    let inverter = Factory::inverter();
    let channels = Channels::new();

    let register = 0 as u16;
    let count = 40 as u16;

    let subject = coordinator::commands::read_hold::ReadHold::new(
        channels.clone(),
        inverter.clone(),
        register,
        count,
    );

    let sf = async {
        let result = subject.run().await;
        assert_eq!(
            result.unwrap_err().to_string(),
            "wait_for_reply TranslatedData(TranslatedData { datalog: 2222222222, device_function: ReadHold, inverter: 5555555555, register: 0, values: [40, 0] }) - timeout"
        );
        Ok::<(), anyhow::Error>(())
    };

    let tf = async {
        channels.to_inverter.subscribe().recv().await?;
        Ok::<(), anyhow::Error>(())
    };

    futures::try_join!(tf, sf).unwrap();
}

#[tokio::test]
async fn inverter_not_receiving() {
    common_setup();

    let inverter = Factory::inverter();
    let channels = Channels::new();

    let register = 0 as u16;
    let count = 40 as u16;

    let subject = coordinator::commands::read_hold::ReadHold::new(
        channels.clone(),
        inverter.clone(),
        register,
        count,
    );

    let sf = async {
        let result = subject.run().await;
        assert_eq!(
            result.unwrap_err().to_string(),
            "send(to_inverter) failed - channel closed?"
        );
        Ok::<(), anyhow::Error>(())
    };

    futures::try_join!(sf).unwrap();
}

#[tokio::test]
async fn test_read_hold_timeout() -> Result<()> {
    let channels = Channels::new();
    let inverter = Inverter::new(1, "test".to_string(), "test".to_string());
    let serial = inverter.serial().unwrap();

    let register = 0;
    let count = 1;

    let mut command = ReadHold::new(channels.clone(), serial, register, count);
    let mut to_inverter = channels.to_inverter.subscribe();

    // Start the command
    command.start().await?;

    // Verify the command sent a ReadHold packet
    assert_eq!(
        unwrap_inverter_channeldata_packet(to_inverter.recv().await?),
        Packet::TranslatedData(eg4::packet::TranslatedData {
            datalog: inverter.datalog(),
            device_function: eg4::packet::DeviceFunction::ReadHold,
            inverter: serial,
            register,
            values: vec![0; count as usize],
        })
    );

    // Don't send a reply, let it timeout
    assert!(command.run().await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_read_hold_wrong_inverter() -> Result<()> {
    let channels = Channels::new();
    let inverter = Inverter::new(1, "test".to_string(), "test".to_string());
    let serial = inverter.serial().unwrap();
    let wrong_inverter = Inverter::new(2, "test".to_string(), "test".to_string());
    let wrong_serial = wrong_inverter.serial().unwrap();

    let register = 0;
    let count = 1;

    let mut command = ReadHold::new(channels.clone(), serial, register, count);
    let mut to_inverter = channels.to_inverter.subscribe();

    // Start the command
    command.start().await?;

    // Verify the command sent a ReadHold packet
    assert_eq!(
        unwrap_inverter_channeldata_packet(to_inverter.recv().await?),
        Packet::TranslatedData(eg4::packet::TranslatedData {
            datalog: inverter.datalog(),
            device_function: eg4::packet::DeviceFunction::ReadHold,
            inverter: serial,
            register,
            values: vec![0; count as usize],
        })
    );

    // Send a reply from the wrong inverter
    let reply = Packet::TranslatedData(eg4::packet::TranslatedData {
        datalog: wrong_inverter.datalog(),
        device_function: eg4::packet::DeviceFunction::ReadHold,
        inverter: wrong_serial,
        register: 0,
        values: vec![42],
    });

    channels
        .from_inverter
        .send(eg4::inverter::ChannelData::Packet(reply))?;

    // Verify the command timed out (ignored the wrong inverter's reply)
    assert!(command.run().await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_read_hold_wrong_register() -> Result<()> {
    let channels = Channels::new();
    let inverter = Inverter::new(1, "test".to_string(), "test".to_string());
    let serial = inverter.serial().unwrap();

    let register = 0;
    let count = 1;

    let mut command = ReadHold::new(channels.clone(), serial, register, count);
    let mut to_inverter = channels.to_inverter.subscribe();

    // Start the command
    command.start().await?;

    // Verify the command sent a ReadHold packet
    assert_eq!(
        unwrap_inverter_channeldata_packet(to_inverter.recv().await?),
        Packet::TranslatedData(eg4::packet::TranslatedData {
            datalog: inverter.datalog(),
            device_function: eg4::packet::DeviceFunction::ReadHold,
            inverter: serial,
            register,
            values: vec![0; count as usize],
        })
    );

    // Send a reply with the wrong register
    let reply = Packet::TranslatedData(eg4::packet::TranslatedData {
        datalog: inverter.datalog(),
        device_function: eg4::packet::DeviceFunction::ReadHold,
        inverter: serial,
        register: 1,
        values: vec![42],
    });

    channels
        .from_inverter
        .send(eg4::inverter::ChannelData::Packet(reply))?;

    // Verify the command timed out (ignored the wrong register's reply)
    assert!(command.run().await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_read_hold_wrong_function() -> Result<()> {
    let channels = Channels::new();
    let inverter = Inverter::new(1, "test".to_string(), "test".to_string());
    let serial = inverter.serial().unwrap();

    let register = 0;
    let count = 1;

    let mut command = ReadHold::new(channels.clone(), serial, register, count);
    let mut to_inverter = channels.to_inverter.subscribe();

    // Start the command
    command.start().await?;

    // Verify the command sent a ReadHold packet
    assert_eq!(
        unwrap_inverter_channeldata_packet(to_inverter.recv().await?),
        Packet::TranslatedData(eg4::packet::TranslatedData {
            datalog: inverter.datalog(),
            device_function: eg4::packet::DeviceFunction::ReadHold,
            inverter: serial,
            register,
            values: vec![0; count as usize],
        })
    );

    // Send a reply with the wrong function
    let reply = Packet::TranslatedData(eg4::packet::TranslatedData {
        datalog: inverter.datalog(),
        device_function: eg4::packet::DeviceFunction::ReadInput,
        inverter: serial,
        register: 0,
        values: vec![42],
    });

    channels
        .from_inverter
        .send(eg4::inverter::ChannelData::Packet(reply))?;

    // Verify the command timed out (ignored the wrong function's reply)
    assert!(command.run().await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_read_hold_wrong_count() -> Result<()> {
    let channels = Channels::new();
    let inverter = Inverter::new(1, "test".to_string(), "test".to_string());
    let serial = inverter.serial().unwrap();

    let register = 0;
    let count = 1;

    let mut command = ReadHold::new(channels.clone(), serial, register, count);
    let mut to_inverter = channels.to_inverter.subscribe();

    // Start the command
    command.start().await?;

    // Verify the command sent a ReadHold packet
    assert_eq!(
        unwrap_inverter_channeldata_packet(to_inverter.recv().await?),
        Packet::TranslatedData(eg4::packet::TranslatedData {
            datalog: inverter.datalog(),
            device_function: eg4::packet::DeviceFunction::ReadHold,
            inverter: serial,
            register,
            values: vec![0; count as usize],
        })
    );

    // Send a reply with the wrong count
    let reply = Packet::TranslatedData(eg4::packet::TranslatedData {
        datalog: inverter.datalog(),
        device_function: eg4::packet::DeviceFunction::ReadHold,
        inverter: serial,
        register: 0,
        values: vec![42, 43],
    });

    channels
        .from_inverter
        .send(eg4::inverter::ChannelData::Packet(reply))?;

    // Verify the command timed out (ignored the wrong count's reply)
    assert!(command.run().await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_read_hold_wrong_datalog() -> Result<()> {
    let channels = Channels::new();
    let inverter = Inverter::new(1, "test".to_string(), "test".to_string());
    let serial = inverter.serial().unwrap();

    let register = 0;
    let count = 1;

    let mut command = ReadHold::new(channels.clone(), serial, register, count);
    let mut to_inverter = channels.to_inverter.subscribe();

    // Start the command
    command.start().await?;

    // Verify the command sent a ReadHold packet
    assert_eq!(
        unwrap_inverter_channeldata_packet(to_inverter.recv().await?),
        Packet::TranslatedData(eg4::packet::TranslatedData {
            datalog: inverter.datalog(),
            device_function: eg4::packet::DeviceFunction::ReadHold,
            inverter: serial,
            register,
            values: vec![0; count as usize],
        })
    );

    // Send a reply with the wrong datalog
    let reply = Packet::TranslatedData(eg4::packet::TranslatedData {
        datalog: "wrong".to_string(),
        device_function: eg4::packet::DeviceFunction::ReadHold,
        inverter: serial,
        register: 0,
        values: vec![42],
    });

    channels
        .from_inverter
        .send(eg4::inverter::ChannelData::Packet(reply))?;

    // Verify the command timed out (ignored the wrong datalog's reply)
    assert!(command.run().await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_read_hold_wrong_register_and_count() -> Result<()> {
    let channels = Channels::new();
    let inverter = Inverter::new(1, "test".to_string(), "test".to_string());
    let serial = inverter.serial().unwrap();

    let register = 0;
    let count = 1;

    let mut command = ReadHold::new(channels.clone(), serial, register, count);
    let mut to_inverter = channels.to_inverter.subscribe();

    // Start the command
    command.start().await?;

    // Verify the command sent a ReadHold packet
    assert_eq!(
        unwrap_inverter_channeldata_packet(to_inverter.recv().await?),
        Packet::TranslatedData(eg4::packet::TranslatedData {
            datalog: inverter.datalog(),
            device_function: eg4::packet::DeviceFunction::ReadHold,
            inverter: serial,
            register,
            values: vec![0; count as usize],
        })
    );

    // Send a reply with the wrong register and count
    let reply = Packet::TranslatedData(eg4::packet::TranslatedData {
        datalog: inverter.datalog(),
        device_function: eg4::packet::DeviceFunction::ReadHold,
        inverter: serial,
        register: 1,
        values: vec![42, 43],
    });

    channels
        .from_inverter
        .send(eg4::inverter::ChannelData::Packet(reply))?;

    // Verify the command timed out (ignored the wrong register and count's reply)
    assert!(command.run().await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_read_hold_wrong_register_and_function() -> Result<()> {
    let channels = Channels::new();
    let inverter = Inverter::new(1, "test".to_string(), "test".to_string());
    let serial = inverter.serial().unwrap();

    let register = 0;
    let count = 1;

    let mut command = ReadHold::new(channels.clone(), serial, register, count);
    let mut to_inverter = channels.to_inverter.subscribe();

    // Start the command
    command.start().await?;

    // Verify the command sent a ReadHold packet
    assert_eq!(
        unwrap_inverter_channeldata_packet(to_inverter.recv().await?),
        Packet::TranslatedData(eg4::packet::TranslatedData {
            datalog: inverter.datalog(),
            device_function: eg4::packet::DeviceFunction::ReadHold,
            inverter: serial,
            register,
            values: vec![0; count as usize],
        })
    );

    // Send a reply with the wrong register and function
    let reply = Packet::TranslatedData(eg4::packet::TranslatedData {
        datalog: inverter.datalog(),
        device_function: eg4::packet::DeviceFunction::ReadInput,
        inverter: serial,
        register: 1,
        values: vec![42],
    });

    channels
        .from_inverter
        .send(eg4::inverter::ChannelData::Packet(reply))?;

    // Verify the command timed out (ignored the wrong register and function's reply)
    assert!(command.run().await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_read_hold_wrong_register_and_datalog() -> Result<()> {
    let channels = Channels::new();
    let inverter = Inverter::new(1, "test".to_string(), "test".to_string());
    let serial = inverter.serial().unwrap();

    let register = 0;
    let count = 1;

    let mut command = ReadHold::new(channels.clone(), serial, register, count);
    let mut to_inverter = channels.to_inverter.subscribe();

    // Start the command
    command.start().await?;

    // Verify the command sent a ReadHold packet
    assert_eq!(
        unwrap_inverter_channeldata_packet(to_inverter.recv().await?),
        Packet::TranslatedData(eg4::packet::TranslatedData {
            datalog: inverter.datalog(),
            device_function: eg4::packet::DeviceFunction::ReadHold,
            inverter: serial,
            register,
            values: vec![0; count as usize],
        })
    );

    // Send a reply with the wrong register and datalog
    let reply = Packet::TranslatedData(eg4::packet::TranslatedData {
        datalog: "wrong".to_string(),
        device_function: eg4::packet::DeviceFunction::ReadHold,
        inverter: serial,
        register: 1,
        values: vec![42],
    });

    channels
        .from_inverter
        .send(eg4::inverter::ChannelData::Packet(reply))?;

    // Verify the command timed out (ignored the wrong register and datalog's reply)
    assert!(command.run().await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_read_hold_wrong_function_and_datalog() -> Result<()> {
    let channels = Channels::new();
    let inverter = Inverter::new(1, "test".to_string(), "test".to_string());
    let serial = inverter.serial().unwrap();

    let register = 0;
    let count = 1;

    let mut command = ReadHold::new(channels.clone(), serial, register, count);
    let mut to_inverter = channels.to_inverter.subscribe();

    // Start the command
    command.start().await?;

    // Verify the command sent a ReadHold packet
    assert_eq!(
        unwrap_inverter_channeldata_packet(to_inverter.recv().await?),
        Packet::TranslatedData(eg4::packet::TranslatedData {
            datalog: inverter.datalog(),
            device_function: eg4::packet::DeviceFunction::ReadHold,
            inverter: serial,
            register,
            values: vec![0; count as usize],
        })
    );

    // Send a reply with the wrong function and datalog
    let reply = Packet::TranslatedData(eg4::packet::TranslatedData {
        datalog: "wrong".to_string(),
        device_function: eg4::packet::DeviceFunction::ReadInput,
        inverter: serial,
        register: 0,
        values: vec![42],
    });

    channels
        .from_inverter
        .send(eg4::inverter::ChannelData::Packet(reply))?;

    // Verify the command timed out (ignored the wrong function and datalog's reply)
    assert!(command.run().await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_read_hold_wrong_register_and_function_and_datalog_and_count() -> Result<()> {
    let channels = Channels::new();
    let inverter = Inverter::new(1, "test".to_string(), "test".to_string());
    let serial = inverter.serial().unwrap();

    let register = 0;
    let count = 1;

    let mut command = ReadHold::new(channels.clone(), serial, register, count);
    let mut to_inverter = channels.to_inverter.subscribe();

    // Start the command
    command.start().await?;

    // Verify the command sent a ReadHold packet
    assert_eq!(
        unwrap_inverter_channeldata_packet(to_inverter.recv().await?),
        Packet::TranslatedData(eg4::packet::TranslatedData {
            datalog: inverter.datalog(),
            device_function: eg4::packet::DeviceFunction::ReadHold,
            inverter: serial,
            register,
            values: vec![0; count as usize],
        })
    );

    // Send a reply with the wrong register, function, datalog, and count
    let reply = Packet::TranslatedData(eg4::packet::TranslatedData {
        datalog: "wrong".to_string(),
        device_function: eg4::packet::DeviceFunction::ReadInput,
        inverter: serial,
        register: 1,
        values: vec![42, 43],
    });

    channels
        .from_inverter
        .send(eg4::inverter::ChannelData::Packet(reply))?;

    // Verify the command timed out (ignored the wrong register, function, datalog, and count's reply)
    assert!(command.run().await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_read_hold_wrong_register_and_function_and_datalog_and_count_and_inverter() -> Result<()> {
    let channels = Channels::new();
    let inverter = Inverter::new(1, "test".to_string(), "test".to_string());
    let serial = inverter.serial().unwrap();
    let wrong_inverter = Inverter::new(2, "test".to_string(), "test".to_string());
    let wrong_serial = wrong_inverter.serial().unwrap();

    let register = 0;
    let count = 1;

    let mut command = ReadHold::new(channels.clone(), serial, register, count);
    let mut to_inverter = channels.to_inverter.subscribe();

    // Start the command
    command.start().await?;

    // Verify the command sent a ReadHold packet
    assert_eq!(
        unwrap_inverter_channeldata_packet(to_inverter.recv().await?),
        Packet::TranslatedData(eg4::packet::TranslatedData {
            datalog: inverter.datalog(),
            device_function: eg4::packet::DeviceFunction::ReadHold,
            inverter: serial,
            register,
            values: vec![0; count as usize],
        })
    );

    // Send a reply with the wrong register, function, datalog, count, and inverter
    let reply = Packet::TranslatedData(eg4::packet::TranslatedData {
        datalog: "wrong".to_string(),
        device_function: eg4::packet::DeviceFunction::ReadInput,
        inverter: wrong_serial,
        register: 1,
        values: vec![42, 43],
    });

    channels
        .from_inverter
        .send(eg4::inverter::ChannelData::Packet(reply))?;

    // Verify the command timed out (ignored the wrong register, function, datalog, count, and inverter's reply)
    assert!(command.run().await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_read_hold_wrong_register_and_function_and_datalog_and_count_and_inverter_and_datalog_and_count_and_register() -> Result<()> {
    let channels = Channels::new();
    let inverter = Inverter::new(1, "test".to_string(), "test".to_string());
    let serial = inverter.serial().unwrap();
    let wrong_inverter = Inverter::new(2, "test".to_string(), "test".to_string());
    let wrong_serial = wrong_inverter.serial().unwrap();

    let register = 0;
    let count = 1;

    let mut command = ReadHold::new(channels.clone(), serial, register, count);
    let mut to_inverter = channels.to_inverter.subscribe();

    // Start the command
    command.start().await?;

    // Verify the command sent a ReadHold packet
    assert_eq!(
        unwrap_inverter_channeldata_packet(to_inverter.recv().await?),
        Packet::TranslatedData(eg4::packet::TranslatedData {
            datalog: inverter.datalog(),
            device_function: eg4::packet::DeviceFunction::ReadHold,
            inverter: serial,
            register,
            values: vec![0; count as usize],
        })
    );

    // Send a reply with the wrong register, function, datalog, count, inverter, datalog, count, and register
    let reply = Packet::TranslatedData(eg4::packet::TranslatedData {
        datalog: "wrong".to_string(),
        device_function: eg4::packet::DeviceFunction::ReadInput,
        inverter: wrong_serial,
        register: 2,
        values: vec![42, 43, 44],
    });

    channels
        .from_inverter
        .send(eg4::inverter::ChannelData::Packet(reply))?;

    // Verify the command timed out (ignored the wrong register, function, datalog, count, inverter, datalog, count, and register's reply)
    assert!(command.run().await.is_err());

    Ok(())
}
