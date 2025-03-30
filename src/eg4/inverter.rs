use crate::prelude::*;
use crate::eg4::packet::{Packet, TcpFrameFactory, WriteParam, Heartbeat};
use crate::eg4::packet_decoder::PacketDecoder;

use {
    async_trait::async_trait,
    serde::{Serialize, Serializer},
    tokio::io::{AsyncReadExt, AsyncWriteExt},
    std::time::Duration,
    net2::TcpStreamExt,
    std::sync::{Arc, Mutex},
};

use crate::coordinator::PacketStats;

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum ChannelData {
    Connected(Serial),  // strictly speaking, these two only ever go
    Disconnect(Serial), // inverter->coordinator, but eh.
    Packet(Packet),     // this one goes both ways through the channel.
    Shutdown,
    Heartbeat(Packet),
    ModbusError(config::Inverter, u8, crate::eg4::packet::ModbusError),
    SerialMismatch(config::Inverter, Serial, Serial),
}
pub type Sender = broadcast::Sender<ChannelData>;
pub type Receiver = broadcast::Receiver<ChannelData>;

// WaitForReply {{{
#[async_trait]
pub trait WaitForReply {
    #[cfg(not(feature = "mocks"))]
    const TIMEOUT: u64 = 30;

    #[cfg(feature = "mocks")]
    const TIMEOUT: u64 = 0; // fail immediately in tests

    async fn wait_for_reply(&mut self, packet: &Packet) -> Result<Packet>;
}
#[async_trait]
impl WaitForReply for Receiver {
    async fn wait_for_reply(&mut self, packet: &Packet) -> Result<Packet> {
        let start = std::time::Instant::now();
        let timeout_duration = std::time::Duration::from_secs(Self::TIMEOUT);

        loop {
            if start.elapsed() >= timeout_duration {
                bail!("Timeout waiting for reply to {:?} after {} seconds", packet, Self::TIMEOUT);
            }

            match (packet, self.try_recv()) {
                (
                    Packet::TranslatedData(td),
                    Ok(ChannelData::Packet(Packet::TranslatedData(reply))),
                ) => {
                    if td.datalog == reply.datalog
                        && td.register == reply.register
                        && td.device_function == reply.device_function
                    {
                        return Ok(Packet::TranslatedData(reply));
                    }
                }
                (Packet::ReadParam(rp), Ok(ChannelData::Packet(Packet::ReadParam(reply)))) => {
                    if rp.datalog == reply.datalog && rp.register == reply.register {
                        return Ok(Packet::ReadParam(reply));
                    }
                }
                (Packet::WriteParam(wp), Ok(ChannelData::Packet(Packet::WriteParam(reply)))) => {
                    if wp.datalog == reply.datalog && wp.register == reply.register {
                        return Ok(Packet::WriteParam(reply));
                    }
                }
                (Packet::Heartbeat(hb), Ok(ChannelData::Packet(Packet::Heartbeat(reply)))) => {
                    if hb.datalog == reply.datalog {
                        return Ok(Packet::Heartbeat(reply));
                    }
                }
                (_, Ok(ChannelData::Packet(_))) => {} // Mismatched packet, continue waiting
                (_, Ok(ChannelData::Heartbeat(_))) => { info!("heartbeat_rx from") } // Heartbeat received, continue waiting
                (_, Ok(ChannelData::Connected(_))) => {} // Connection status update, continue waiting
                (_, Ok(ChannelData::Disconnect(inverter_datalog))) => {
                    if inverter_datalog == packet.datalog() {
                        bail!("Inverter {} disconnected while waiting for reply", inverter_datalog);
                    }
                }
                (_, Ok(ChannelData::Shutdown)) => bail!("Channel shutdown received while waiting for reply"),
                (_, Ok(ChannelData::ModbusError(_, _, _))) => {} // Modbus error, continue waiting
                (_, Ok(ChannelData::SerialMismatch(_, _, _))) => {} // Serial mismatch, continue waiting
                (_, Err(broadcast::error::TryRecvError::Empty)) => {
                    // Channel empty, sleep briefly before retrying
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                (_, Err(err)) => bail!("Channel error while waiting for reply: {:?}", err),
            }
        }
    }
} // }}}

// Serial {{{
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Serial([u8; 10]);

impl Serial {
    pub fn new(bytes: &[u8]) -> Result<Self> {
        Ok(Self(bytes.try_into()?))
    }

    pub fn data(&self) -> [u8; 10] {
        self.0
    }

    pub fn as_bytes(&self) -> &[u8; 10] {
        &self.0
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }
}

impl From<[u8; 10]> for Serial {
    fn from(bytes: [u8; 10]) -> Self {
        Self(bytes)
    }
}

impl From<&[u8]> for Serial {
    fn from(bytes: &[u8]) -> Self {
        let mut result = [0u8; 10];
        result.copy_from_slice(&bytes[..std::cmp::min(bytes.len(), 10)]);
        Self(result)
    }
}

impl From<&str> for Serial {
    fn from(s: &str) -> Self {
        let mut result = [0u8; 10];
        let bytes = s.as_bytes();
        result.copy_from_slice(&bytes[..std::cmp::min(bytes.len(), 10)]);
        Self(result)
    }
}

impl std::fmt::Display for Serial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", String::from_utf8_lossy(&self.0))
    }
}

impl Serialize for Serial {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl std::str::FromStr for Serial {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 10 {
            return Err(anyhow!("inverter.rs:{} must be exactly 10 characters", s));
        }

        let mut r: [u8; 10] = Default::default();
        r.copy_from_slice(s.as_bytes());
        Ok(Self(r))
    }
}

impl std::fmt::Debug for Serial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", String::from_utf8_lossy(&self.0))
    }
} // }}}

#[derive(Clone)]
pub struct Inverter {
    config: ConfigWrapper,
    host: String,
    channels: Channels,
    shared_stats: Arc<Mutex<PacketStats>>,
}

const READ_TIMEOUT_SECS: u64 = 1; // Multiplier for read_timeout from config
const WRITE_TIMEOUT_SECS: u64 = 5; // Timeout for write operations
const RECONNECT_DELAY_SECS: u64 = 5; // Delay before reconnection attempts
const TCP_KEEPALIVE_SECS: u64 = 60; // TCP keepalive interval

impl Inverter {
    pub fn new(config: ConfigWrapper, inverter: &config::Inverter, channels: Channels) -> Self {
        Self {
            config: config.clone(),
            host: inverter.host().to_string(),
            channels,
            shared_stats: Arc::new(Mutex::new(PacketStats::default())),
        }
    }

    pub fn new_with_stats(config: ConfigWrapper, inverter: &config::Inverter, channels: Channels, shared_stats: Arc<Mutex<PacketStats>>) -> Self {
        Self {
            config: config.clone(),
            host: inverter.host().to_string(),
            channels,
            shared_stats,
        }
    }

    pub fn config(&self) -> config::Inverter {
        self.config
            .inverter_with_host(&self.host)
            .expect("can't find my inverter")
    }

    pub async fn start(&self) -> Result<()> {
        while let Err(e) = self.connect().await {
            error!("inverter {}: {}", self.config().datalog().map(|s| s.to_string()).unwrap_or_default(), e);
            info!(
                "inverter {}: reconnecting in {}s", 
                self.config().datalog().map(|s| s.to_string()).unwrap_or_default(), 
                RECONNECT_DELAY_SECS
            );
            tokio::time::sleep(std::time::Duration::from_secs(RECONNECT_DELAY_SECS)).await;
        }

        Ok(())
    }

    pub async fn stop(&self) {
        info!("Stopping inverter {}...", self.config().datalog().map(|s| s.to_string()).unwrap_or_default());
        
        // Send shutdown signal
        let _ = self.channels.to_inverter.send(ChannelData::Shutdown);
        
        // Give tasks time to process shutdown
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    pub async fn connect(&self) -> Result<()> {
        let inverter_config = self.config();
        info!(
            "Inverter {} attempting connection to {}:{}",
            inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default(),
            inverter_config.host(),
            inverter_config.port()
        );

        let inverter_hp = (inverter_config.host().to_owned(), inverter_config.port());

        // Attempt TCP connection with timeout
        let stream = match tokio::time::timeout(
            Duration::from_secs(WRITE_TIMEOUT_SECS * 2),
            tokio::net::TcpStream::connect(inverter_hp)
        ).await {
            Ok(Ok(stream)) => {
                info!("Inverter {} TCP connection established", 
                    inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default());
                stream
            },
            Ok(Err(e)) => bail!("Failed to connect to inverter: {}", e),
            Err(_) => bail!("Connection timeout after {} seconds", WRITE_TIMEOUT_SECS * 2),
        };

        // Configure TCP socket
        let std_stream = stream.into_std()?;
        if let Err(e) = std_stream.set_keepalive(Some(Duration::new(TCP_KEEPALIVE_SECS, 0))) {
            warn!("Failed to set TCP keepalive: {}", e);
        }
        
        let stream = tokio::net::TcpStream::from_std(std_stream)?;
        
        // Set TCP_NODELAY based on configuration
        if inverter_config.use_tcp_nodelay() {
            if let Err(e) = stream.set_nodelay(true) {
                warn!("Failed to set TCP_NODELAY: {}", e);
            }
        }

        info!("Inverter {} socket configured", 
            inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default());

        let (reader, writer) = stream.into_split();

        // Start sender and receiver tasks
        let sender_task = self.sender(writer);
        let receiver_task = self.receiver(reader);

        info!("Inverter {} tasks started", 
            inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default());

        // Send Connected message after tasks are started
        if let Some(datalog) = inverter_config.datalog() {
            info!("Inverter {} sending Connected message", datalog);
            let _ = self.channels.to_coordinator.send(crate::coordinator::ChannelData::Packet(Packet::Heartbeat(Heartbeat {
                datalog,
            })));
        }

        Ok(())
    }

    async fn sender(&self, mut writer: tokio::net::tcp::OwnedWriteHalf) -> Result<()> {
        let mut receiver = self.channels.to_inverter.subscribe();
        let inverter_config = self.config();
        let frame_factory = TcpFrameFactory::new(inverter_config.datalog().expect("datalog must be set"));

        loop {
            match receiver.recv().await {
                Ok(channel_data) => {
                    match channel_data {
                        ChannelData::Packet(packet) => {
                            let frame = frame_factory.create_frame(&packet)?;
                            let bytes = frame;

                            // Log the packet being sent
                            match &packet {
                                Packet::Heartbeat(_) => {
                                    info!("inverter {}: TX Heartbeat packet", 
                                          inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default());
                                    if let Ok(mut stats) = self.shared_stats.lock() {
                                        stats.packets_sent += 1;
                                        stats.heartbeat_packets_sent += 1;
                                    }
                                }
                                Packet::TranslatedData(td) => {
                                    info!("inverter {}: TX TranslatedData packet - function: {:?}, register: {}", 
                                          inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default(),
                                          td.device_function, td.register);
                                    if let Ok(mut stats) = self.shared_stats.lock() {
                                        stats.packets_sent += 1;
                                        stats.translated_data_packets_sent += 1;
                                    }
                                }
                                Packet::ReadParam(rp) => {
                                    info!("inverter {}: TX ReadParam packet - register: {}", 
                                          inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default(),
                                          rp.register);
                                    if let Ok(mut stats) = self.shared_stats.lock() {
                                        stats.packets_sent += 1;
                                        stats.read_param_packets_sent += 1;
                                    }
                                }
                                Packet::WriteParam(wp) => {
                                    info!("inverter {}: TX WriteParam packet - register: {}, values: {:?}", 
                                          inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default(),
                                          wp.register, wp.values);
                                    if let Ok(mut stats) = self.shared_stats.lock() {
                                        stats.packets_sent += 1;
                                        stats.write_param_packets_sent += 1;
                                    }
                                }
                            }

                            // Use timeout for write operations
                            match tokio::time::timeout(
                                Duration::from_secs(WRITE_TIMEOUT_SECS),
                                writer.write_all(&bytes)
                            ).await {
                                Ok(Ok(_)) => {
                                    // Ensure data is actually sent
                                    if let Err(_e) = writer.flush().await {
                                        bail!("Failed to write to socket for {}", inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default());
                                    }
                                }
                                Ok(Err(_e)) => bail!("Failed to write packet for {}", inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default()),
                                Err(_) => bail!("Write timeout after {} seconds for {}", WRITE_TIMEOUT_SECS, inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default()),
                            }
                        }
                        ChannelData::Shutdown => {
                            info!("inverter {}: sender received shutdown signal", 
                                inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default());
                            break;
                        }
                        _ => {}
                    }
                }
                Err(_e) => {
                    warn!("eg4:inverter.rs {}: sender channel closed", 
                        inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default());
                    break;
                }
            }
        }

        info!("eg4:inverter.rs {}: sender exiting", 
            inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default());
        Ok(())
    }

    // inverter -> coordinator
    async fn receiver(&self, mut socket: tokio::net::tcp::OwnedReadHalf) -> Result<()> {
        use std::time::Duration;
        use tokio::time::timeout;
        use {bytes::BytesMut, tokio_util::codec::Decoder};

        const MAX_BUFFER_SIZE: usize = 65536; // 64KB max buffer size
        let mut buf = BytesMut::with_capacity(MAX_BUFFER_SIZE); // Start with MAX_BUFFER_SIZE
        let mut decoder = PacketDecoder::new();
        let inverter_config = self.config();
        let mut to_inverter_rx = self.channels.to_inverter.subscribe();

        loop {
            // Check buffer capacity and prevent potential memory issues
            if buf.len() >= MAX_BUFFER_SIZE {
                bail!("Buffer overflow: received data exceeds maximum size of {} bytes", MAX_BUFFER_SIZE);
            }

            // Use select! to efficiently wait for either data or shutdown
            tokio::select! {
                // Check for shutdown signal
                msg = to_inverter_rx.recv() => {
                    match msg {
                        Ok(ChannelData::Shutdown) => {
                            info!("Receiver received shutdown signal for {}", inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default());
                            // Process any remaining data in buffer before exiting
                            while let Some(packet) = decoder.decode_eof(&mut buf)? {
                                if let Err(_e) = self.handle_incoming_packet(packet) {
                                    warn!("Failed to handle final packet during shutdown");
                                }
                            }
                            break;
                        }
                        Ok(_) => continue,
                        Err(_e) => {
                            warn!("Error receiving from channel");
                            continue;
                        }
                    }
                }

                // Wait for socket data with timeout
                read_result = async {
                    if inverter_config.read_timeout() > 0 {
                        timeout(
                            Duration::from_secs(inverter_config.read_timeout() * READ_TIMEOUT_SECS),
                            socket.read_buf(&mut buf)
                        ).await
                    } else {
                        Ok(socket.read_buf(&mut buf).await)
                    }
                } => {
                    let len = match read_result {
                        Ok(Ok(n)) => n,
                        Ok(Err(_e)) => bail!("Read error"),
                        Err(_) => bail!("No data received for {} seconds", inverter_config.read_timeout() * READ_TIMEOUT_SECS),
                    };

                    if len == 0 {
                        // Try to process any remaining data before disconnecting
                        while let Some(packet) = decoder.decode_eof(&mut buf)? {
                            if let Err(_e) = self.handle_incoming_packet(packet) {
                                warn!("Failed to handle final packet");
                            }
                        }
                        bail!("Connection closed by peer");
                    }

                    // Process received data
                    while let Some(packet) = decoder.decode(&mut buf)? {
                        let packet_clone = packet.clone();
                        info!("RX packet from {} !", inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default());
                        
                        // Validate and process the packet
                        self.compare_datalog(&packet)?;
                        if let Packet::TranslatedData(_) = packet {
                            self.compare_inverter(&packet)?;
                        }

                        // Track received packet
                        if let Ok(mut stats) = self.shared_stats.lock() {
                            stats.packets_received += 1;
                            match &packet {
                                Packet::Heartbeat(_) => stats.heartbeat_packets_received += 1,
                                Packet::TranslatedData(_) => stats.translated_data_packets_received += 1,
                                Packet::ReadParam(_) => stats.read_param_packets_received += 1,
                                Packet::WriteParam(_) => stats.write_param_packets_received += 1,
                            }
                        }

                        if let Err(_e) = self.handle_incoming_packet(packet_clone) {
                            warn!("Failed to handle packet");
                            // Continue processing other packets even if one fails
                            continue;
                        }
                    }
                }
            }
        }

        info!("inverter {}: receiver exiting", inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default());
        Ok(())
    }

    fn handle_incoming_packet(&self, packet: Packet) -> Result<()> {
        let inverter_config = self.config();
        match self.channels.from_inverter.send(ChannelData::Packet(packet.clone())) {
            Ok(_) => Ok(()),
            Err(_e) => {
                let packet_info = match &packet {
                    Packet::TranslatedData(td) => format!("TranslatedData(register={:?}, datalog={})", td.register, td.datalog),
                    Packet::ReadParam(rp) => format!("ReadParam(register={:?}, datalog={})", rp.register, rp.datalog),
                    Packet::WriteParam(wp) => format!("WriteParam(register={:?}, datalog={})", wp.register, wp.datalog),
                    Packet::Heartbeat(hb) => format!("Heartbeat(datalog={})", hb.datalog),
                };
                bail!("Failed to forward packet from inverter {} ({})", 
                    inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default(),
                    packet_info,
                );
            }
        }
    }

    pub fn compare_datalog(&self, packet: &Packet) -> Result<()> {
        if packet.datalog() != self.config().datalog().expect("datalog must be set") {
            warn!(
                "Datalog serial mismatch: packet={}, config={}. {}",
                packet.datalog(),
                self.config().datalog().map(|s| s.to_string()).unwrap_or_default(),
                if self.config.strict_data_check() {
                    "Configuration updates are disabled due to strict_data_check=true"
                } else {
                    "Updating configuration with new datalog serial"
                }
            );
            if !self.config.strict_data_check() {
                if let Err(e) = self.config.update_inverter_datalog(
                    self.config().datalog().expect("datalog must be set"),
                    packet.datalog(),
                ) {
                    error!("Failed to update datalog serial in config: {}", e);
                }
            }
        }
        Ok(())
    }

    pub fn compare_inverter(&self, packet: &Packet) -> Result<()> {
        if let Packet::TranslatedData(td) = packet {
            if td.inverter != self.config().serial().expect("serial must be set") {
                warn!(
                    "Inverter serial mismatch: packet={}, config={}. {}",
                    td.inverter,
                    self.config().serial().map(|s| s.to_string()).unwrap_or_default(),
                    if self.config.strict_data_check() {
                        "Configuration updates are disabled due to strict_data_check=true"
                    } else {
                        "Updating configuration with new inverter serial"
                    }
                );
                if !self.config.strict_data_check() {
                    if let Err(e) = self.config.update_inverter_serial(
                        self.config().serial().expect("serial must be set"),
                        td.inverter,
                    ) {
                        error!("Failed to update inverter serial in config: {}", e);
                    }
                }
            }
        }
        Ok(())
    }

    /// Set the inverter's output power limit
    pub async fn set_output_power_limit(&self, power_limit: u16) -> Result<()> {
        let packet = Packet::WriteParam(WriteParam {
            datalog: self.config().datalog().expect("datalog must be set"),
            register: 0x0001,
            values: vec![(power_limit >> 8) as u8, power_limit as u8],
        });
        self.channels.to_inverter.send(ChannelData::Packet(packet))?;
        Ok(())
    }

    /// Set the inverter's grid-tie mode
    pub async fn set_grid_tie_mode(&self, mode: u16) -> Result<()> {
        let packet = Packet::WriteParam(WriteParam {
            datalog: self.config().datalog().expect("datalog must be set"),
            register: 0x0002,
            values: vec![(mode >> 8) as u8, mode as u8],
        });
        self.channels.to_inverter.send(ChannelData::Packet(packet))?;
        Ok(())
    }

    /// Set the inverter's battery charge current
    pub async fn set_battery_charge_current(&self, current: u16) -> Result<()> {
        let packet = Packet::WriteParam(WriteParam {
            datalog: self.config().datalog().expect("datalog must be set"),
            register: 0x0003,
            values: vec![(current >> 8) as u8, current as u8],
        });
        self.channels.to_inverter.send(ChannelData::Packet(packet))?;
        Ok(())
    }

    /// Set the inverter's battery discharge current
    pub async fn set_battery_discharge_current(&self, current: u16) -> Result<()> {
        let packet = Packet::WriteParam(WriteParam {
            datalog: self.config().datalog().expect("datalog must be set"),
            register: 0x0004,
            values: vec![(current >> 8) as u8, current as u8],
        });
        self.channels.to_inverter.send(ChannelData::Packet(packet))?;
        Ok(())
    }

    /// Set the inverter's battery charge voltage
    pub async fn set_battery_charge_voltage(&self, voltage: u16) -> Result<()> {
        let packet = Packet::WriteParam(WriteParam {
            datalog: self.config().datalog().expect("datalog must be set"),
            register: 0x0005,
            values: vec![(voltage >> 8) as u8, voltage as u8],
        });
        self.channels.to_inverter.send(ChannelData::Packet(packet))?;
        Ok(())
    }

    /// Set the inverter's battery discharge cutoff voltage
    pub async fn set_battery_discharge_cutoff_voltage(&self, voltage: u16) -> Result<()> {
        let packet = Packet::WriteParam(WriteParam {
            datalog: self.config().datalog().expect("datalog must be set"),
            register: 0x0006,
            values: vec![(voltage >> 8) as u8, voltage as u8],
        });
        self.channels.to_inverter.send(ChannelData::Packet(packet))?;
        Ok(())
    }

    /// Set the inverter's AC charge current
    pub async fn set_ac_charge_current(&self, current: u16) -> Result<()> {
        let packet = Packet::WriteParam(WriteParam {
            datalog: self.config().datalog().expect("datalog must be set"),
            register: 0x0007,
            values: vec![(current >> 8) as u8, current as u8],
        });
        self.channels.to_inverter.send(ChannelData::Packet(packet))?;
        Ok(())
    }

    /// Set the inverter's AC charge voltage
    pub async fn set_ac_charge_voltage(&self, voltage: u16) -> Result<()> {
        let packet = Packet::WriteParam(WriteParam {
            datalog: self.config().datalog().expect("datalog must be set"),
            register: 0x0008,
            values: vec![(voltage >> 8) as u8, voltage as u8],
        });
        self.channels.to_inverter.send(ChannelData::Packet(packet))?;
        Ok(())
    }

    /// Set the inverter's AC charge frequency
    pub async fn set_ac_charge_frequency(&self, frequency: u16) -> Result<()> {
        let packet = Packet::WriteParam(WriteParam {
            datalog: self.config().datalog().expect("datalog must be set"),
            register: 0x0009,
            values: vec![(frequency >> 8) as u8, frequency as u8],
        });
        self.channels.to_inverter.send(ChannelData::Packet(packet))?;
        Ok(())
    }

    /// Set the inverter's AC charge power factor
    pub async fn set_ac_charge_power_factor(&self, power_factor: u16) -> Result<()> {
        let packet = Packet::WriteParam(WriteParam {
            datalog: self.config().datalog().expect("datalog must be set"),
            register: 0x000A,
            values: vec![(power_factor >> 8) as u8, power_factor as u8],
        });
        self.channels.to_inverter.send(ChannelData::Packet(packet))?;
        Ok(())
    }

    /// Set the inverter's AC charge priority
    pub async fn set_ac_charge_priority(&self, priority: u16) -> Result<()> {
        let packet = Packet::WriteParam(WriteParam {
            datalog: self.config().datalog().expect("datalog must be set"),
            register: 0x000B,
            values: vec![(priority >> 8) as u8, priority as u8],
        });
        self.channels.to_inverter.send(ChannelData::Packet(packet))?;
        Ok(())
    }

    /// Set the inverter's AC charge time window
    pub async fn set_ac_charge_time(&self, start_hour: u8, start_minute: u8, end_hour: u8, end_minute: u8) -> Result<()> {
        let start_time = ((start_hour as u16) << 8) | (start_minute as u16);
        let packet = Packet::WriteParam(WriteParam {
            datalog: self.config().datalog().expect("datalog must be set"),
            register: 0x000C,
            values: vec![(start_time >> 8) as u8, start_time as u8],
        });
        self.channels.to_inverter.send(ChannelData::Packet(packet))?;

        let end_time = ((end_hour as u16) << 8) | (end_minute as u16);
        let packet = Packet::WriteParam(WriteParam {
            datalog: self.config().datalog().expect("datalog must be set"),
            register: 0x000D,
            values: vec![(end_time >> 8) as u8, end_time as u8],
        });
        self.channels.to_inverter.send(ChannelData::Packet(packet))?;
        Ok(())
    }

    /// Set the inverter's forced discharge mode
    pub async fn set_forced_discharge_mode(&self, enabled: bool) -> Result<()> {
        let value = if enabled { 1u16 } else { 0u16 };
        let packet = Packet::WriteParam(WriteParam {
            datalog: self.config().datalog().expect("datalog must be set"),
            register: 0x000E,
            values: vec![(value >> 8) as u8, value as u8],
        });
        self.channels.to_inverter.send(ChannelData::Packet(packet))?;
        Ok(())
    }

    /// Set the inverter's forced discharge time window
    pub async fn set_forced_discharge_time(&self, start_hour: u8, start_minute: u8, end_hour: u8, end_minute: u8) -> Result<()> {
        let start_time = ((start_hour as u16) << 8) | (start_minute as u16);
        let packet = Packet::WriteParam(WriteParam {
            datalog: self.config().datalog().expect("datalog must be set"),
            register: 0x000F,
            values: vec![(start_time >> 8) as u8, start_time as u8],
        });
        self.channels.to_inverter.send(ChannelData::Packet(packet))?;

        let end_time = ((end_hour as u16) << 8) | (end_minute as u16);
        let packet = Packet::WriteParam(WriteParam {
            datalog: self.config().datalog().expect("datalog must be set"),
            register: 0x0010,
            values: vec![(end_time >> 8) as u8, end_time as u8],
        });
        self.channels.to_inverter.send(ChannelData::Packet(packet))?;
        Ok(())
    }

    /// Set the inverter's forced discharge power
    pub async fn set_forced_discharge_power(&self, power: u16) -> Result<()> {
        let packet = Packet::WriteParam(WriteParam {
            datalog: self.config().datalog().expect("datalog must be set"),
            register: 0x0011,
            values: vec![(power >> 8) as u8, power as u8],
        });
        self.channels.to_inverter.send(ChannelData::Packet(packet))?;
        Ok(())
    }

    /// Set the inverter's grid-tie power limit
    pub async fn set_grid_tie_power_limit(&self, power_limit: u16) -> Result<()> {
        let packet = Packet::WriteParam(WriteParam {
            datalog: self.config().datalog().expect("datalog must be set"),
            register: 0x0012,
            values: vec![(power_limit >> 8) as u8, power_limit as u8],
        });
        self.channels.to_inverter.send(ChannelData::Packet(packet))?;
        Ok(())
    }

    /// Set the inverter's grid-tie frequency
    pub async fn set_grid_tie_frequency(&self, frequency: u16) -> Result<()> {
        let packet = Packet::WriteParam(WriteParam {
            datalog: self.config().datalog().expect("datalog must be set"),
            register: 0x0013,
            values: vec![(frequency >> 8) as u8, frequency as u8],
        });
        self.channels.to_inverter.send(ChannelData::Packet(packet))?;
        Ok(())
    }

    /// Set the inverter's grid-tie voltage
    pub async fn set_grid_tie_voltage(&self, voltage: u16) -> Result<()> {
        let packet = Packet::WriteParam(WriteParam {
            datalog: self.config().datalog().expect("datalog must be set"),
            register: 0x0014,
            values: vec![(voltage >> 8) as u8, voltage as u8],
        });
        self.channels.to_inverter.send(ChannelData::Packet(packet))?;
        Ok(())
    }

    /// Set the inverter's grid-tie power factor
    pub async fn set_grid_tie_power_factor(&self, power_factor: u16) -> Result<()> {
        let packet = Packet::WriteParam(WriteParam {
            datalog: self.config().datalog().expect("datalog must be set"),
            register: 0x0015,
            values: vec![(power_factor >> 8) as u8, power_factor as u8],
        });
        self.channels.to_inverter.send(ChannelData::Packet(packet))?;
        Ok(())
    }
}
