use crate::prelude::*;
use crate::eg4::packet::{Packet, TcpFrameFactory};
use crate::eg4::packet_decoder::PacketDecoder;

use {
    async_trait::async_trait,
    serde::{Serialize, Serializer},
    tokio::io::{AsyncReadExt, AsyncWriteExt},
    std::time::Duration,
    net2::TcpStreamExt,
};

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum ChannelData {
    Connected(Serial),  // strictly speaking, these two only ever go
    Disconnect(Serial), // inverter->coordinator, but eh.
    Packet(Packet),     // this one goes both ways through the channel.
    Shutdown,
    Heartbeat(Packet),
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
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Serial([u8; 10]);

impl Serial {
    pub fn new(input: &[u8]) -> Result<Self> {
        Ok(Self(input.try_into()?))
    }

    pub fn default() -> Self {
        Self([0; 10])
    }

    pub fn data(&self) -> [u8; 10] {
        self.0
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

impl std::fmt::Display for Serial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", String::from_utf8_lossy(&self.0))
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
}

const READ_TIMEOUT_SECS: u64 = 1; // Multiplier for read_timeout from config
const WRITE_TIMEOUT_SECS: u64 = 5; // Timeout for write operations
const RECONNECT_DELAY_SECS: u64 = 5; // Delay before reconnection attempts
const TCP_KEEPALIVE_SECS: u64 = 60; // TCP keepalive interval

impl Inverter {
    pub fn new(config: ConfigWrapper, inverter: &config::Inverter, channels: Channels) -> Self {
        // remember which inverter this instance is for
        let host = inverter.host().to_string();

        Self {
            config,
            host,
            channels,
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

    pub fn stop(&self) {
        let _ = self.channels.to_inverter.send(ChannelData::Shutdown);
    }

    pub async fn connect(&self) -> Result<()> {
        let inverter_config = self.config();
        info!(
            "connecting to inverter {} at {}:{}",
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
            Ok(Ok(stream)) => stream,
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

        let (reader, writer) = stream.into_split();

        info!("inverter {}: connected!", inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default());

        // Start sender and receiver tasks
        let sender_task = self.sender(writer);
        let receiver_task = self.receiver(reader);

        // Send Connected message after tasks are started
        if let Err(e) = self.channels.from_inverter.send(ChannelData::Connected(inverter_config.datalog().expect("datalog must be set"))) {
            warn!("Failed to send Connected message: {}", e);
        } else {
            info!("{}:sent Connected message", inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default());
        }

        tokio::select! {
            res = sender_task => {
                if let Err(e) = res {
                    warn!("Sender task error: {} for {}", e, inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default());
                } else {
                    warn!("Sender task ended for {}", inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default());
                }
            }
            res = receiver_task => {
                if let Err(e) = res {
                    warn!("Receiver task error: {} for {}", e, inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default());
                } else {
                    warn!("Receiver task ended for {}", inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default());
                }
            }
        }

        // Ensure we send a disconnect message
        let _ = self.channels.from_inverter.send(ChannelData::Disconnect(inverter_config.datalog().expect("datalog must be set")));
        Ok(())
    }

    async fn sender(&self, mut writer: tokio::net::tcp::OwnedWriteHalf) -> Result<()> {
        let mut to_inverter_rx = self.channels.to_inverter.subscribe();
        let inverter_config = self.config();

        loop {
            match to_inverter_rx.recv().await {
                Ok(ChannelData::Shutdown) => {
                    info!("Received shutdown signal for {}", inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default());
                    break;
                }
                Ok(ChannelData::Connected(_)) | Ok(ChannelData::Disconnect(_)) => {
                    // These messages shouldn't be sent to this channel
                    warn!("Unexpected connection status message in sender channel");
                    continue;
                }
                Ok(ChannelData::Packet(packet)) => {
                    if packet.datalog() != inverter_config.datalog().expect("datalog must be set") {
                        warn!(
                            "Datalog mismatch - packet: {}, inverter: {}",
                            packet.datalog(),
                            inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default()
                        );
                        continue;
                    }

                    let bytes = TcpFrameFactory::build(&packet);
                    if bytes.is_empty() {
                        warn!("Generated empty packet data for {:?}", packet);
                        continue;
                    }

                    debug!("inverter {}: TX {:?}", inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default(), bytes);
                    
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
                Ok(ChannelData::Heartbeat(hb)) => {
                    let packet = hb.clone();
                    if let Err(_e) = self.handle_incoming_packet(packet) {
                        warn!("Failed to send heartbeat packet: {}", inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default());
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    bail!("{}:Channel closed", inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default());
                }
                Err(_e) => {
                    warn!("Error reading from channel: {}", inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default());
                    continue;
                }
            }
        }

        info!("inverter {}: sender exiting", inverter_config.datalog().map(|s| s.to_string()).unwrap_or_default());
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
                            info!("Receiver received shutdown signal");
                            break;
                        }
                        Ok(_) => continue, // Ignore other messages
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
                        
                        // Validate and process the packet
                        self.compare_datalog(&packet)?;
                        if let Packet::TranslatedData(_) = packet {
                            self.compare_inverter(&packet)?;
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
}
