pub mod commands;

use crate::prelude::*;
use crate::coordinator::commands::time_register_ops::Action;
use crate::eg4::packet::{Register, RegisterBit};
use crate::command::Command;
use crate::datalog_writer::DatalogWriter;

use crate::eg4::{
    packet::{DeviceFunction, TranslatedData, Packet},
};

use commands::{
    parse_hold,
    parse_input,
};

use std::sync::{Arc, Mutex};
use crate::eg4::inverter;

// Sleep durations - keeping only the ones actively used
const RETRY_DELAY_MS: u64 = 1000;    // 1 second

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum ChannelData {
    Shutdown,
    Packet(crate::eg4::packet::Packet),
    SendPacket(crate::eg4::packet::Packet),
}

pub type InputsStore = std::collections::HashMap<Serial, crate::eg4::packet::ReadInputs>;

#[derive(Debug, Default)]
pub struct PacketStats {
    pub packets_received: u64,
    pub packets_sent: u64,
    // Received packet counters
    pub heartbeat_packets_received: u64,
    pub translated_data_packets_received: u64,
    pub read_param_packets_received: u64,
    pub write_param_packets_received: u64,
    // Sent packet counters
    pub heartbeat_packets_sent: u64,
    pub translated_data_packets_sent: u64,
    pub read_param_packets_sent: u64,
    pub write_param_packets_sent: u64,
    // Error counters
    pub modbus_errors: u64,
    pub mqtt_errors: u64,
    pub influx_errors: u64,
    pub database_errors: u64,
    pub register_cache_errors: u64,
    // Other stats
    pub mqtt_messages_sent: u64,
    pub influx_writes: u64,
    pub database_writes: u64,
    pub register_cache_writes: u64,
    // Connection stats
    pub inverter_disconnections: std::collections::HashMap<Serial, u64>,
    pub serial_mismatches: u64,
    // Last message received per inverter
    pub last_messages: std::collections::HashMap<Serial, String>,
}

impl PacketStats {
    pub fn print_summary(&self) {
        info!("Packet Statistics:");
        info!("  Total packets received: {}", self.packets_received);
        info!("  Total packets sent: {}", self.packets_sent);
        info!("  Received Packet Types:");
        info!("    Heartbeat packets: {}", self.heartbeat_packets_received);
        info!("    TranslatedData packets: {}", self.translated_data_packets_received);
        info!("    ReadParam packets: {}", self.read_param_packets_received);
        info!("    WriteParam packets: {}", self.write_param_packets_received);
        info!("  Sent Packet Types:");
        info!("    Heartbeat packets: {}", self.heartbeat_packets_sent);
        info!("    TranslatedData packets: {}", self.translated_data_packets_sent);
        info!("    ReadParam packets: {}", self.read_param_packets_sent);
        info!("    WriteParam packets: {}", self.write_param_packets_sent);
        info!("  Errors:");
        info!("    Modbus errors: {}", self.modbus_errors);
        info!("    MQTT errors: {}", self.mqtt_errors);
        info!("    InfluxDB errors: {}", self.influx_errors);
        info!("    Database errors: {}", self.database_errors);
        info!("    Register cache errors: {}", self.register_cache_errors);
        info!("  MQTT:");
        info!("    Messages sent: {}", self.mqtt_messages_sent);
        info!("  InfluxDB:");
        info!("    Writes: {}", self.influx_writes);
        info!("  Database:");
        info!("    Writes: {}", self.database_writes);
        info!("  Register Cache:");
        info!("    Writes: {}", self.register_cache_writes);
        info!("  Connection Stats:");
        info!("    Serial number mismatches: {}", self.serial_mismatches);
        info!("    Inverter disconnections by serial:");
        for (serial, count) in &self.inverter_disconnections {
            info!("      {}: {}", serial, count);
            if let Some(last_msg) = self.last_messages.get(serial) {
                info!("      Last message: {}", last_msg);
            }
        }
    }

    pub fn increment_serial_mismatches(&mut self) {
        self.serial_mismatches += 1;
    }

    pub fn increment_mqtt_errors(&mut self) {
        self.mqtt_errors += 1;
    }

    pub fn increment_cache_errors(&mut self) {
        self.register_cache_errors += 1;
    }

    pub fn copy_from(&mut self, other: &PacketStats) {
        self.packets_received = other.packets_received;
        self.packets_sent = other.packets_sent;
        self.heartbeat_packets_received = other.heartbeat_packets_received;
        self.translated_data_packets_received = other.translated_data_packets_received;
        self.read_param_packets_received = other.read_param_packets_received;
        self.write_param_packets_received = other.write_param_packets_received;
        self.heartbeat_packets_sent = other.heartbeat_packets_sent;
        self.translated_data_packets_sent = other.translated_data_packets_sent;
        self.read_param_packets_sent = other.read_param_packets_sent;
        self.write_param_packets_sent = other.write_param_packets_sent;
        self.modbus_errors = other.modbus_errors;
        self.mqtt_errors = other.mqtt_errors;
        self.influx_errors = other.influx_errors;
        self.database_errors = other.database_errors;
        self.register_cache_errors = other.register_cache_errors;
        self.mqtt_messages_sent = other.mqtt_messages_sent;
        self.influx_writes = other.influx_writes;
        self.database_writes = other.database_writes;
        self.register_cache_writes = other.register_cache_writes;
        self.inverter_disconnections = other.inverter_disconnections.clone();
        self.serial_mismatches = other.serial_mismatches;
        self.last_messages = other.last_messages.clone();
    }
}

#[derive(Clone)]
pub struct Coordinator {
    config: Arc<ConfigWrapper>,
    mqtt: Option<Arc<Mqtt>>,
    influx: Option<Arc<Influx>>,
    databases: Vec<Arc<Database>>,
    datalog_writer: Option<Arc<DatalogWriter>>,
    channels: Channels,
    pub shared_stats: Arc<Mutex<PacketStats>>,
}

#[allow(dead_code)]
impl Coordinator {
    pub fn new(config: Arc<ConfigWrapper>, channels: Channels) -> Self {
        let shared_stats = Arc::new(Mutex::new(PacketStats::default()));
        
        // Share stats with inverters
        for inverter_config in config.inverters() {
            if let Some(inverter) = config.enabled_inverter_with_datalog(inverter_config.datalog().unwrap_or_default()) {
                let _inverter = eg4::inverter::Inverter::new_with_stats(
                    (*config).clone(),
                    &inverter,
                    channels.clone(),
                    shared_stats.clone()
                );
            }
        }

        Self {
            config,
            mqtt: None,
            influx: None,
            databases: Vec::new(),
            datalog_writer: None,
            channels,
            shared_stats,
        }
    }

    pub fn stop(&self) {
        info!("Stopping coordinator...");

        // Send shutdown signals through channels first
        let _ = self.channels.to_inverter.send(crate::eg4::inverter::ChannelData::Shutdown);
        let _ = self.channels.to_mqtt.send(mqtt::ChannelData::Shutdown);
        let _ = self.channels.to_influx.send(influx::ChannelData::Shutdown);
        let _ = self.channels.to_database.send(database::ChannelData::Shutdown);
        let _ = self.channels.to_register_cache.send(register_cache::ChannelData::Shutdown);
    }

    pub async fn start(&mut self) -> Result<()> {
        // Start all components
        self.start_mqtt()?;
        self.start_influx()?;
        self.start_databases()?;
        self.start_datalog_writer()?;

        let mut shutdown = false;
        let mut receiver = self.channels.from_inverter.subscribe();
        let mut coordinator_receiver = self.channels.from_coordinator.subscribe();

        loop {
            if shutdown {
                info!("Coordinator shutting down, stopping message processing");
                break;
            }

            tokio::select! {
                msg = receiver.recv() => {
                    match msg {
                        Ok(inverter::ChannelData::Packet(packet)) => {
                            if let Err(e) = self.handle_packet(packet).await {
                                error!("Failed to handle packet: {}", e);
                            }
                        }
                        Ok(inverter::ChannelData::Connected(datalog)) => {
                            info!("Inverter connected: {}", datalog);
                            if let Err(e) = self.inverter_connected(datalog).await {
                                error!("Failed to process inverter connection: {}", e);
                            }
                        }
                        Ok(inverter::ChannelData::Disconnect(serial)) => {
                            warn!("Inverter disconnected: {}", serial);
                            if let Ok(mut stats) = self.shared_stats.lock() {
                                let count = stats.inverter_disconnections
                                    .entry(serial)
                                    .or_insert(0);
                                *count += 1;
                                // Print statistics after disconnection
                                info!("Statistics after inverter disconnection:");
                                stats.print_summary();
                            }
                        }
                        Ok(inverter::ChannelData::Shutdown) => {
                            info!("Coordinator received shutdown signal");
                            shutdown = true;
                        }
                        Ok(inverter::ChannelData::Heartbeat(packet)) => {
                            // Handle heartbeat packets using the same stats tracking as other packets
                            if let Err(e) = self.handle_packet(packet).await {
                                error!("Failed to handle heartbeat packet: {}", e);
                            }
                        }
                        Ok(inverter::ChannelData::ModbusError(inverter, error_code, error)) => {
                            error!("Modbus error from inverter {}: {} (code: {:#04x})", 
                                inverter.datalog().map(|s| s.to_string()).unwrap_or_default(), error.description(), error_code);
                            if let Ok(mut stats) = self.shared_stats.lock() {
                                stats.modbus_errors += 1;
                                // Print statistics after significant error
                                stats.print_summary();
                            }
                        }
                        Ok(inverter::ChannelData::SerialMismatch(inverter, expected, actual)) => {
                            error!("Serial number mismatch for inverter {}: expected {}, got {}", 
                                inverter.datalog().map(|s| s.to_string()).unwrap_or_default(), expected, actual);

                            if let Ok(mut stats) = self.shared_stats.lock() {
                                stats.serial_mismatches += 1;
                                stats.last_messages.insert(
                                    inverter.datalog().unwrap_or_default(),
                                    format!("Serial number mismatch for inverter {}: expected {}, got {}", 
                                        inverter.datalog().map(|s| s.to_string()).unwrap_or_default(), 
                                        inverter.serial().map(|s| s.to_string()).unwrap_or_default(),
                                        actual)
                                );
                            }
                        }
                        Err(e) => {
                            error!("Error receiving from channel: {}", e);
                        }
                    }
                }
                msg = coordinator_receiver.recv() => {
                    match msg {
                        Ok(ChannelData::SendPacket(packet)) => {
                            if let Err(e) = self.send_to_inverter(packet).await {
                                error!("Failed to send packet to inverter: {}", e);
                            }
                        }
                        Ok(ChannelData::Shutdown) => {
                            info!("Coordinator received shutdown signal");
                            shutdown = true;
                        }
                        Ok(ChannelData::Packet(_)) => {
                            // Ignore regular packets in this channel
                        }
                        Err(e) => {
                            error!("Error receiving from coordinator channel: {}", e);
                        }
                    }
                }
            }
        }

        info!("Coordinator shutdown complete");
        Ok(())
    }

    async fn handle_packet(&self, packet: Packet) -> Result<()> {
        // Update shared stats for received packets
        if let Ok(mut stats) = self.shared_stats.lock() {
            stats.packets_received += 1;
            match &packet {
                Packet::TranslatedData(_) => stats.translated_data_packets_received += 1,
                Packet::ReadParam(_rp) => stats.read_param_packets_received += 1,
                Packet::WriteParam(_wp) => stats.write_param_packets_received += 1,
                Packet::Heartbeat(_) => stats.heartbeat_packets_received += 1,
            }
        }

        let datalog = match &packet {
            Packet::TranslatedData(td) => td.datalog,
            Packet::ReadParam(rp) => rp.datalog,
            Packet::WriteParam(wp) => wp.datalog,
            Packet::Heartbeat(hb) => hb.datalog,
        };

        // Log the type of packet received
        match &packet {
            Packet::TranslatedData(td) => {
                info!("Received TranslatedData packet - datalog: {}, inverter: {}, function: {:?}, register: {}", 
                    td.datalog, td.inverter, td.device_function, td.register);
            }
            Packet::ReadParam(rp) => {
                info!("Received ReadParam packet - datalog: {}, register: {}", 
                    rp.datalog, rp.register);
            }
            Packet::WriteParam(wp) => {
                info!("Received WriteParam packet - datalog: {}, register: {}", 
                    wp.datalog, wp.register);
            }
            Packet::Heartbeat(hb) => {
                info!("Received Heartbeat packet - datalog: {}", 
                    hb.datalog);
            }
        }

        let _inverter = match self.config.enabled_inverter_with_datalog(datalog) {
            Some(inverter) => inverter,
            None => {
                warn!("Unknown inverter datalog connected: {}, will continue processing its data", datalog);
                return Ok(());
            }
        };

        // Skip processing if we're shutting down
        if self.is_shutting_down().await {
            return Ok(());
        }

        // Process the packet
        match packet {
            Packet::TranslatedData(td) => {
                // Skip heartbeat packets for InfluxDB
                if !matches!(td.device_function, DeviceFunction::WriteSingle | DeviceFunction::WriteMulti) {
                    // Send to InfluxDB
                    if let Err(e) = self.send_to_influx(&td).await {
                        error!("Failed to send data to InfluxDB: {}", e);
                    }
                }

                // Cache register values
                if let Err(e) = self.cache_register(td.register, td.values.clone()) {
                    error!("Failed to cache register {}: {}", td.register, e);
                }

                // Send to MQTT
                if let Err(e) = self.send_to_mqtt(&td).await {
                    error!("Failed to send data to MQTT: {}", e);
                }
            }
            Packet::ReadParam(rp) => {
                // Cache register values
                if let Err(e) = self.cache_register(rp.register, rp.values.clone()) {
                    error!("Failed to cache register {}: {}", rp.register, e);
                }
            }
            Packet::WriteParam(wp) => {
                // Check if we're in read-only mode
                if let Some(inverter) = self.config.enabled_inverter_with_datalog(wp.datalog) {
                    if self.config.read_only() || inverter.read_only() {
                        error!("Received write parameter packet but inverter is in read-only mode - datalog: {}, register: {}, value: {:?}", 
                            wp.datalog, wp.register, wp.values);
                        return Ok(());
                    }
                }

                // Cache register values
                if let Err(e) = self.cache_register(wp.register, wp.values.clone()) {
                    error!("Failed to cache register {}: {}", wp.register, e);
                }
            }
            Packet::Heartbeat(_) => {
                // Heartbeat packets are handled in the main loop and don't need to be sent to InfluxDB
            }
        }

        Ok(())
    }

    async fn is_shutting_down(&self) -> bool {
        // Check if any of the channels are closed by trying to subscribe
        self.channels.to_influx.subscribe().is_closed() || 
        self.channels.to_mqtt.subscribe().is_closed() || 
        self.channels.to_database.subscribe().is_closed() ||
        self.channels.read_register_cache.subscribe().is_closed()
    }

    async fn mqtt_receiver(&self) -> Result<()> {
        let mut receiver = self.channels.from_mqtt.subscribe();

        while let mqtt::ChannelData::Message(message) = receiver.recv().await? {
            let _ = self.process_message(message).await;
        }

        Ok(())
    }

    async fn process_message(&self, message: mqtt::Message) -> Result<()> {
        // If MQTT is disabled, don't process any messages
        if !self.config.mqtt().enabled() {
            return Ok(());
        }

        for inverter in self.config.inverters_for_message(&message)? {
            match message.to_command(inverter) {
                Ok(command) => {
                    info!("parsed command {:?}", command);
                    let result = self.process_command(command.clone()).await;
                    if result.is_err() {
                    let topic_reply = command.to_result_topic();
                    let reply = mqtt::ChannelData::Message(mqtt::Message {
                        topic: topic_reply,
                        retain: false,
                            payload: "FAIL".to_string(),
                    });
                    if self.channels.to_mqtt.send(reply).is_err() {
                        bail!("send(to_mqtt) failed - channel closed?");
                        }
                    }
                }
                Err(err) => {
                    error!("{:?}", err);
                }
            }
        }

        Ok(())
    }

    /// Process a command received from MQTT or other sources
    /// This function routes commands to appropriate read/write handlers
    async fn process_command(&self, command: Command) -> Result<()> {
        let inverter = match &command {
            Command::ChargeRate(inv, _) |
            Command::DischargeRate(inv, _) |
            Command::AcChargeRate(inv, _) |
            Command::AcChargeSocLimit(inv, _) |
            Command::DischargeCutoffSocLimit(inv, _) |
            Command::SetHold(inv, _, _) |
            Command::WriteParam(inv, _, _) |
            Command::SetAcChargeTime(inv, _, _) |
            Command::SetAcFirstTime(inv, _, _) |
            Command::SetChargePriorityTime(inv, _, _) |
            Command::SetForcedDischargeTime(inv, _, _) |
            Command::ReadInputs(inv, _) |
            Command::ReadInput(inv, _, _) |
            Command::ReadHold(inv, _, _) |
            Command::ReadParam(inv, _) |
            Command::ReadAcChargeTime(inv, _) |
            Command::ReadAcFirstTime(inv, _) |
            Command::ReadChargePriorityTime(inv, _) |
            Command::ReadForcedDischargeTime(inv, _) |
            Command::AcCharge(inv, _) |
            Command::ChargePriority(inv, _) |
            Command::ForcedDischarge(inv, _) => inv.clone(),
        };

        let write_inverter = commands::write_inverter::WriteInverter::new(
            self.channels.clone(),
            inverter.clone(),
            (*self.config).clone(),
        );

        match command {
            // Write operations - these are blocked by read_only mode
            Command::ChargeRate(_, value) => write_inverter.set_charge_rate(value).await,
            Command::DischargeRate(_, value) => write_inverter.set_discharge_rate(value).await,
            Command::AcChargeRate(_, value) => write_inverter.set_ac_charge_rate(value).await,
            Command::AcChargeSocLimit(_, value) => write_inverter.set_ac_charge_soc_limit(value).await,
            Command::DischargeCutoffSocLimit(_, value) => write_inverter.set_discharge_cutoff_soc_limit(value).await,
            Command::SetHold(_, register, value) => write_inverter.set_hold(register, value).await,
            Command::WriteParam(_, register, value) => write_inverter.set_param(register, value).await,
            Command::SetAcChargeTime(_, _, values) => write_inverter.set_ac_charge_time(values).await,
            Command::SetAcFirstTime(_, _, values) => write_inverter.set_ac_first_time(values).await,
            Command::SetChargePriorityTime(_, _, values) => write_inverter.set_charge_priority_time(values).await,
            Command::SetForcedDischargeTime(_, _, values) => write_inverter.set_forced_discharge_time(values).await,
            
            // Read operations - these are always allowed regardless of read_only mode
            Command::ReadInputs(_, block) => self.read_input_block(&inverter, block * 40, inverter.register_block_size()).await,
            Command::ReadInput(_, register, count) => self.read_input_registers(&inverter, register, count).await,
            Command::ReadHold(_, register, count) => self.read_hold_registers(&inverter, register, count).await,
            Command::ReadParam(_, register) => self.read_param_register(&inverter, register).await,
            Command::ReadAcChargeTime(_, num) => self.read_ac_charge_time(&inverter, num).await,
            Command::ReadAcFirstTime(_, num) => self.read_ac_first_time(&inverter, num).await,
            Command::ReadChargePriorityTime(_, num) => self.read_charge_priority_time(&inverter, num).await,
            Command::ReadForcedDischargeTime(_, num) => self.read_forced_discharge_time(&inverter, num).await,
            
            // Enable/Disable operations - these are blocked by read_only mode
            Command::AcCharge(_, enable) => {
                self.update_hold(
                    inverter,
                    Register::Register21,
                    RegisterBit::AcChargeEnable,
                    enable,
                ).await
            },
            Command::ChargePriority(_, enable) => {
                self.update_hold(
                    inverter,
                    Register::Register21,
                    RegisterBit::ChargePriorityEnable,
                    enable,
                ).await
            },
            Command::ForcedDischarge(_, enable) => {
                self.update_hold(
                    inverter,
                    Register::Register21,
                    RegisterBit::ForcedDischargeEnable,
                    enable,
                ).await
            },
        }
    }

    /// Read a block of input registers from the inverter
    /// This operation is always allowed regardless of read_only mode
    async fn read_input_block<U>(
        &self,
        inverter: &config::Inverter,
        register: U,
        count: u16,
    ) -> Result<()>
    where
        U: Into<u16>,
    {
        commands::read_inputs::ReadInputs::new(self.channels.clone(), inverter.clone(), register, count)
        .run()
        .await?;

        // Add delay between reads if configured
        if inverter.delay_ms().unwrap_or(0) > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(inverter.delay_ms().unwrap_or(0))).await;
        }
        Ok(())
    }

    /// Read specific input registers from the inverter
    /// This operation is always allowed regardless of read_only mode
    async fn read_input_registers<U>(&self, inverter: &config::Inverter, register: U, count: u16) -> Result<()>
    where
        U: Into<u16>,
    {
        commands::read_inputs::ReadInputs::new(self.channels.clone(), inverter.clone(), register, count)
        .run()
        .await?;

        // Add delay between reads if configured
        if inverter.delay_ms().unwrap_or(0) > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(inverter.delay_ms().unwrap_or(0))).await;
        }
        Ok(())
    }

    /// Read holding registers from the inverter
    /// This operation is always allowed regardless of read_only mode
    async fn read_hold_registers<U>(&self, inverter: &config::Inverter, register: U, count: u16) -> Result<()>
    where
        U: Into<u16>,
    {
        commands::read_hold::ReadHold::new(self.channels.clone(), inverter.clone(), register, count)
        .run()
        .await?;

        // Add delay between reads if configured
        if inverter.delay_ms().unwrap_or(0) > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(inverter.delay_ms().unwrap_or(0))).await;
        }
        Ok(())
    }

    /// Read a parameter register from the inverter
    /// This operation is always allowed regardless of read_only mode
    async fn read_param_register<U>(&self, inverter: &config::Inverter, register: U) -> Result<()>
    where
        U: Into<u16>,
    {
        commands::read_param::ReadParam::new(self.channels.clone(), inverter.clone(), register)
            .run()
            .await?;

        // Add delay between reads if configured
        if inverter.delay_ms().unwrap_or(0) > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(inverter.delay_ms().unwrap_or(0))).await;
        }
        Ok(())
    }

    /// Read AC charge time settings from the inverter
    /// This operation is always allowed regardless of read_only mode
    async fn read_ac_charge_time(
        &self,
        inverter: &config::Inverter,
        num: u16,
    ) -> Result<()> {
        self.read_time_register(inverter, Action::AcCharge(num)).await
    }

    /// Read AC first time settings from the inverter
    /// This operation is always allowed regardless of read_only mode
    async fn read_ac_first_time(
        &self,
        inverter: &config::Inverter,
        num: u16,
    ) -> Result<()> {
        self.read_time_register(inverter, Action::AcFirst(num)).await
    }

    /// Read charge priority time settings from the inverter
    /// This operation is always allowed regardless of read_only mode
    async fn read_charge_priority_time(
        &self,
        inverter: &config::Inverter,
        num: u16,
    ) -> Result<()> {
        self.read_time_register(inverter, Action::ChargePriority(num)).await
    }

    /// Read forced discharge time settings from the inverter
    /// This operation is always allowed regardless of read_only mode
    async fn read_forced_discharge_time(
        &self,
        inverter: &config::Inverter,
        num: u16,
    ) -> Result<()> {
        self.read_time_register(inverter, Action::ForcedDischarge(num)).await
    }

    /// Internal helper to read time register settings
    /// This operation is always allowed regardless of read_only mode
    async fn read_time_register(
        &self,
        inverter: &config::Inverter,
        action: commands::time_register_ops::Action,
    ) -> Result<()> {
        commands::time_register_ops::ReadTimeRegister::new(
            self.channels.clone(),
            inverter.clone(),
            (*self.config).clone(),
            action,
        )
        .run()
        .await?;
        
        // Add delay between reads if configured
        if inverter.delay_ms().unwrap_or(0) > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(inverter.delay_ms().unwrap_or(0))).await;
        }
        Ok(())
    }

    /// Write a parameter to the inverter
    /// This operation is blocked by read_only mode
    async fn write_param<U>(
        &self,
        inverter: config::Inverter,
        register: U,
        value: u16,
    ) -> Result<()>
    where
        U: Into<u16>,
    {
        commands::write_param::WriteParam::new(
            self.channels.clone(),
            inverter.clone(),
            register,
            value,
        )
        .run()
        .await?;

        Ok(())
    }

    /// Write time register settings to the inverter
    /// This operation is blocked by read_only mode
    async fn set_time_register(
        &self,
        inverter: config::Inverter,
        action: commands::time_register_ops::Action,
        values: [u8; 4],
    ) -> Result<()> {
        commands::time_register_ops::SetTimeRegister::new(
            self.channels.clone(),
            inverter.clone(),
            (*self.config).clone(),
            action,
            values,
        )
        .run()
        .await
    }

    /// Write a holding register to the inverter
    /// This operation is blocked by read_only mode
    async fn set_hold<U>(&self, inverter: config::Inverter, register: U, value: u16) -> Result<()>
    where
        U: Into<u16>,
    {
        commands::set_hold::SetHold::new(self.channels.clone(), inverter.clone(), register, value)
            .run()
            .await?;

        Ok(())
    }

    /// Update a bit in a holding register
    /// This operation is blocked by read_only mode
    async fn update_hold<U>(
        &self,
        inverter: config::Inverter,
        register: U,
        bit: crate::eg4::packet::RegisterBit,
        enable: bool,
    ) -> Result<()>
    where
        U: Into<u16>,
    {
        commands::update_hold::UpdateHold::new(
            self.channels.clone(),
            inverter.clone(),
            register.into(),
            bit,
            enable,
        )
        .run()
        .await?;

        Ok(())
    }

    async fn process_inverter_packet(&self, packet: Packet, inverter: &config::Inverter) -> Result<()> {
        match &packet {
            Packet::TranslatedData(td) => {
                let datalog = td.datalog;
                // Check for Modbus error response
                if td.values.len() >= 1 {
                    let first_byte = td.values[0];
                    if first_byte & 0x80 != 0 {  // Check if MSB is set (error response)
                        let error_code = first_byte & 0x7F;  // Remove MSB to get error code
                        if let Some(error) = crate::eg4::packet::ModbusError::from_code(error_code) {
                            error!("Modbus error from inverter {}: {} (code: {:#04x})", 
                                inverter.datalog().map(|s| s.to_string()).unwrap_or_default(), error.description(), error_code);
                            if let Ok(mut stats) = self.shared_stats.lock() {
                                stats.modbus_errors += 1;
                                // Print statistics after significant error
                                info!("Statistics after Modbus error:");
                                stats.print_summary();
                            }
                            return Ok(());  // Return early as this is an error response
                        }
                    }
                }

                // Log TCP function for debugging
                trace!("Processing TCP function: {:?}", td.tcp_function());

                // Check if serial matches configured inverter
                if let Some(inverter_serial) = inverter.serial() {
                    if td.inverter != inverter_serial {
                        warn!(
                            "Serial mismatch detected - updating inverter configuration. Got {}, was {}",
                            td.inverter,
                            inverter_serial
                        );
                        
                        // Update inverter configuration with new serial
                        info!("Updating inverter serial from {} to {}", inverter_serial, td.inverter);
                        if let Err(e) = self.config.update_inverter_serial(inverter_serial, td.inverter) {
                            error!("Failed to update inverter serial: {}", e);
                        }

                        if let Ok(mut stats) = self.shared_stats.lock() {
                            stats.serial_mismatches += 1;
                            stats.last_messages.insert(
                                inverter.datalog().unwrap_or_default(),
                                format!("Serial number mismatch for inverter {}: expected {}, got {}", 
                                    inverter.datalog().map(|s| s.to_string()).unwrap_or_default(), 
                                    inverter.serial().map(|s| s.to_string()).unwrap_or_default(),
                                    td.inverter)
                            );
                        }
                    }
                }

                if let Some(datalog) = inverter.datalog() {
                    if td.datalog != datalog {
                        warn!(
                            "Datalog mismatch - packet: {}, inverter: {}",
                            td.datalog,
                            datalog
                        );
                        info!("Updating inverter datalog from {} to {}", datalog, td.datalog);
                        if let Err(e) = self.config.update_inverter_datalog(datalog, td.datalog) {
                            error!("Failed to update datalog: {}", e);
                        }
                        info!(
                            "{}",
                            format!("Datalog updated - was {}, now {}", datalog, td.datalog)
                        );
                    }
                }

                // Update packet stats
                if let Ok(mut stats) = self.shared_stats.lock() {
                    stats.packets_received += 1;
                    let packet_clone = packet.clone();
                    stats.last_messages.insert(datalog, format!("{:?}", packet_clone));
                    stats.translated_data_packets_received += 1;
                }

                // Process the packet based on its type
                match td.device_function {
                    DeviceFunction::ReadInput => {
                        debug!("Processing ReadInput packet");
                        let register = td.register();
                        let pairs = td.pairs();
                        
                        // Log all register values
                        debug!("Input Register Values:");
                        for (reg, value) in &pairs {
                            // Cache the register value
                            if let Err(e) = self.channels.to_register_cache.send(register_cache::ChannelData::RegisterData(*reg, *value)) {
                                error!("Failed to cache register {}: {}", reg, e);
                                if let Ok(mut stats) = self.shared_stats.lock() {
                                    stats.register_cache_errors += 1;
                                }
                            }
                            
                            // Parse and log the register value using the new module
                            let schema = self.config.register_schema();
                            let result = parse_input::parse_input_register(*reg, (*value).into(), &schema);
                            debug!("  {}", result);
                        }

                        // Write to datalog file if enabled
                        if let Some(writer) = &self.datalog_writer {
                            if let Err(e) = writer.write_input_data(td.inverter, td.datalog, &pairs) {
                                error!("Failed to write to datalog file: {}", e);
                            }
                        }

                        // Send to InfluxDB if enabled
                        if self.config.influx().enabled() {
                            let mut data = serde_json::json!({
                                "time": chrono::Utc::now().timestamp(),
                                "serial": td.inverter.to_string(),
                                "datalog": td.datalog.to_string(),
                                "raw_data": {}
                            });

                            // Add raw register data
                            for (reg, value) in &pairs {
                                data["raw_data"][reg.to_string()] = serde_json::json!(format!("{:04x}", value));
                            }

                            if let Err(e) = self.send_to_influx(td).await {
                                error!("Failed to send data to InfluxDB: {}", e);
                                if let Ok(mut stats) = self.shared_stats.lock() {
                                    stats.influx_errors += 1;
                                }
                            }
                        }

                        if let Err(e) = self.publish_input_message(register, pairs, inverter).await {
                            error!("Failed to publish input message: {}", e);
                            if let Ok(mut stats) = self.shared_stats.lock() {
                                stats.mqtt_errors += 1;
                            }
                        }
                    }
                    DeviceFunction::ReadHold => {
                        debug!("Processing ReadHold packet");
                        let register = td.register();
                        let pairs = td.pairs();
                        
                        // Log all register values
                        debug!("Hold Register Values:");
                        for (reg, value) in &pairs {
                            // Cache the register value
                            if let Err(e) = self.channels.to_register_cache.send(register_cache::ChannelData::RegisterData(*reg, *value)) {
                                error!("Failed to cache register {}: {}", reg, e);
                                if let Ok(mut stats) = self.shared_stats.lock() {
                                    stats.register_cache_errors += 1;
                                }
                            }
                            
                            // Parse and log the register value using the new module
                            let schema = self.config.register_schema();
                            let result = parse_hold::parse_hold_register(*reg, (*value).into(), &schema);
                            debug!("  {}", result);
                        }

                        // Write to datalog file if enabled
                        if let Some(writer) = &self.datalog_writer {
                            if let Err(e) = writer.write_hold_data(td.inverter, td.datalog, &pairs) {
                                error!("Failed to write to datalog file: {}", e);
                            }
                        }

                        // Send to InfluxDB if enabled
                        if self.config.influx().enabled() {
                            let mut data = serde_json::json!({
                                "time": chrono::Utc::now().timestamp(),
                                "serial": td.inverter.to_string(),
                                "datalog": td.datalog.to_string(),
                                "raw_data": {}
                            });

                            // Add raw register data
                            for (reg, value) in &pairs {
                                data["raw_data"][reg.to_string()] = serde_json::json!(format!("{:04x}", value));
                            }

                            if let Err(e) = self.send_to_influx(td).await {
                                error!("Failed to send data to InfluxDB: {}", e);
                                if let Ok(mut stats) = self.shared_stats.lock() {
                                    stats.influx_errors += 1;
                                }
                            }
                        }
                        
                        if let Err(e) = self.publish_hold_message(register, pairs, inverter).await {
                            error!("Failed to publish hold message: {}", e);
                            if let Ok(mut stats) = self.shared_stats.lock() {
                                stats.mqtt_errors += 1;
                            }
                        }
                    }
                    DeviceFunction::WriteSingle => {
                        debug!("Processing WriteSingle packet");
                        let register = td.register();
                        let value = td.value();
                        if let Err(e) = self.channels.to_register_cache.send(register_cache::ChannelData::RegisterData(register, value)) {
                            error!("Failed to cache register {}: {}", register, e);
                            if let Ok(mut stats) = self.shared_stats.lock() {
                                stats.register_cache_errors += 1;
                            }
                        }
                        if let Err(e) = self.publish_write_confirmation(register, value, inverter).await {
                            error!("Failed to publish write confirmation: {}", e);
                            if let Ok(mut stats) = self.shared_stats.lock() {
                                stats.mqtt_errors += 1;
                            }
                        }
                    }
                    DeviceFunction::WriteMulti => {
                        debug!("Processing WriteMulti packet");
                        let pairs = td.pairs();
                        for (register, value) in &pairs {
                            if let Err(e) = self.channels.to_register_cache.send(register_cache::ChannelData::RegisterData(*register, *value)) {
                                error!("Failed to cache register {}: {}", register, e);
                                if let Ok(mut stats) = self.shared_stats.lock() {
                                    stats.register_cache_errors += 1;
                                }
                            }
                        }
                        if let Err(e) = self.publish_write_multi_confirmation(pairs, inverter).await {
                            error!("Failed to publish write multi confirmation: {}", e);
                            if let Ok(mut stats) = self.shared_stats.lock() {
                                stats.mqtt_errors += 1;
                            }
                        }
                    }
                }
            }
            Packet::Heartbeat(_) => {
                if let Ok(mut stats) = self.shared_stats.lock() {
                    stats.heartbeat_packets_received += 1;
                }
            }
            Packet::ReadParam(_) => {
                if let Ok(mut stats) = self.shared_stats.lock() {
                    stats.read_param_packets_received += 1;
                }
            }
            Packet::WriteParam(_) => {
                if let Ok(mut stats) = self.shared_stats.lock() {
                    stats.write_param_packets_received += 1;
                }
            }
        }

        Ok(())
    }

    async fn inverter_connected(&self, datalog: Serial) -> Result<()> {
        let inverter = match self.config.enabled_inverter_with_datalog(datalog) {
            Some(inverter) => inverter,
            None => {
                warn!("Unknown inverter datalog connected: {}, will continue processing its data", datalog);
                return Ok(());
            }
        };

        if !inverter.publish_holdings_on_connect() {
            return Ok(());
        }

        info!("Reading all registers for inverter {}", datalog);

        let block_size = inverter.register_block_size();

        // Read all holding register blocks
        for start_register in (0..=240).step_by(block_size as usize) {
            self.read_hold_registers(&inverter, start_register as u16, block_size).await?;
        }

        // Read all input register blocks
        for start_register in (0..=200).step_by(block_size as usize) {
            self.read_input_block(&inverter, start_register as u16, block_size).await?;
        }

        // Read time registers
        for num in &[1, 2, 3] {
            self.read_time_register(
                &inverter,
                commands::time_register_ops::Action::AcCharge(*num),
            ).await?;

            self.read_time_register(
                &inverter,
                commands::time_register_ops::Action::ChargePriority(*num),
            ).await?;

            self.read_time_register(
                &inverter,
                commands::time_register_ops::Action::ForcedDischarge(*num),
            ).await?;

            self.read_time_register(
                &inverter,
                commands::time_register_ops::Action::AcFirst(*num),
            ).await?;
        }

        Ok(())
    }

    async fn publish_message(&self, topic: String, payload: String, retain: bool) -> Result<()> {
        let m = mqtt::Message {
            topic,
            payload,
            retain,
        };
        let channel_data = mqtt::ChannelData::Message(m);
        
        // Try sending with retries
        let mut retries = 3;
        while retries > 0 {
            match self.channels.to_mqtt.send(channel_data.clone()) {
                Ok(_) => {
                    if let Ok(mut stats) = self.shared_stats.lock() {
                        stats.mqtt_messages_sent += 1;
                    }
                    return Ok(());
                }
                Err(e) => {
                    if retries > 1 {
                        warn!("Failed to send MQTT message, retrying... ({} attempts left): {}", retries - 1, e);
                        tokio::time::sleep(std::time::Duration::from_millis(RETRY_DELAY_MS)).await;
                    }
                    retries -= 1;
                }
            }
        }

        if let Ok(mut stats) = self.shared_stats.lock() {
            stats.mqtt_errors += 1;
        }
        bail!("send(to_mqtt) failed after retries - channel closed?");
    }

    async fn publish_input_message(&self, _register: u16, pairs: Vec<(u16, u16)>, inverter: &config::Inverter) -> Result<()> {
        if !self.config.mqtt().enabled() {
            return Ok(());
        }

        // Publish raw values
        for (reg, value) in pairs {
            let topic = format!("{}/inputs/{}", inverter.datalog().map(|s| s.to_string()).unwrap_or_default(), reg);
            if let Err(e) = self.publish_message(topic, value.to_string(), false).await {
                error!("Failed to publish input message: {}", e);
                if let Ok(mut stats) = self.shared_stats.lock() {
                    stats.mqtt_errors += 1;
                }
            }
        }

        Ok(())
    }

    async fn publish_hold_message(&self, _register: u16, pairs: Vec<(u16, u16)>, inverter: &config::Inverter) -> Result<()> {
        if !self.config.mqtt().enabled() {
            return Ok(());
        }

        // Publish raw values
        for (reg, value) in pairs {
            let topic = format!("{}/hold/{}", inverter.datalog().map(|s| s.to_string()).unwrap_or_default(), reg);
            if let Err(e) = self.publish_message(topic, value.to_string(), true).await {
                error!("Failed to publish hold message: {}", e);
                if let Ok(mut stats) = self.shared_stats.lock() {
                    stats.mqtt_errors += 1;
                }
            }
        }

        Ok(())
    }

    async fn publish_write_confirmation(&self, register: u16, value: u16, inverter: &config::Inverter) -> Result<()> {
        if !self.config.mqtt().enabled() {
            return Ok(());
        }

        let topic = format!("{}/write/status", inverter.datalog().map(|s| s.to_string()).unwrap_or_default());
        if let Err(e) = self.publish_message(topic, format!("OK: {} = {}", register, value), false).await {
            error!("Failed to publish write confirmation: {}", e);
            if let Ok(mut stats) = self.shared_stats.lock() {
                stats.mqtt_errors += 1;
            }
        }

        Ok(())
    }

    async fn publish_write_multi_confirmation(&self, pairs: Vec<(u16, u16)>, inverter: &config::Inverter) -> Result<()> {
        if !self.config.mqtt().enabled() {
            return Ok(());
        }

        let topic = format!("{}/write_multi/status", inverter.datalog().map(|s| s.to_string()).unwrap_or_default());
        if let Err(e) = self.publish_message(topic, format!("OK: {:?}", pairs), false).await {
            error!("Failed to publish write multi confirmation: {}", e);
            if let Ok(mut stats) = self.shared_stats.lock() {
                stats.mqtt_errors += 1;
            }
        }

        Ok(())
    }

    fn start_mqtt(&mut self) -> Result<()> {
        if self.config.mqtt().enabled() {
            info!("Initializing MQTT");
            let mqtt = Mqtt::new((*self.config).clone(), self.channels.clone(), self.shared_stats.clone());
            self.mqtt = Some(Arc::new(mqtt));
        }
        Ok(())
    }

    fn start_influx(&mut self) -> Result<()> {
        if self.config.influx().enabled() {
            info!("Initializing InfluxDB");
            let influx = Influx::new((*self.config).clone(), self.channels.clone(), self.shared_stats.clone());
            self.influx = Some(Arc::new(influx));
        }
        Ok(())
    }

    fn start_databases(&mut self) -> Result<()> {
        for db in &self.config.databases() {
            if db.enabled() {
                info!("Initializing database {}", db.url());
                let database = Database::new(db.clone(), self.channels.clone(), self.shared_stats.clone());
                self.databases.push(Arc::new(database));
            }
        }
        Ok(())
    }

    fn start_datalog_writer(&mut self) -> Result<()> {
        if let Some(path) = self.config.datalog_file() {
            info!("Initializing datalog writer");
            let writer = DatalogWriter::new(&path)?;
            self.datalog_writer = Some(Arc::new(writer));
        }
        Ok(())
    }

    fn cache_register(&self, register: u16, values: Vec<u8>) -> Result<()> {
        for (i, value) in values.iter().enumerate() {
            match self.channels.to_register_cache.send(register_cache::ChannelData::RegisterData(register + i as u16, *value as u16)) {
                Ok(_) => {
                    // Increment stats after successful cache write
                    if let Ok(mut stats) = self.shared_stats.lock() {
                        stats.register_cache_writes += 1;
                        trace!("Incremented register cache writes counter to {}", stats.register_cache_writes);
                    }
                }
                Err(e) => {
                    error!("Failed to cache register {}: {}", register + i as u16, e);
                    if let Ok(mut stats) = self.shared_stats.lock() {
                        stats.register_cache_errors += 1;
                    }
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    async fn send_to_influx(&self, td: &TranslatedData) -> Result<()> {
        if !self.config.influx().enabled() {
            return Ok(());
        }

        let mut data = serde_json::json!({
            "time": chrono::Utc::now().timestamp(),
            "serial": td.inverter.to_string(),
            "datalog": td.datalog.to_string(),
            "raw_data": {}
        });

        // Add raw register data
        for (i, value) in td.values.iter().enumerate() {
            data["raw_data"][(td.register + i as u16).to_string()] = serde_json::json!(format!("{:04x}", value));
        }

        // Send data to InfluxDB
        match self.channels.to_influx.send(influx::ChannelData::InputData(data)) {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("Failed to send data to InfluxDB: {}", e);
                if let Ok(mut stats) = self.shared_stats.lock() {
                    stats.influx_errors += 1;
                }
                Err(e.into())
            }
        }
    }

    async fn send_to_mqtt(&self, td: &TranslatedData) -> Result<()> {
        if !self.config.mqtt().enabled() {
            return Ok(());
        }

        // Publish raw values
        for (i, value) in td.values.iter().enumerate() {
            let topic = format!("{}/inputs/{}", td.datalog, td.register + i as u16);
            if let Err(e) = self.publish_message(topic, value.to_string(), false).await {
                error!("Failed to publish input message: {}", e);
                if let Ok(mut stats) = self.shared_stats.lock() {
                    stats.mqtt_errors += 1;
                }
                return Err(e);
            }
        }

        Ok(())
    }

    fn increment_packets_sent(&self, packet: &Packet) {
        if let Ok(mut stats) = self.shared_stats.lock() {
            stats.packets_sent += 1;
            trace!("Incremented total packets sent to {}", stats.packets_sent);

            match packet {
                Packet::TranslatedData(_) => stats.translated_data_packets_sent += 1,
                Packet::ReadParam(_) => stats.read_param_packets_sent += 1,
                Packet::WriteParam(_) => stats.write_param_packets_sent += 1,
                Packet::Heartbeat(_) => stats.heartbeat_packets_sent += 1,
            }
        }
    }

    async fn send_to_inverter(&self, packet: Packet) -> Result<()> {
        // Send packet to inverter
        if let Err(e) = self.channels.to_inverter.send(eg4::inverter::ChannelData::Packet(packet)) {
            bail!("Failed to send packet to inverter: {}", e);
        }
        Ok(())
    }
}

