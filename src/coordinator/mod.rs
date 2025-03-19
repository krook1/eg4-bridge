pub mod commands;

use crate::prelude::*;
use crate::coordinator::commands::time_register_ops::Action;
use crate::lxp::packet::{Register, RegisterBit};
use crate::command::Command;

use lxp::{
    packet::{DeviceFunction, TranslatedData, Packet},
};

use commands::{
    parse_hold,
    parse_input,
};

use std::sync::{Arc, Mutex};
use lxp::inverter;

// Sleep durations - keeping only the ones actively used
const RETRY_DELAY_MS: u64 = 1000;    // 1 second

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum ChannelData {
    Shutdown,
    Packet(lxp::packet::Packet),
}

pub type InputsStore = std::collections::HashMap<Serial, lxp::packet::ReadInputs>;

#[derive(Default)]
pub struct PacketStats {
    packets_received: u64,
    packets_sent: u64,
    // Received packet counters
    heartbeat_packets_received: u64,
    translated_data_packets_received: u64,
    read_param_packets_received: u64,
    write_param_packets_received: u64,
    // Sent packet counters
    heartbeat_packets_sent: u64,
    translated_data_packets_sent: u64,
    read_param_packets_sent: u64,
    write_param_packets_sent: u64,
    // Error counters
    modbus_errors: u64,
    mqtt_errors: u64,
    influx_errors: u64,
    database_errors: u64,
    register_cache_errors: u64,
    // Other stats
    mqtt_messages_sent: u64,
    influx_writes: u64,
    database_writes: u64,
    register_cache_writes: u64,
    // Connection stats
    inverter_disconnections: std::collections::HashMap<Serial, u64>,
    serial_mismatches: u64,
    // Last message received per inverter
    last_messages: std::collections::HashMap<Serial, String>,
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
}

#[derive(Clone)]
pub struct Coordinator {
    config: ConfigWrapper,
    channels: Channels,
    pub stats: Arc<Mutex<PacketStats>>,
}

impl Coordinator {
    pub fn new(config: ConfigWrapper, channels: Channels) -> Self {
        Self { 
            config, 
            channels,
            stats: Arc::new(Mutex::new(PacketStats::default())),
        }
    }

    pub async fn start(&self) -> Result<()> {
        if self.config.mqtt().enabled() {
            tokio::select! {
                res = self.inverter_receiver() => {
                    if let Err(e) = res {
                        error!("Inverter receiver error: {}", e);
                    }
                }
                res = self.mqtt_receiver() => {
                    if let Err(e) = res {
                        error!("MQTT receiver error: {}", e);
                    }
                }
            }
        } else {
            if let Err(e) = self.inverter_receiver().await {
                error!("Inverter receiver error: {}", e);
            }
        }

        Ok(())
    }

    pub fn stop(&self) {
        info!("Stopping coordinator...");
        let _ = self.channels.to_inverter.send(lxp::inverter::ChannelData::Shutdown);
        let _ = self.channels.to_mqtt.send(mqtt::ChannelData::Shutdown);
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

    fn increment_packets_sent(&self, packet: &Packet) {
        if let Ok(mut stats) = self.stats.lock() {
            stats.packets_sent += 1;
            
            // Increment counter for specific sent packet type
            match packet {
                Packet::Heartbeat(_) => stats.heartbeat_packets_sent += 1,
                Packet::TranslatedData(_) => stats.translated_data_packets_sent += 1,
                Packet::ReadParam(_) => stats.read_param_packets_sent += 1,
                Packet::WriteParam(_) => stats.write_param_packets_sent += 1,
            }
        }
    }

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
            Command::ForcedDischarge(inv, _) => inv
        };

        let write_inverter = commands::write_inverter::WriteInverter::new(self.channels.clone(), inverter.clone());

        match command {
            Command::ChargeRate(_, value) => {
                if self.config.read_only() {
                    warn!("Read-only mode enabled, ignoring charge rate command");
                    return Ok(());
                }
                write_inverter.set_charge_rate(value).await?;
            }
            Command::DischargeRate(_, value) => {
                if self.config.read_only() {
                    warn!("Read-only mode enabled, ignoring discharge rate command");
                    return Ok(());
                }
                write_inverter.set_discharge_rate(value).await?;
            }
            Command::AcChargeRate(_, value) => {
                if self.config.read_only() {
                    warn!("Read-only mode enabled, ignoring AC charge rate command");
                    return Ok(());
                }
                write_inverter.set_ac_charge_rate(value).await?;
            }
            Command::AcChargeSocLimit(_, value) => {
                if self.config.read_only() {
                    warn!("Read-only mode enabled, ignoring AC charge SOC limit command");
                    return Ok(());
                }
                write_inverter.set_ac_charge_soc_limit(value).await?;
            }
            Command::DischargeCutoffSocLimit(_, value) => {
                if self.config.read_only() {
                    warn!("Read-only mode enabled, ignoring discharge cutoff SOC limit command");
                    return Ok(());
                }
                write_inverter.set_discharge_cutoff_soc_limit(value).await?;
            }
            Command::SetHold(_, register, value) => {
                if self.config.read_only() {
                    warn!("Read-only mode enabled, ignoring set hold command");
                    return Ok(());
                }
                write_inverter.set_hold(register, value).await?;
            }
            Command::WriteParam(_, register, value) => {
                if self.config.read_only() {
                    warn!("Read-only mode enabled, ignoring write param command");
                    return Ok(());
                }
                write_inverter.set_param(register, value).await?;
            }
            Command::SetAcChargeTime(_, num, values) => {
                if self.config.read_only() {
                    warn!("Read-only mode enabled, ignoring set AC charge time command");
                    return Ok(());
                }
                write_inverter.set_ac_charge_time(self.config.clone(), values).await?;
            }
            Command::SetAcFirstTime(_, num, values) => {
                if self.config.read_only() {
                    warn!("Read-only mode enabled, ignoring set AC first time command");
                    return Ok(());
                }
                write_inverter.set_ac_first_time(self.config.clone(), values).await?;
            }
            Command::SetChargePriorityTime(_, num, values) => {
                if self.config.read_only() {
                    warn!("Read-only mode enabled, ignoring set charge priority time command");
                    return Ok(());
                }
                write_inverter.set_charge_priority_time(self.config.clone(), values).await?;
            }
            Command::SetForcedDischargeTime(_, num, values) => {
                if self.config.read_only() {
                    warn!("Read-only mode enabled, ignoring set forced discharge time command");
                    return Ok(());
                }
                write_inverter.set_forced_discharge_time(self.config.clone(), values).await?;
            }
            Command::ReadInputs(_, 1) => {
                self.read_inputs(inverter.clone(), 0_u16, inverter.register_block_size()).await?
            },
            Command::ReadInputs(_, 2) => self.read_inputs(inverter.clone(), 40_u16, inverter.register_block_size()).await?,
            Command::ReadInputs(_, 3) => self.read_inputs(inverter.clone(), 80_u16, inverter.register_block_size()).await?,
            Command::ReadInputs(_, 4) => self.read_inputs(inverter.clone(), 120_u16, inverter.register_block_size()).await?,
            Command::ReadInputs(_, 5) => self.read_inputs(inverter.clone(), 160_u16, inverter.register_block_size()).await?,
            Command::ReadInputs(_, 6) => self.read_inputs(inverter.clone(), 200_u16, inverter.register_block_size()).await?,
            Command::ReadInputs(_, n) => bail!("Invalid input register block number: {}", n),
            Command::ReadInput(_, register, count) => {
                self.read_inputs(inverter.clone(), register, count).await?
            },
            Command::ReadHold(_, register, count) => {
                self.read_hold(inverter.clone(), register, count).await?
            },
            Command::ReadParam(_, register) => {
                self.read_param(inverter.clone(), register).await?
            },
            Command::ReadAcChargeTime(_, num) => {
                self.read_time_register(inverter.clone(), Action::AcCharge(num))
                    .await?
            },
            Command::ReadAcFirstTime(_, num) => {
                self.read_time_register(inverter.clone(), Action::AcFirst(num))
                    .await?
            },
            Command::ReadChargePriorityTime(_, num) => {
                self.read_time_register(inverter.clone(), Action::ChargePriority(num))
                    .await?
            },
            Command::ReadForcedDischargeTime(_, num) => {
                self.read_time_register(inverter.clone(), Action::ForcedDischarge(num))
                    .await?
            },
            Command::AcCharge(_, enable) => {
                self.update_hold(
                    inverter.clone(),
                    Register::Register21,
                    RegisterBit::AcChargeEnable,
                    enable,
                )
                .await?
            },
            Command::ChargePriority(_, enable) => {
                self.update_hold(
                    inverter.clone(),
                    Register::Register21,
                    RegisterBit::ChargePriorityEnable,
                    enable,
                )
                .await?
            },
            Command::ForcedDischarge(_, enable) => {
                self.update_hold(
                    inverter.clone(),
                    Register::Register21,
                    RegisterBit::ForcedDischargeEnable,
                    enable,
                )
                .await?
            },
        }
        Ok(())
    }

    async fn read_inputs<U>(
        &self,
        inverter: config::Inverter,
        register: U,
        count: u16,
    ) -> Result<()>
    where
        U: Into<u16>,
    {
        commands::read_inputs::ReadInputs::new(self.channels.clone(), inverter.clone(), register, count)
        .run()
        .await?;

        // Add delay after read operation
        tokio::time::sleep(std::time::Duration::from_millis(inverter.delay_ms())).await;
        Ok(())
    }

    async fn read_hold<U>(&self, inverter: config::Inverter, register: U, count: u16) -> Result<()>
    where
        U: Into<u16>,
    {
        commands::read_hold::ReadHold::new(self.channels.clone(), inverter.clone(), register, count)
        .run()
        .await?;

        // Add delay after read operation
        tokio::time::sleep(std::time::Duration::from_millis(inverter.delay_ms())).await;
        Ok(())
    }

    async fn read_param<U>(&self, inverter: config::Inverter, register: U) -> Result<()>
    where
        U: Into<u16>,
    {
        commands::read_param::ReadParam::new(self.channels.clone(), inverter.clone(), register)
            .run()
            .await?;

        // Add delay after read operation
        tokio::time::sleep(std::time::Duration::from_millis(inverter.delay_ms())).await;
        Ok(())
    }

    async fn read_time_register(
        &self,
        inverter: config::Inverter,
        action: commands::time_register_ops::Action,
    ) -> Result<()> {
        commands::time_register_ops::ReadTimeRegister::new(
            self.channels.clone(),
            inverter.clone(),
            self.config.clone(),
            action,
        )
        .run()
        .await?;
        
        // Add delay after read operation
        tokio::time::sleep(std::time::Duration::from_millis(inverter.delay_ms())).await;
        Ok(())
    }

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

    async fn set_time_register(
        &self,
        inverter: config::Inverter,
        action: commands::time_register_ops::Action,
        values: [u8; 4],
    ) -> Result<()> {
        commands::time_register_ops::SetTimeRegister::new(
            self.channels.clone(),
            inverter.clone(),
            self.config.clone(),
            action,
            values,
        )
        .run()
        .await
    }

    async fn set_hold<U>(&self, inverter: config::Inverter, register: U, value: u16) -> Result<()>
    where
        U: Into<u16>,
    {
        commands::set_hold::SetHold::new(self.channels.clone(), inverter.clone(), register, value)
            .run()
            .await?;

        Ok(())
    }

    async fn update_hold<U>(
        &self,
        inverter: config::Inverter,
        register: U,
        bit: lxp::packet::RegisterBit,
        enable: bool,
    ) -> Result<()>
    where
        U: Into<u16>,
    {
        commands::update_hold::UpdateHold::new(
            self.channels.clone(),
            inverter.clone(),
            register,
            bit,
            enable,
        )
        .run()
        .await?;

        Ok(())
    }

    async fn process_inverter_packet(&self, packet: Packet, inverter: &config::Inverter) -> Result<()> {
        if let Packet::TranslatedData(td) = packet {
            // Check for Modbus error response
            if td.values.len() >= 1 {
                let first_byte = td.values[0];
                if first_byte & 0x80 != 0 {  // Check if MSB is set (error response)
                    let error_code = first_byte & 0x7F;  // Remove MSB to get error code
                    if let Some(error) = lxp::packet::ModbusError::from_code(error_code) {
                        error!("Modbus error from inverter {}: {} (code: {:#04x})", 
                            inverter.datalog(), error.description(), error_code);
                        if let Ok(mut stats) = self.stats.lock() {
                            stats.modbus_errors += 1;
                        }
                        return Ok(());  // Return early as this is an error response
                    }
                }
            }

            // Validate serial number format
            if let Some(serial) = td.inverter() {
                if !serial.to_string().chars().all(|c| c.is_ascii_alphanumeric()) {
                    warn!("Invalid serial number format: {}", serial);
                    if let Ok(mut stats) = self.stats.lock() {
                        stats.serial_mismatches += 1;
                    }
                    return Ok(());
                }
            }

            // Log TCP function for debugging
            debug!("Processing TCP function: {:?}", td.tcp_function());

            // Check if serial matches configured inverter
            if td.inverter() != Some(inverter.serial) {
                warn!(
                    "Serial mismatch - got {:?}, expected {}",
                    td.inverter(),
                    inverter.serial
                );
                if let Ok(mut stats) = self.stats.lock() {
                    stats.serial_mismatches += 1;
                    
                    // Track disconnection for this serial
                    let count = stats.inverter_disconnections
                        .entry(inverter.serial)
                        .or_insert(0);
                    *count += 1;
                    
                    // Store last message for debugging
                    stats.last_messages.insert(
                        inverter.serial,
                        format!("Serial mismatch - got {:?}, expected {}", td.inverter(), inverter.serial)
                    );
                }

                // Try to recover by requesting a new connection
                if let Err(e) = self.channels.from_inverter.send(inverter::ChannelData::Disconnect(inverter.serial)) {
                    error!("Failed to request disconnect after serial mismatch: {}", e);
                }
                
                return Ok(());
            }

            match td.device_function {
                DeviceFunction::ReadInput => {
                    debug!("Processing ReadInput packet");
                    let register = td.register();
                    let pairs = td.pairs();
                    
                    // Log all register values
                    info!("Input Register Values:");
                    for (reg, value) in &pairs {
                        // Cache the register value
                        if let Err(e) = self.channels.to_register_cache.send(register_cache::ChannelData::RegisterData(*reg, *value)) {
                            error!("Failed to cache register {}: {}", reg, e);
                            if let Ok(mut stats) = self.stats.lock() {
                                stats.register_cache_errors += 1;
                            }
                        }
                        
                        // Parse and log the register value using the new module
                        info!("  {}", parse_input::parse_input_register(*reg, (*value).into()));
                    }
                    
                    if let Err(e) = self.publish_input_message(register, pairs, inverter).await {
                        error!("Failed to publish input message: {}", e);
                        if let Ok(mut stats) = self.stats.lock() {
                            stats.mqtt_errors += 1;
                        }
                    }
                }
                DeviceFunction::ReadHold => {
                    debug!("Processing ReadHold packet");
                    let register = td.register();
                    let pairs = td.pairs();
                    
                    // Log all register values
                    info!("Hold Register Values:");
                    for (reg, value) in &pairs {
                        // Cache the register value
                        if let Err(e) = self.channels.to_register_cache.send(register_cache::ChannelData::RegisterData(*reg, *value)) {
                            error!("Failed to cache register {}: {}", reg, e);
                            if let Ok(mut stats) = self.stats.lock() {
                                stats.register_cache_errors += 1;
                            }
                        }
                        
                        // Parse and log the register value using the new module
                        info!("  {}", parse_hold::parse_hold_register(*reg, *value));
                    }
                    
                    if let Err(e) = self.publish_hold_message(register, pairs, inverter).await {
                        error!("Failed to publish hold message: {}", e);
                        if let Ok(mut stats) = self.stats.lock() {
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
                        if let Ok(mut stats) = self.stats.lock() {
                            stats.register_cache_errors += 1;
                        }
                    }
                    if let Err(e) = self.publish_write_confirmation(register, value, inverter).await {
                        error!("Failed to publish write confirmation: {}", e);
                        if let Ok(mut stats) = self.stats.lock() {
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
                            if let Ok(mut stats) = self.stats.lock() {
                                stats.register_cache_errors += 1;
                            }
                        }
                    }
                    if let Err(e) = self.publish_write_multi_confirmation(pairs, inverter).await {
                        error!("Failed to publish write multi confirmation: {}", e);
                        if let Ok(mut stats) = self.stats.lock() {
                            stats.mqtt_errors += 1;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn inverter_receiver(&self) -> Result<()> {
        let mut receiver = self.channels.from_inverter.subscribe();
        let mut buffer_size = 0;
        const BUFFER_CLEAR_THRESHOLD: usize = 1024; // 1KB threshold for buffer clearing

        while let Ok(message) = receiver.recv().await {
            match message {
                inverter::ChannelData::Packet(packet) => {
                    // Track buffer size
                    buffer_size += std::mem::size_of_val(&packet);
                    
                    // Clear buffer if threshold exceeded
                    if buffer_size >= BUFFER_CLEAR_THRESHOLD {
                        debug!("Clearing receiver buffer (size: {} bytes)", buffer_size);
                        while let Ok(inverter::ChannelData::Packet(_)) = receiver.try_recv() {
                            // Drain excess messages
                        }
                        buffer_size = 0;
                    }

                    // Process packet based on type first to avoid unnecessary cloning
                    if let Packet::TranslatedData(ref td) = packet {
                        // Find the inverter for this packet
                        if let Some(inverter) = self.config.enabled_inverter_with_datalog(td.datalog()) {
                            // Validate serial number format before proceeding
                            if let Some(serial) = td.inverter() {
                                if !serial.to_string().chars().all(|c| c.is_ascii_alphanumeric()) {
                                    warn!("Invalid serial number format: {}", serial);
                                    if let Ok(mut stats) = self.stats.lock() {
                                        stats.serial_mismatches += 1;
                                    }
                                    continue;
                                }
                            }

                            // Update packet stats after validation
                            if let Ok(mut stats) = self.stats.lock() {
                                stats.packets_received += 1;
                                stats.last_messages.insert(td.datalog(), format!("{:?}", packet));
                                stats.translated_data_packets_received += 1;
                            }

                            if let Err(e) = self.process_inverter_packet(packet.clone(), &inverter).await {
                                warn!("Failed to process packet: {}", e);
                            }
                        } else {
                            warn!("No enabled inverter found for datalog {}", td.datalog());
                        }
                    } else {
                        // Handle non-TranslatedData packets
                        if let Ok(mut stats) = self.stats.lock() {
                            stats.packets_received += 1;
                            match &packet {
                                Packet::Heartbeat(_) => stats.heartbeat_packets_received += 1,
                                Packet::ReadParam(_) => stats.read_param_packets_received += 1,
                                Packet::WriteParam(_) => stats.write_param_packets_received += 1,
                                _ => {}
                            }
                        }
                    }
                }
                inverter::ChannelData::Connected(datalog) => {
                    info!("Inverter connected: {}", datalog);
                    if let Err(e) = self.inverter_connected(datalog).await {
                        error!("Failed to process inverter connection: {}", e);
                    }
                }
                inverter::ChannelData::Disconnect(serial) => {
                    warn!("Inverter disconnected: {}", serial);
                    if let Ok(mut stats) = self.stats.lock() {
                        let count = stats.inverter_disconnections
                            .entry(serial)
                            .or_insert(0);
                        *count += 1;
                    }
                }
                inverter::ChannelData::Shutdown => {
                    info!("Received shutdown signal");
                    break;
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

        // Create a packet for stats tracking
        let packet = Packet::TranslatedData(TranslatedData {
            datalog: Serial::default(),
            device_function: DeviceFunction::ReadHold,
            inverter: Serial::default(),
            register: 0,
            values: vec![],
        });

        let block_size = inverter.register_block_size();

        // Read all holding register blocks
        for start_register in (0..=240).step_by(block_size as usize) {
            self.increment_packets_sent(&packet);
            self.read_hold(inverter.clone(), start_register as u16, block_size).await?;
        }

        // Read all input register blocks
        for start_register in (0..=200).step_by(block_size as usize) {
            self.increment_packets_sent(&packet);
            self.read_inputs(inverter.clone(), start_register as u16, block_size).await?;
        }

        // Read time registers
        for num in &[1, 2, 3] {
            self.increment_packets_sent(&packet);
            self.read_time_register(
                inverter.clone(),
                commands::time_register_ops::Action::AcCharge(*num),
            ).await?;

            self.increment_packets_sent(&packet);
            self.read_time_register(
                inverter.clone(),
                commands::time_register_ops::Action::ChargePriority(*num),
            ).await?;

            self.increment_packets_sent(&packet);
            self.read_time_register(
                inverter.clone(),
                commands::time_register_ops::Action::ForcedDischarge(*num),
            ).await?;

            self.increment_packets_sent(&packet);
            self.read_time_register(
                inverter.clone(),
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
                    if let Ok(mut stats) = self.stats.lock() {
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

        if let Ok(mut stats) = self.stats.lock() {
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
            let topic = format!("{}/inputs/{}", inverter.datalog, reg);
            if let Err(e) = self.publish_message(topic, value.to_string(), false).await {
                error!("Failed to publish input message: {}", e);
                if let Ok(mut stats) = self.stats.lock() {
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
            let topic = format!("{}/hold/{}", inverter.datalog, reg);
            if let Err(e) = self.publish_message(topic, value.to_string(), true).await {
                error!("Failed to publish hold message: {}", e);
                if let Ok(mut stats) = self.stats.lock() {
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

        let topic = format!("{}/write/status", inverter.datalog);
        if let Err(e) = self.publish_message(topic, format!("OK: {} = {}", register, value), false).await {
            error!("Failed to publish write confirmation: {}", e);
            if let Ok(mut stats) = self.stats.lock() {
                stats.mqtt_errors += 1;
            }
        }

        Ok(())
    }

    async fn publish_write_multi_confirmation(&self, pairs: Vec<(u16, u16)>, inverter: &config::Inverter) -> Result<()> {
        if !self.config.mqtt().enabled() {
            return Ok(());
        }

        let topic = format!("{}/write_multi/status", inverter.datalog);
        if let Err(e) = self.publish_message(topic, format!("OK: {:?}", pairs), false).await {
            error!("Failed to publish write multi confirmation: {}", e);
            if let Ok(mut stats) = self.stats.lock() {
                stats.mqtt_errors += 1;
            }
        }

        Ok(())
    }
}
