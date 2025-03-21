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
}

pub type InputsStore = std::collections::HashMap<Serial, crate::eg4::packet::ReadInputs>;

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
    config: Arc<ConfigWrapper>,
    mqtt: Option<Arc<Mqtt>>,
    influx: Option<Arc<Influx>>,
    databases: Vec<Arc<Database>>,
    datalog_writer: Option<Arc<DatalogWriter>>,
    channels: Channels,
    pub stats: Arc<Mutex<PacketStats>>,
}

impl Coordinator {
    pub fn new(config: Arc<ConfigWrapper>, channels: Channels) -> Self {
        Self {
            config,
            mqtt: None,
            influx: None,
            databases: Vec::new(),
            datalog_writer: None,
            channels,
            stats: Arc::new(Mutex::new(PacketStats::default())),
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        info!("Starting coordinator");

        // Initialize services
        self.start_mqtt()?;
        self.start_influx()?;
        self.start_databases()?;
        self.start_datalog_writer()?;

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
        let _ = self.channels.to_inverter.send(crate::eg4::inverter::ChannelData::Shutdown);
        let _ = self.channels.to_mqtt.send(mqtt::ChannelData::Shutdown);
        // The datalog writer will be dropped when the Coordinator is dropped
        // since it's wrapped in an Arc
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

        // Add delay after read operation
        tokio::time::sleep(std::time::Duration::from_millis(inverter.delay_ms())).await;
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

        // Add delay after read operation
        tokio::time::sleep(std::time::Duration::from_millis(inverter.delay_ms())).await;
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

        // Add delay after read operation
        tokio::time::sleep(std::time::Duration::from_millis(inverter.delay_ms())).await;
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

        // Add delay after read operation
        tokio::time::sleep(std::time::Duration::from_millis(inverter.delay_ms())).await;
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
        
        // Add delay after read operation
        tokio::time::sleep(std::time::Duration::from_millis(inverter.delay_ms())).await;
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
                            if let Ok(mut stats) = self.stats.lock() {
                                stats.modbus_errors += 1;
                            }
                            return Ok(());  // Return early as this is an error response
                        }
                    }
                }

                // Log TCP function for debugging
                debug!("Processing TCP function: {:?}", td.tcp_function());

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

                        if let Ok(mut stats) = self.stats.lock() {
                            stats.serial_mismatches += 1;
                            stats.last_messages.insert(
                                inverter_serial,
                                format!("Serial updated - was {}, now {}", inverter_serial, td.inverter)
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
                if let Ok(mut stats) = self.stats.lock() {
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
                                if let Ok(mut stats) = self.stats.lock() {
                                    stats.register_cache_errors += 1;
                                }
                            }
                            
                            // Parse and log the register value using the new module
                            let parsed = parse_input::parse_input_register(*reg, (*value).into());
                            debug!("  {}", parsed);
                        }

                        // Write to datalog file if enabled
                        if let Some(writer) = &self.datalog_writer {
                            if let Err(e) = writer.write_input_data(td.inverter, td.datalog, &pairs) {
                                error!("Failed to write to datalog file: {}", e);
                            }
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
                        debug!("Hold Register Values:");
                        for (reg, value) in &pairs {
                            // Cache the register value
                            if let Err(e) = self.channels.to_register_cache.send(register_cache::ChannelData::RegisterData(*reg, *value)) {
                                error!("Failed to cache register {}: {}", reg, e);
                                if let Ok(mut stats) = self.stats.lock() {
                                    stats.register_cache_errors += 1;
                                }
                            }
                            
                            // Parse and log the register value using the new module
                            let parsed = parse_hold::parse_hold_register(*reg, *value);
                            debug!("  {}", parsed);
                        }

                        // Write to datalog file if enabled
                        if let Some(writer) = &self.datalog_writer {
                            if let Err(e) = writer.write_hold_data(td.inverter, td.datalog, &pairs) {
                                error!("Failed to write to datalog file: {}", e);
                            }
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
            Packet::Heartbeat(_) => {
                if let Ok(mut stats) = self.stats.lock() {
                    stats.packets_received += 1;
                    stats.heartbeat_packets_received += 1;
                }
            }
            Packet::ReadParam(_) => {
                if let Ok(mut stats) = self.stats.lock() {
                    stats.packets_received += 1;
                    stats.read_param_packets_received += 1;
                }
            }
            Packet::WriteParam(_) => {
                if let Ok(mut stats) = self.stats.lock() {
                    stats.packets_received += 1;
                    stats.write_param_packets_received += 1;
                }
            }
        }

        Ok(())
    }

    async fn inverter_receiver(&self) -> Result<()> {
        let mut receiver = self.channels.from_inverter.subscribe();
        let mut buffer_size = 0;
        const BUFFER_CLEAR_THRESHOLD: usize = 1024; // 1KB threshold for buffer clearing
        const MAX_BUFFER_CLEAR_ATTEMPTS: u32 = 3; // Maximum number of buffer clear attempts

        while let Ok(message) = receiver.recv().await {
            match message {
                inverter::ChannelData::Packet(packet) => {
                    // Track buffer size
                    buffer_size += std::mem::size_of_val(&packet);
                    
                    // Clear buffer if threshold exceeded, but with a limit on attempts
                    if buffer_size >= BUFFER_CLEAR_THRESHOLD {
                        debug!("Clearing receiver buffer (size: {} bytes)", buffer_size);
                        let mut clear_attempts = 0;
                        while clear_attempts < MAX_BUFFER_CLEAR_ATTEMPTS {
                            match receiver.try_recv() {
                                Ok(inverter::ChannelData::Packet(_)) => {
                                    clear_attempts += 1;
                                    continue;
                                }
                                Ok(_) => break, // Non-packet message, stop clearing
                                Err(_) => break, // No more messages, stop clearing
                            }
                        }
                        buffer_size = 0;
                    }

                    // Process packet based on type first to avoid unnecessary cloning
                    let packet_clone = packet.clone();
                    if let Packet::TranslatedData(ref td) = packet {
                        // Find the inverter for this packet
                        if let Some(inverter) = self.config.enabled_inverter_with_datalog(td.datalog()) {
                            // Update packet stats before validation
                            if let Ok(mut stats) = self.stats.lock() {
                                stats.packets_received += 1;
                                stats.last_messages.insert(td.datalog(), format!("{:?}", packet_clone));
                                stats.translated_data_packets_received += 1;
                            }

                            // Process the packet even if serial validation fails
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
                inverter::ChannelData::Heartbeat(packet) => {
                    // Handle heartbeat packets similarly to regular packets
                    if let Ok(mut stats) = self.stats.lock() {
                        stats.packets_received += 1;
                        stats.heartbeat_packets_received += 1;
                        let packet_clone = packet.clone();
                        stats.last_messages.insert(packet.datalog(), format!("{:?}", packet_clone));
                    }
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
            self.read_hold_registers(&inverter, start_register as u16, block_size).await?;
        }

        // Read all input register blocks
        for start_register in (0..=200).step_by(block_size as usize) {
            self.increment_packets_sent(&packet);
            self.read_input_block(&inverter, start_register as u16, block_size).await?;
        }

        // Read time registers
        for num in &[1, 2, 3] {
            self.increment_packets_sent(&packet);
            self.read_time_register(
                &inverter,
                commands::time_register_ops::Action::AcCharge(*num),
            ).await?;

            self.increment_packets_sent(&packet);
            self.read_time_register(
                &inverter,
                commands::time_register_ops::Action::ChargePriority(*num),
            ).await?;

            self.increment_packets_sent(&packet);
            self.read_time_register(
                &inverter,
                commands::time_register_ops::Action::ForcedDischarge(*num),
            ).await?;

            self.increment_packets_sent(&packet);
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
            let topic = format!("{}/inputs/{}", inverter.datalog().map(|s| s.to_string()).unwrap_or_default(), reg);
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
            let topic = format!("{}/hold/{}", inverter.datalog().map(|s| s.to_string()).unwrap_or_default(), reg);
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

        let topic = format!("{}/write/status", inverter.datalog().map(|s| s.to_string()).unwrap_or_default());
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

        let topic = format!("{}/write_multi/status", inverter.datalog().map(|s| s.to_string()).unwrap_or_default());
        if let Err(e) = self.publish_message(topic, format!("OK: {:?}", pairs), false).await {
            error!("Failed to publish write multi confirmation: {}", e);
            if let Ok(mut stats) = self.stats.lock() {
                stats.mqtt_errors += 1;
            }
        }

        Ok(())
    }

    fn start_mqtt(&mut self) -> Result<()> {
        if self.config.mqtt().enabled() {
            info!("Initializing MQTT");
            let mqtt = Mqtt::new((*self.config).clone(), self.channels.clone());
            self.mqtt = Some(Arc::new(mqtt));
        }
        Ok(())
    }

    fn start_influx(&mut self) -> Result<()> {
        if self.config.influx().enabled() {
            info!("Initializing InfluxDB");
            let influx = Influx::new((*self.config).clone(), self.channels.clone());
            self.influx = Some(Arc::new(influx));
        }
        Ok(())
    }

    fn start_databases(&mut self) -> Result<()> {
        for db in &self.config.databases() {
            if db.enabled() {
                info!("Initializing database {}", db.url());
                let database = Database::new(db.clone(), self.channels.clone());
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
}

