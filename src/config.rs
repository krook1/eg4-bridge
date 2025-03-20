use crate::prelude::*;

use serde::Deserialize;
use serde_with::serde_as;
use serde_yaml;
use std::sync::{Arc, Mutex};

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub inverters: Vec<Inverter>,
    pub mqtt: Mqtt,
    pub influx: Influx,
    #[serde(default = "Vec::new")]
    pub databases: Vec<Database>,

    pub scheduler: Option<Scheduler>,

    #[serde(default = "Config::default_loglevel")]
    pub loglevel: String,

    pub read_only: bool,

    #[serde(default = "Config::default_homeassistant_enabled")]
    pub homeassistant_enabled: bool,

    #[serde(default = "Config::default_strict_data_check")]
    pub strict_data_check: bool,

    /// Optional path to output datalog data in JSON format
    pub datalog_file: Option<String>,
}

// Inverter {{{
#[derive(Clone, Debug, Deserialize)]
pub struct Inverter {
    #[serde(default = "Config::default_enabled")]
    pub enabled: bool,

    pub host: String,
    pub port: u16,
    #[serde(deserialize_with = "de_serial")]
    pub serial: Option<Serial>,
    #[serde(deserialize_with = "de_serial")]
    pub datalog: Option<Serial>,

    pub heartbeats: Option<bool>,
    pub publish_holdings_on_connect: Option<bool>,
    pub read_timeout: Option<u64>,
    pub use_tcp_nodelay: Option<bool>,
    pub register_block_size: Option<u16>,
    pub delay_ms: Option<u64>,
    pub read_only: Option<bool>,
}
impl Inverter {
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn serial(&self) -> Option<Serial> {
        self.serial
    }

    pub fn datalog(&self) -> Option<Serial> {
        self.datalog
    }

    pub fn heartbeats(&self) -> bool {
        self.heartbeats == Some(true)
    }

    pub fn publish_holdings_on_connect(&self) -> bool {
        self.publish_holdings_on_connect == Some(true)
    }

    pub fn read_timeout(&self) -> u64 {
        self.read_timeout.unwrap_or(900) // 15 minutes
    }

    pub fn use_tcp_nodelay(&self) -> bool {
        self.use_tcp_nodelay.unwrap_or(true)  // Default to true for backward compatibility
    }

    pub fn register_block_size(&self) -> u16 {
        self.register_block_size.unwrap_or(40)  // Default to 40 for backward compatibility
    }

    pub fn delay_ms(&self) -> u64 {
        self.delay_ms.unwrap_or(1000)  // Default to 1000ms if not specified
    }

    pub fn read_only(&self) -> bool {
        self.read_only == Some(true)  // Default to false if not specified
    }
} // }}}

// HomeAssistant {{{
#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct HomeAssistant {
    #[serde(default = "Config::default_enabled")]
    pub enabled: bool,

    #[serde(default = "Config::default_mqtt_homeassistant_prefix")]
    pub prefix: String,
}

impl HomeAssistant {
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }
} // }}}

// Mqtt {{{
#[derive(Clone, Debug, Deserialize)]
pub struct Mqtt {
    #[serde(default = "Config::default_enabled")]
    pub enabled: bool,

    pub host: String,
    #[serde(default = "Config::default_mqtt_port")]
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,

    #[serde(default = "Config::default_mqtt_namespace")]
    pub namespace: String,

    #[serde(default = "Config::default_mqtt_homeassistant")]
    pub homeassistant: HomeAssistant,

    pub publish_individual_input: Option<bool>,
}
impl Mqtt {
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn username(&self) -> &Option<String> {
        &self.username
    }

    pub fn password(&self) -> &Option<String> {
        &self.password
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn homeassistant(&self) -> &HomeAssistant {
        &self.homeassistant
    }

    pub fn publish_individual_input(&self) -> bool {
        self.publish_individual_input == Some(true)
    }
} // }}}

// Influx {{{
#[derive(Clone, Debug, Deserialize)]
pub struct Influx {
    #[serde(default = "Config::default_enabled")]
    pub enabled: bool,

    pub url: String,
    pub username: Option<String>,
    pub password: Option<String>,

    pub database: String,
}
impl Influx {
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn username(&self) -> &Option<String> {
        &self.username
    }

    pub fn password(&self) -> &Option<String> {
        &self.password
    }

    pub fn database(&self) -> &str {
        &self.database
    }
} // }}}

// Database {{{
#[derive(Clone, Debug, Deserialize)]
pub struct Database {
    #[serde(default = "Config::default_enabled")]
    pub enabled: bool,

    pub url: String,
}
impl Database {
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn url(&self) -> &str {
        &self.url
    }
} // }}}

// Scheduler {{{
#[derive(Clone, Debug, Deserialize)]
pub struct Scheduler {
    #[serde(default = "Config::default_enabled")]
    pub enabled: bool,

    pub timesync_cron: Option<String>,
}
impl Scheduler {
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn timesync_cron(&self) -> &Option<String> {
        &self.timesync_cron
    }
} // }}}

pub struct ConfigWrapper {
    config: Arc<Mutex<Config>>,
}

impl Clone for ConfigWrapper {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
        }
    }
}

impl ConfigWrapper {
    pub fn new(file: String) -> Result<Self> {
        let config = Config::new(file)?;
        Ok(Self {
            config: Arc::new(Mutex::new(config)),
        })
    }

    pub fn inverters(&self) -> Vec<Inverter> {
        self.config.lock().unwrap().inverters.clone()
    }

    pub fn set_inverters(&self, new: Vec<Inverter>) {
        self.config.lock().unwrap().inverters = new;
    }

    pub fn enabled_inverters(&self) -> Vec<Inverter> {
        self.inverters().into_iter().filter(|i| i.enabled()).collect()
    }

    pub fn inverter_with_host(&self, host: &str) -> Option<Inverter> {
        self.inverters().into_iter().find(|i| i.host() == host)
    }

    pub fn enabled_inverter_with_datalog(&self, datalog: Serial) -> Option<Inverter> {
        self.enabled_inverters()
            .into_iter()
            .find(|i| i.datalog() == Some(datalog))
    }

    pub fn inverters_for_message(&self, message: &mqtt::Message) -> Result<Vec<Inverter>> {
        let (target_inverter, _) = message.split_cmd_topic()?;
        let inverters = self.enabled_inverters();

        match target_inverter {
            mqtt::TargetInverter::All => Ok(inverters),
            mqtt::TargetInverter::Serial(datalog) => Ok(inverters
                .into_iter()
                .filter(|i| i.datalog() == Some(datalog))
                .collect()),
        }
    }

    pub fn mqtt(&self) -> Mqtt {
        self.config.lock().unwrap().mqtt.clone()
    }

    pub fn influx(&self) -> Influx {
        self.config.lock().unwrap().influx.clone()
    }

    pub fn databases(&self) -> Vec<Database> {
        self.config.lock().unwrap().databases.clone()
    }

    pub fn set_databases(&self, new: Vec<Database>) {
        self.config.lock().unwrap().databases = new;
    }

    pub fn have_enabled_database(&self) -> bool {
        self.enabled_databases().len() > 0
    }

    pub fn enabled_databases(&self) -> Vec<Database> {
        self.databases().into_iter().filter(|d| d.enabled()).collect()
    }

    pub fn scheduler(&self) -> Option<Scheduler> {
        self.config.lock().unwrap().scheduler.clone()
    }

    pub fn loglevel(&self) -> String {
        self.config.lock().unwrap().loglevel.clone()
    }

    pub fn read_only(&self) -> bool {
        self.config.lock().unwrap().read_only
    }

    /// Update an inverter's serial number at runtime
    pub fn update_inverter_serial(&self, old_serial: Serial, new_serial: Serial) -> Result<()> {
        let mut config = self.config.lock().map_err(|_| anyhow::anyhow!("config.rs:Failed to lock config"))?;
        
        // Find and update the inverter
        for inverter in &mut config.inverters {
            if inverter.serial == Some(old_serial) {
                info!("Updating inverter serial from {} to {}", old_serial, new_serial);
                inverter.serial = Some(new_serial);
                return Ok(());
            }
        }
        
        Err(anyhow::anyhow!("config.rs:Inverter with serial {} not found", old_serial))
    }

    /// Update an inverter's datalog value at runtime
    pub fn update_inverter_datalog(&self, old_datalog: Serial, new_datalog: Serial) -> Result<()> {
        let mut config = self.config.lock().map_err(|_| anyhow::anyhow!("config.rs:Failed to lock config"))?;
        
        // Find and update the inverter
        for inverter in &mut config.inverters {
            if inverter.datalog == Some(old_datalog) {
                info!("Updating inverter datalog from {} to {}", old_datalog, new_datalog);
                inverter.datalog = Some(new_datalog);
                return Ok(());
            }
        }
        
        Err(anyhow::anyhow!("config.rs:Inverter with datalog {} not found", old_datalog))
    }

    pub fn homeassistant_enabled(&self) -> bool {
        self.config.lock().unwrap().homeassistant_enabled
    }

    pub fn datalog_file(&self) -> Option<String> {
        self.config.lock().unwrap().datalog_file.clone()
    }

    pub fn strict_data_check(&self) -> bool {
        self.config.lock().unwrap().strict_data_check
    }
}

impl Config {
    pub fn new(file: String) -> Result<Self> {
        info!("Reading configuration from {}", file);
        let content = std::fs::read_to_string(&file)
            .map_err(|err| anyhow!("config.rs:error reading {}: {}", file, err))?;

        let config: Self = serde_yaml::from_str(&content)?;
        
        // Log configuration details
        info!("Configuration loaded successfully:");
        info!("  Inverters: {} configured, {} enabled", 
            config.inverters.len(),
            config.inverters.iter().filter(|i| i.enabled).count()
        );
        for (i, inv) in config.inverters.iter().enumerate() {
            info!("    Inverter[{}]:", i);
            info!("      Enabled: {}", inv.enabled);
            info!("      Host: {}", inv.host);
            info!("      Port: {}", inv.port);
            info!("      Serial: {}", inv.serial.map(|s| s.to_string()).unwrap_or_default());
            info!("      Datalog: {}", inv.datalog.map(|s| s.to_string()).unwrap_or_default());
            info!("      Read Timeout: {}s", inv.read_timeout.unwrap_or(900));
            info!("      TCP NoDelay: {}", inv.use_tcp_nodelay.unwrap_or(true));
            info!("      Register Block Size: {}", inv.register_block_size.unwrap_or(40));
            info!("      Delay MS: {}ms", inv.delay_ms.unwrap_or(1000));
            info!("      Read Only: {}", inv.read_only.unwrap_or(false));
        }

        info!("  MQTT: {}", if config.mqtt.enabled { "enabled" } else { "disabled" });
        if config.mqtt.enabled {
            info!("    Host: {}", config.mqtt.host);
            info!("    Port: {}", config.mqtt.port);
            info!("    Namespace: {}", config.mqtt.namespace);
            info!("    Home Assistant: {}", if config.mqtt.homeassistant.enabled { "enabled" } else { "disabled" });
        }

        info!("  InfluxDB: {}", if config.influx.enabled { "enabled" } else { "disabled" });
        if config.influx.enabled {
            info!("    URL: {}", config.influx.url);
            info!("    Database: {}", config.influx.database);
        }

        info!("  Databases: {} configured, {} enabled",
            config.databases.len(),
            config.databases.iter().filter(|d| d.enabled).count()
        );
        for (i, db) in config.databases.iter().enumerate() {
            info!("    Database[{}]:", i);
            info!("      Enabled: {}", db.enabled);
            info!("      URL: {}", db.url);
        }

        info!("  Scheduler: {}", if config.scheduler.is_some() { "enabled" } else { "disabled" });
        if let Some(scheduler) = &config.scheduler {
            info!("    Enabled: {}", scheduler.enabled);
            if let Some(cron) = &scheduler.timesync_cron {
                info!("    Timesync Cron: {}", cron);
            }
        }

        info!("  Global Read Only: {}", config.read_only);
        info!("  Log Level: {}", config.loglevel);

        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        // Validate MQTT configuration
        if self.mqtt.enabled {
            if self.mqtt.port == 0 {
                bail!("mqtt.port must be between 1 and 65535");
            }
            if self.mqtt.host.is_empty() {
                return Err(anyhow!("config.rs:MQTT host cannot be empty"));
            }
        }

        // Validate InfluxDB configuration
        if self.influx.enabled {
            if let Err(e) = url::Url::parse(&self.influx.url) {
                return Err(anyhow!("config.rs:Invalid InfluxDB URL: {}", e));
            }
            if self.influx.database.is_empty() {
                return Err(anyhow!("config.rs:InfluxDB database name cannot be empty"));
            }
        }

        // Validate database URLs
        for db in &self.databases {
            if db.enabled {
                if let Err(e) = url::Url::parse(db.url()) {
                    return Err(anyhow!("config.rs:Invalid database URL: {}", e));
                }
            }
        }

        // Validate inverter configurations
        for (i, inv) in self.inverters.iter().enumerate() {
            if inv.enabled {
                if inv.port == 0 {
                    bail!("inverter[{}].port must be between 1 and 65535", i);
                }
                if inv.host.is_empty() {
                    return Err(anyhow!("config.rs:Inverter host cannot be empty"));
                }
                if inv.read_timeout.unwrap_or(900) == 0 {
                    return Err(anyhow!("config.rs:Invalid read timeout: 0"));
                }
            }
        }

        // Validate scheduler configuration
        if let Some(scheduler) = &self.scheduler {
            if scheduler.enabled {
                if let Some(cron) = &scheduler.timesync_cron {
                    if cron.is_empty() {
                        return Err(anyhow!("config.rs:Scheduler cron expression cannot be empty"));
                    }
                }
            }
        }

        Ok(())
    }

    fn default_mqtt_port() -> u16 {
        1883
    }
    fn default_mqtt_namespace() -> String {
        "lxp".to_string()
    }

    fn default_mqtt_homeassistant() -> HomeAssistant {
        HomeAssistant {
            enabled: Self::default_enabled(),
            prefix: Self::default_mqtt_homeassistant_prefix(),
        }
    }

    fn default_mqtt_homeassistant_prefix() -> String {
        "homeassistant".to_string()
    }

    fn default_enabled() -> bool {
        true
    }

    fn default_loglevel() -> String {
        "debug".to_string()
    }

    fn default_homeassistant_enabled() -> bool {
        false
    }

    fn default_strict_data_check() -> bool {
        false
    }
}

fn de_serial<'de, D>(deserializer: D) -> Result<Option<Serial>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    if s.is_empty() {
        Ok(None)
    } else {
        Serial::from_str(&s).map(Some).map_err(serde::de::Error::custom)
    }
}
