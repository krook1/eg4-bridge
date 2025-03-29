pub mod channels;
pub mod command;
pub mod config;
pub mod coordinator;
pub mod database;
pub mod datalog_writer;
pub mod home_assistant;
pub mod influx;
pub mod mqtt;
pub mod options;
pub mod prelude;
pub mod register_cache;
pub mod scheduler;
pub mod unixtime;
pub mod utils;
pub mod eg4;
pub mod error;
pub mod register;

const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

use crate::prelude::*;
use std::sync::{Arc, Mutex};
use crate::coordinator::PacketStats;
use crate::coordinator::Coordinator;
use crate::scheduler::Scheduler;
use crate::mqtt::Mqtt;
use crate::influx::Influx;
use crate::database::Database;
use crate::datalog_writer::DatalogWriter;
use crate::eg4::inverter::Inverter;
use crate::prelude::Channels;
use std::error::Error;

// Helper struct to manage component shutdown
#[derive(Clone)]
pub struct Components {
    pub coordinator: Coordinator,
    pub scheduler: Scheduler,
    pub mqtt: Option<Mqtt>,
    pub influx: Option<Influx>,
    pub databases: Vec<Database>,
    pub datalog_writer: Option<DatalogWriter>,
    #[allow(dead_code)]
    pub channels: Channels,
}

impl Components {
    pub fn new(
        coordinator: Coordinator,
        scheduler: Scheduler,
        mqtt: Option<Mqtt>,
        influx: Option<Influx>,
        databases: Vec<Database>,
        datalog_writer: Option<DatalogWriter>,
        channels: Channels,
    ) -> Self {
        Self {
            coordinator,
            scheduler,
            mqtt,
            influx,
            databases,
            datalog_writer,
            channels,
        }
    }

    pub async fn stop(&mut self) {
        info!("Stopping all components...");
        
        // Stop coordinator first
        self.coordinator.stop();

        // Stop other components if they exist
        if let Some(influx) = &self.influx {
            influx.stop();
        }
        if let Some(mqtt) = &mut self.mqtt {
            let _ = mqtt.stop().await;
        }
        for database in &self.databases {
            database.stop();
        }
        if let Some(writer) = &self.datalog_writer {
            let _ = writer.stop();
        }

        info!("Shutdown complete");
    }
}

pub async fn shutdown(
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    config: Arc<ConfigWrapper>,
    channels: Channels,
    coordinator: Coordinator,
    scheduler: Scheduler,
    mqtt: Mqtt,
    influx: Influx,
    databases: Vec<Database>,
) -> Result<((), Arc<Mutex<PacketStats>>)> {
    info!("Initiating shutdown sequence");
    
    // Stop all components in sequence
    let mut components = Components {
        coordinator: coordinator.clone(),
        scheduler: scheduler.clone(),
        mqtt: Some(mqtt.clone()),
        influx: Some(influx.clone()),
        databases: databases.clone(),
        datalog_writer: None,
        channels: channels.clone(),
    };

    components.stop().await;
    info!("Shutdown complete");

    // Get final stats after all components are stopped
    let stats = components.coordinator.shared_stats.clone();

    Ok(((), stats))
}

pub async fn app(
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    _config: Arc<ConfigWrapper>,
) -> Result<(), Box<dyn Error>> {
    let options = Options::new();
    let config_file = options.config_file.clone();

    // Initialize logger first
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format(|buf, record| {
            writeln!(
                buf,
                "[{} {} {}] {}",
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.3f"),
                record.level(),
                record.module_path().unwrap_or(""),
                record.args()
            )
        })
        .write_style(env_logger::WriteStyle::Never)
        .init();

    info!("Starting eg4-bridge {} with config file: {}", CARGO_PKG_VERSION, config_file);
    info!("eg4-bridge {} starting", CARGO_PKG_VERSION);

    // Load config after logger is initialized
    let config = ConfigWrapper::new(options.config_file).unwrap_or_else(|err| {
        error!("Failed to load config: {:?}", err);
        std::process::exit(255);
    });

    // Update log level from config
    if let Err(e) = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(config.loglevel()))
        .format(|buf, record| {
            writeln!(
                buf,
                "[{} {} {}] {}",
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.3f"),
                record.level(),
                record.module_path().unwrap_or(""),
                record.args()
            )
        })
        .write_style(env_logger::WriteStyle::Never)
        .try_init() {
        error!("Failed to update log level: {}", e);
    }

    info!("Initializing channels...");
    let channels = Channels::new();

    // Initialize components in a specific order
    info!("Initializing components...");
    info!("  Creating RegisterCache...");
    let register_cache = RegisterCache::new(channels.clone());
    
    info!("  Creating Coordinator...");
    let config = Arc::new(config);
    let coordinator = Coordinator::new(config.clone(), channels.clone());
    
    info!("  Creating Scheduler...");
    let scheduler = Scheduler::new((*config).clone(), channels.clone());
    
    info!("  Creating MQTT client...");
    let mqtt = Mqtt::new((*config).clone(), channels.clone(), coordinator.shared_stats.clone());
    
    info!("  Creating InfluxDB client...");
    let influx = Influx::new((*config).clone(), channels.clone(), coordinator.shared_stats.clone());

    info!("  Creating Inverters...");
    let inverters: Vec<_> = config
        .enabled_inverters()
        .into_iter()
        .map(|inverter| Inverter::new((*config).clone(), &inverter, channels.clone()))
        .collect();
    info!("    Created {} inverter instances", inverters.len());

    info!("  Creating Databases...");
    let databases: Vec<_> = config
        .enabled_databases()
        .into_iter()
        .map(|database| Database::new(database, channels.clone(), coordinator.shared_stats.clone()))
        .collect();
    info!("    Created {} database instances", databases.len());

    // Start components in sequence to ensure proper initialization
    info!("Starting components in sequence...");
    
    // Start databases first
    info!("Starting databases...");
    if let Err(e) = start_databases(databases.clone()).await {
        error!("Failed to start databases: {}", e);
        let mut components = Components {
            coordinator: coordinator.clone(),
            scheduler: scheduler.clone(),
            mqtt: Some(mqtt.clone()),
            influx: Some(influx.clone()),
            databases: databases.clone(),
            datalog_writer: None,
            channels: channels.clone(),
        };
        components.stop().await;
        return Err(e.into());
    }
    info!("Databases started successfully");

    // Start InfluxDB before inverters
    info!("Starting InfluxDB...");
    if let Err(e) = influx.start().await {
        error!("Failed to start InfluxDB: {}", e);
        let mut components = Components {
            coordinator: coordinator.clone(),
            scheduler: scheduler.clone(),
            mqtt: Some(mqtt.clone()),
            influx: Some(influx.clone()),
            databases: databases.clone(),
            datalog_writer: None,
            channels: channels.clone(),
        };
        components.stop().await;
        return Err(e.into());
    }
    info!("InfluxDB started successfully");

    // Start Coordinator before inverters to ensure it's ready to receive messages
    info!("Starting Coordinator...");
    let _coordinator_handle = tokio::spawn({
        let mut coordinator = coordinator.clone();
        async move {
            if let Err(e) = coordinator.start().await {
                error!("Coordinator error: {}", e);
            }
        }
    });

    // Start RegisterCache before inverters
    info!("Starting RegisterCache...");
    let _register_cache_handle = tokio::spawn(async move {
        if let Err(e) = register_cache.start().await {
            error!("RegisterCache error: {}", e);
        }
    });

    // Start inverters
    info!("Starting inverters...");
    if let Err(e) = start_inverters(inverters.clone()).await {
        error!("Failed to start inverters: {}", e);
        let mut components = Components {
            coordinator: coordinator.clone(),
            scheduler: scheduler.clone(),
            mqtt: Some(mqtt.clone()),
            influx: Some(influx.clone()),
            databases: databases.clone(),
            datalog_writer: None,
            channels: channels.clone(),
        };
        components.stop().await;
        return Err(e.into());
    }
    info!("Inverters started successfully");

    // Start remaining components
    info!("Starting remaining components (scheduler, MQTT)...");
    let _app_result: Result<()> = tokio::select! {
        res = async {
            futures::try_join!(
                scheduler.start(),
                mqtt.start(),
            )
        } => {
            if let Err(e) = res {
                error!("Application error: {}", e);
            }
            Ok(())
        }
        _ = shutdown_rx.recv() => {
            info!("Initiating shutdown sequence");
            Ok(())
        }
    };

    // Stop all components in sequence
    info!("Shutting down...");
    let mut components = Components {
        coordinator: coordinator.clone(),
        scheduler: scheduler.clone(),
        mqtt: Some(mqtt.clone()),
        influx: Some(influx.clone()),
        databases: databases.clone(),
        datalog_writer: None,
        channels: channels.clone(),
    };
    components.stop().await;
    info!("Shutdown complete");

    Ok(())
}

async fn start_databases(databases: Vec<Database>) -> Result<()> {
    let futures = databases.iter().map(|d| d.start());
    futures::future::join_all(futures).await;
    Ok(())
}

async fn start_inverters(inverters: Vec<Inverter>) -> Result<()> {
    for inverter in &inverters {
        let config = inverter.config();
        info!(
            "Starting inverter - Serial: {}, Datalog: {}, Host: {}",
            config.serial().map(|s| s.to_string()).unwrap_or_default(),
            config.datalog().map(|s| s.to_string()).unwrap_or_default(),
            config.host(),
        );
    }
    let futures = inverters.iter().map(|i| i.start());
    futures::future::join_all(futures).await;
    Ok(())
}

pub async fn run(config: Config) -> Result<()> {
    info!("Starting up...");

    info!("  Creating Channels...");
    let channels = Channels::new();

    info!("  Creating Coordinator...");
    let config = Arc::new(ConfigWrapper::from_config(config));
    let mut coordinator = Coordinator::new(config.clone(), channels.clone());

    info!("  Creating Scheduler...");
    let scheduler = scheduler::Scheduler::new((*config).clone(), channels.clone());

    info!("  Creating Register Cache...");
    let register_cache = register_cache::RegisterCache::new(channels.clone());

    info!("  Starting Register Cache...");
    let register_cache_handle = tokio::spawn(async move {
        if let Err(e) = register_cache.start().await {
            error!("register_cache error: {}", e);
        }
    });

    info!("  Starting Scheduler...");
    let scheduler_handle = tokio::spawn(async move {
        if let Err(e) = scheduler.start().await {
            error!("scheduler error: {}", e);
        }
    });

    info!("  Starting Coordinator...");
    let coordinator_handle = tokio::spawn(async move {
        if let Err(e) = coordinator.start().await {
            error!("coordinator error: {}", e);
        }
    });

    futures::try_join!(register_cache_handle, scheduler_handle, coordinator_handle)?;

    Ok(())
}
