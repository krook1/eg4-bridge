// Module declarations for the application's core components
pub mod channels;      // Inter-component communication channels
pub mod command;       // Command processing and handling
pub mod config;        // Configuration management
pub mod coordinator;   // Main application coordinator
pub mod database;      // Database operations and storage
pub mod datalog_writer; // Data logging functionality
pub mod home_assistant; // Home Assistant integration
pub mod influx;        // InfluxDB integration
pub mod mqtt;          // MQTT client and messaging
pub mod options;       // Command line options parsing
pub mod prelude;       // Common imports and types
pub mod register_cache; // Register value caching
pub mod scheduler;     // Task scheduling
pub mod unixtime;      // Unix timestamp handling
pub mod utils;         // Utility functions
pub mod eg4;           // EG4 inverter protocol implementation
pub mod error;         // Error handling and types
pub mod register;      // Register definitions and parsing

// Get the package version from Cargo.toml
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
use std::time::Duration;

/// Manages all application components and their lifecycle
/// 
/// This struct holds references to all major components of the application
/// and provides methods to coordinate their startup and shutdown.
#[derive(Clone)]
pub struct Components {
    pub coordinator: Arc<Coordinator>,      // Main application coordinator
    pub scheduler: Arc<Scheduler>,          // Task scheduler
    pub mqtt: Option<Arc<Mqtt>>,           // Optional MQTT client
    pub influx: Option<Arc<Influx>>,       // Optional InfluxDB client
    pub databases: Vec<Arc<Database>>,     // List of configured databases
    pub datalog_writer: Option<Arc<DatalogWriter>>, // Optional data logger
    pub channels: Channels,           // Inter-component communication channels
}

impl Components {
    /// Creates a new Components instance with all required components
    pub fn new(
        coordinator: Arc<Coordinator>,
        scheduler: Arc<Scheduler>,
        mqtt: Option<Arc<Mqtt>>,
        influx: Option<Arc<Influx>>,
        databases: Vec<Arc<Database>>,
        datalog_writer: Option<Arc<DatalogWriter>>,
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

    /// Gracefully stops all components in the correct order
    /// 
    /// The shutdown sequence is:
    /// 1. Coordinator (to stop processing new commands)
    /// 2. InfluxDB (to stop data collection)
    /// 3. MQTT (to stop message publishing)
    /// 4. Databases (to stop data storage)
    /// 5. Datalog writer (to stop logging)
    pub async fn stop(&mut self) {
        info!("Stopping all components...");
        
        // Stop coordinator first to prevent new command processing
        self.coordinator.stop();

        // Stop optional components if they exist
        if let Some(influx) = &self.influx {
            influx.stop();
        }
        if let Some(mqtt) = &self.mqtt {
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

/// Handles the application shutdown sequence
/// 
/// This function coordinates the shutdown of all components and ensures
/// that final statistics are collected before the application exits.
pub async fn shutdown(
    _shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    _config: Arc<ConfigWrapper>,
    channels: Channels,
    coordinator: Coordinator,
    scheduler: Scheduler,
    mqtt: Mqtt,
    influx: Influx,
    databases: Vec<Database>,
) -> Result<((), Arc<Mutex<PacketStats>>)> {
    info!("Initiating shutdown sequence");
    
    // Create components instance for coordinated shutdown
    let mut components = Components {
        coordinator: Arc::new(coordinator),
        scheduler: Arc::new(scheduler),
        mqtt: Some(Arc::new(mqtt)),
        influx: Some(Arc::new(influx)),
        databases: databases.into_iter().map(Arc::new).collect(),
        datalog_writer: None,
        channels: channels.clone(),
    };

    // Execute shutdown sequence
    components.stop().await;
    info!("Shutdown complete");

    // Collect final statistics after all components are stopped
    let stats = components.coordinator.shared_stats.clone();

    Ok(((), stats))
}

/// Main application entry point
/// 
/// This function initializes and starts all components of the application
/// in the correct order to ensure proper dependencies are available.
pub async fn app(
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    _config: Arc<ConfigWrapper>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Parse command line options
    let options = Options::new();
    let config_file = options.config_file.clone();

    // Initialize logging with default level
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

    // Load and validate configuration
    let config = ConfigWrapper::new(options.config_file).unwrap_or_else(|err| {
        error!("Failed to load config: {:?}", err);
        std::process::exit(255);
    });

    // Update log level based on configuration
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

    // Initialize communication channels
    info!("Initializing channels...");
    let channels = Channels::new();

    // Initialize all components in dependency order
    info!("Initializing components...");
    
    // Start with RegisterCache as it's a dependency for other components
    info!("  Creating RegisterCache...");
    let register_cache = RegisterCache::new(channels.clone());
    let register_cache_handle = tokio::spawn(async move {
        if let Err(e) = register_cache.start().await {
            error!("RegisterCache error: {}", e);
        }
    });
    
    // Create Coordinator which manages the overall application flow
    info!("  Creating Coordinator...");
    let config = Arc::new(config);
    let coordinator = Coordinator::new(config.clone(), channels.clone());
    let mut coordinator_clone = coordinator.clone();
    let coordinator_handle = tokio::spawn(async move {
        if let Err(e) = coordinator_clone.start().await {
            error!("Coordinator task failed: {}", e);
        }
    });
    
    // Initialize Scheduler for periodic tasks
    info!("  Creating Scheduler...");
    let scheduler = Scheduler::new((*config).clone(), channels.clone());
    let mut scheduler_clone = scheduler.clone();
    let scheduler_handle = tokio::spawn(async move {
        if let Err(e) = scheduler_clone.start().await {
            error!("Scheduler task failed: {}", e);
        }
    });
    
    // Set up MQTT client for external communication
    info!("  Creating MQTT client...");
    let mqtt = Mqtt::new((*config).clone(), channels.clone(), coordinator.shared_stats.clone());
    let mqtt_clone = mqtt.clone();
    let mqtt_handle = tokio::spawn(async move {
        if let Err(e) = mqtt_clone.start().await {
            error!("MQTT task failed: {}", e);
        }
    });
    
    // Set up InfluxDB client for time-series data
    info!("  Creating InfluxDB client...");
    let influx = Influx::new((*config).clone(), channels.clone(), coordinator.shared_stats.clone());
    let influx_clone = influx.clone();
    let influx_handle = tokio::spawn(async move {
        if let Err(e) = influx_clone.start().await {
            error!("InfluxDB task failed: {}", e);
        }
    });

    // Create inverter instances
    info!("  Creating Inverter instances...");
    let mut inverter_handles = Vec::new();
    for inverter in config
        .inverters()
        .iter()
        .filter(|inverter| inverter.enabled())
        .map(|inverter| Inverter::new((*config).clone(), inverter, channels.clone()))
    {
        let inverter_clone = inverter.clone();
        let handle = tokio::spawn(async move {
            if let Err(e) = inverter_clone.start().await {
                error!("Inverter task failed: {}", e);
            }
        });
        inverter_handles.push(handle);
    }
    info!("Inverters started successfully");

    // Initialize database connections
    info!("  Creating Database connections...");
    let databases: Vec<_> = config
        .databases()
        .iter()
        .filter(|db| db.enabled)
        .map(|db| Database::new(db.clone(), channels.clone(), coordinator.shared_stats.clone()))
        .collect();
    info!("    Created {} database connections", databases.len());

    // Wait for shutdown signal
    info!("Waiting for shutdown signal...");
    let _ = shutdown_rx.recv().await;

    // Execute shutdown sequence
    info!("Shutdown signal received, stopping components...");
    
    // First stop all components
    let mut components = Components {
        coordinator: Arc::new(coordinator),
        scheduler: Arc::new(scheduler),
        mqtt: Some(Arc::new(mqtt)),
        influx: Some(Arc::new(influx)),
        databases: databases.into_iter().map(Arc::new).collect(),
        datalog_writer: None,
        channels: channels.clone(),
    };
    components.stop().await;

    // Then wait for all task handles to complete
    if let Err(e) = coordinator_handle.await {
        error!("Error waiting for coordinator task: {}", e);
    }
    if let Err(e) = scheduler_handle.await {
        error!("Error waiting for scheduler task: {}", e);
    }
    for handle in inverter_handles {
        if let Err(e) = handle.await {
            error!("Error waiting for inverter task: {}", e);
        }
    }
    if let Err(e) = register_cache_handle.await {
        error!("Error waiting for register cache task: {}", e);
    }

    info!("Application shutdown complete");
    Ok(())
}

/// Starts all configured database connections
/// 
/// This function initializes connections to all enabled databases
/// and ensures they are ready to accept data.
async fn start_databases(databases: Vec<Database>) -> Result<()> {
    for database in databases {
        if let Err(e) = database.start().await {
            bail!("Failed to start database: {}", e);
        }
    }
    Ok(())
}

/// Starts all configured inverter connections
/// 
/// This function initializes connections to all enabled inverters
/// and begins monitoring their status and data.
async fn start_inverters(inverters: Vec<Inverter>) -> Result<()> {
    for inverter in inverters {
        if let Err(e) = inverter.start().await {
            bail!("Failed to start inverter: {}", e);
        }
    }
    Ok(())
}

/// Application entry point
/// 
/// This function is the main entry point for the application.
/// It initializes the configuration and starts the main application loop.
pub async fn run(config: Config) -> Result<()> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let config = Arc::new(ConfigWrapper::from_config(config));

    // Set up signal handlers for graceful shutdown
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            error!("Failed to listen for ctrl+c: {}", e);
        }
        let _ = shutdown_tx_clone.send(());
    });

    // Run the main application
    app(shutdown_rx, config).await.map_err(|e| anyhow::anyhow!("{}", e))?;

    Ok(())
}
