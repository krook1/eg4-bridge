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

/// Manages all application components and their lifecycle
/// 
/// This struct holds references to all major components of the application
/// and provides methods to coordinate their startup and shutdown.
#[derive(Clone)]
pub struct Components {
    pub coordinator: Coordinator,      // Main application coordinator
    pub scheduler: Scheduler,          // Task scheduler
    pub mqtt: Option<Mqtt>,           // Optional MQTT client
    pub influx: Option<Influx>,       // Optional InfluxDB client
    pub databases: Vec<Database>,     // List of configured databases
    pub datalog_writer: Option<DatalogWriter>, // Optional data logger
    #[allow(dead_code)]
    pub channels: Channels,           // Inter-component communication channels
}

impl Components {
    /// Creates a new Components instance with all required components
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
        coordinator: coordinator.clone(),
        scheduler: scheduler.clone(),
        mqtt: Some(mqtt.clone()),
        influx: Some(influx.clone()),
        databases: databases.clone(),
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
    config: Arc<ConfigWrapper>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Initialize communication channels
    info!("Initializing channels...");
    let channels = Channels::new();

    // Initialize all components in dependency order
    info!("Initializing components...");
    
    // Start with RegisterCache as it's a dependency for other components
    info!("  Creating RegisterCache...");
    let _register_cache = RegisterCache::new(channels.clone());
    
    // Create Coordinator which manages the overall application flow
    info!("  Creating Coordinator...");
    let mut coordinator = Coordinator::new(config.clone(), channels.clone());
    
    // Initialize Scheduler for periodic tasks
    info!("  Creating Scheduler...");
    let scheduler = Scheduler::new((*config).clone(), channels.clone());
    
    // Set up MQTT client for external communication
    info!("  Creating MQTT client...");
    let mqtt = Mqtt::new((*config).clone(), channels.clone(), coordinator.shared_stats.clone());
    
    // Initialize InfluxDB client for time-series data
    info!("  Creating InfluxDB client...");
    let influx = Influx::new((*config).clone(), channels.clone(), coordinator.shared_stats.clone());

    // Create inverter instances for each configured inverter
    info!("  Creating Inverters...");
    let inverters: Vec<_> = config
        .enabled_inverters()
        .into_iter()
        .map(|inverter| Inverter::new((*config).clone(), &inverter, channels.clone()))
        .collect();
    info!("    Created {} inverter instances", inverters.len());

    // Initialize database connections
    info!("  Creating Databases...");
    let databases: Vec<_> = config
        .enabled_databases()
        .into_iter()
        .map(|database| Database::new(database, channels.clone(), coordinator.shared_stats.clone()))
        .collect();
    info!("    Created {} database instances", databases.len());

    // Start all components in the correct order
    info!("Starting components in sequence...");
    
    // Start databases first as they're a core dependency
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

    // Start InfluxDB before inverters to ensure data collection is ready
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
    if let Err(e) = coordinator.start().await {
        error!("Failed to start Coordinator: {}", e);
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
    info!("Coordinator started successfully");

    // Start MQTT client to enable external communication
    info!("Starting MQTT client...");
    if let Err(e) = mqtt.start().await {
        error!("Failed to start MQTT client: {}", e);
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
    info!("MQTT client started successfully");

    // Start Scheduler to begin periodic tasks
    info!("Starting Scheduler...");
    if let Err(e) = scheduler.start().await {
        error!("Failed to start Scheduler: {}", e);
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
    info!("Scheduler started successfully");

    // Start all configured inverters
    info!("Starting Inverters...");
    if let Err(e) = start_inverters(inverters.clone()).await {
        error!("Failed to start Inverters: {}", e);
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

    // Wait for shutdown signal
    info!("Waiting for shutdown signal...");
    let _ = shutdown_rx.recv().await;

    // Execute shutdown sequence
    info!("Shutdown signal received, stopping components...");
    let mut components = Components {
        coordinator,
        scheduler,
        mqtt: Some(mqtt),
        influx: Some(influx),
        databases,
        datalog_writer: None,
        channels,
    };
    components.stop().await;

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
    let total = inverters.len();
    info!("Starting {} inverters...", total);
    for (i, inverter) in inverters.into_iter().enumerate() {
        let config = inverter.config();
        let datalog = config.datalog().map(|s| s.to_string()).unwrap_or_default();
        let host = config.host();
        let port = config.port();
        debug!("Starting inverter {}/{} (datalog: {}, host: {}, port: {})", 
            i + 1, total, datalog, host, port);
        
        if let Err(e) = inverter.start().await {
            error!("Failed to start inverter {}: {}", datalog, e);
            bail!("Failed to start inverter {}: {}", datalog, e);
        }
        debug!("Successfully started inverter {}/{} (datalog: {}, host: {}, port: {})", 
            i + 1, total, datalog, host, port);
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
