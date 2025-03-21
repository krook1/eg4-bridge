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

const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

use crate::prelude::*;
use std::sync::Arc;
use std::sync::Mutex;

// Helper struct to manage component shutdown
#[derive(Clone)]
pub struct Components {
    coordinator: Coordinator,
    mqtt: Mqtt,
    influx: Influx,
    inverters: Vec<Inverter>,
    databases: Vec<Database>,
    channels: Channels,
}

impl Components {
    fn stop(mut self) {
        // First send shutdown signals to all components
        info!("Sending shutdown signals...");
        let _ = self.channels.from_inverter.send(eg4::inverter::ChannelData::Shutdown);
        let _ = self.channels.from_mqtt.send(mqtt::ChannelData::Shutdown);
        let _ = self.channels.to_influx.send(influx::ChannelData::Shutdown);
        
        // Give a moment for shutdown signals to be processed
        std::thread::sleep(std::time::Duration::from_millis(500));

        // Print final statistics
        if let Ok(stats) = self.coordinator.stats.lock() {
            info!("Final Statistics:");
            stats.print_summary();
        } else {
            error!("Failed to lock statistics for printing");
        }

        // Now stop all components
        info!("Stopping components...");
        for inverter in self.inverters {
            inverter.stop();
        }
        for database in self.databases {
            database.stop();
        }
        self.mqtt.stop();
        self.influx.stop();
        self.coordinator.stop();
    }
}

pub async fn app() -> Result<()> {
    let options = Options::new();
    info!("Starting eg4-bridge {} with config file: {}", CARGO_PKG_VERSION, options.config_file);

    let config = ConfigWrapper::new(options.config_file).unwrap_or_else(|err| {
        // no logging available yet, so eprintln! will have to do
        eprintln!("Error: {:?}", err);
        std::process::exit(255);
    });

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(config.loglevel()))
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

    info!("eg4-bridge {} starting", CARGO_PKG_VERSION);

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
    let mqtt = Mqtt::new((*config).clone(), channels.clone());
    
    info!("  Creating InfluxDB client...");
    let influx = Influx::new((*config).clone(), channels.clone());

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
        .map(|database| Database::new(database, channels.clone()))
        .collect();
    info!("    Created {} database instances", databases.len());

    // Store components that need to be stopped
    let components = Components {
        coordinator: coordinator.clone(),
        mqtt: mqtt.clone(),
        influx: influx.clone(),
        inverters: inverters.clone(),
        databases: databases.clone(),
        channels: channels.clone(),
    };

    // Set up graceful shutdown
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    
    // Handle Ctrl+C
    tokio::spawn(async move {
        if let Ok(()) = tokio::signal::ctrl_c().await {
            info!("Received Ctrl+C, initiating graceful shutdown");
            let _ = shutdown_tx.send(());
        }
    });

    // Start components in sequence to ensure proper initialization
    info!("Starting components in sequence...");
    
    // Start databases first
    info!("Starting databases...");
    if let Err(e) = start_databases(databases.clone()).await {
        error!("Failed to start databases: {}", e);
        components.stop();
        return Err(e);
    }
    info!("Databases started successfully");

    // Start InfluxDB before inverters
    info!("Starting InfluxDB...");
    if let Err(e) = influx.start().await {
        error!("Failed to start InfluxDB: {}", e);
        components.stop();
        return Err(e);
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
        components.stop();
        return Err(e);
    }
    info!("Inverters started successfully");

    // Start remaining components
    info!("Starting remaining components (scheduler, MQTT)...");
    let app_result = tokio::select! {
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
        _ = shutdown_rx => {
            info!("Initiating shutdown sequence");
            Ok(())
        }
    };

    // Graceful shutdown sequence
    info!("Stopping all components...");
    components.stop();
    info!("Shutdown complete");

    app_result
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
