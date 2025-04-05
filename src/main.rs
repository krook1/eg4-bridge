use anyhow::Result;
use log::{error, info};
use std::sync::Arc;
use tokio::sync::broadcast;
use std::error::Error;
use std::time::Duration;
use clap::Parser;
use std::io::Write;

use eg4_bridge::prelude::*;

// Get the package version from Cargo.toml
const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

/// EG4 Bridge - A bridge for EG4 inverters
#[derive(Parser)]
#[command(author, version, about)]
struct Args {
    /// Optional configuration file path
    #[arg(short, long, default_value = "config.yaml")]
    config: String,

    /// Optional runtime limit in seconds
    #[arg(short, long)]
    time: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Parse command line arguments
    let args = Args::parse();

    // Load configuration from the specified file
    let config = Config::new(args.config)?;
    let config = Arc::new(ConfigWrapper::from_config(config));

    // Initialize logging once with the configured level
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

    info!("Starting eg4-bridge {}", CARGO_PKG_VERSION);

    // Create a channel for shutdown signaling
    let (shutdown_tx, _) = broadcast::channel(1);

    // Handle Ctrl+C
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            error!("Failed to listen for Ctrl+C: {}", e);
        }
        info!("Ctrl+C received, initiating shutdown");
        if let Err(e) = shutdown_tx_clone.send(()) {
            error!("Failed to send shutdown signal: {}", e);
        }
    });

    // Run the application
    let app_handle = tokio::spawn(eg4_bridge::app(shutdown_tx.subscribe(), config.clone()));

    // If runtime is specified, spawn a task to terminate after the specified duration
    if let Some(time) = args.time {
        let shutdown_tx_clone = shutdown_tx.clone();
        tokio::spawn(async move {
            info!("Runtime of {} seconds specified, will terminate automatically", time);
            tokio::time::sleep(Duration::from_secs(time)).await;
            info!("Runtime duration reached, initiating shutdown");
            if let Err(e) = shutdown_tx_clone.send(()) {
                error!("Failed to send shutdown signal: {}", e);
            }
        });
    }

    // Wait for the application to complete
    if let Err(e) = app_handle.await? {
        error!("Application error: {}", e);
    }

    Ok(())
}


