use anyhow::Result;
use log::{error, info};
use std::sync::Arc;
use tokio::sync::broadcast;
use std::error::Error;
use std::time::Duration;
use clap::Parser;

use eg4_bridge::prelude::*;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the configuration file
    #[arg(short, long, default_value = "config.yaml")]
    config: String,

    /// Runtime duration in seconds before automatic termination
    #[arg(short, long)]
    runtime: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = Args::parse();
    let config = Config::new(args.config)?;
    let config = Arc::new(ConfigWrapper::from_config(config));

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
    if let Some(runtime) = args.runtime {
        let shutdown_tx_clone = shutdown_tx.clone();
        tokio::spawn(async move {
            info!("Runtime of {} seconds specified, will terminate automatically", runtime);
            tokio::time::sleep(Duration::from_secs(runtime)).await;
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


