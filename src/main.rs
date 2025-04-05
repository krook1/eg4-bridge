use anyhow::Result;
use log::error;
use std::sync::Arc;
use tokio::sync::broadcast;
use std::error::Error;

use eg4_bridge::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let config_file = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.yaml".to_string());

    let config = Config::new(config_file)?;
    let config = Arc::new(ConfigWrapper::from_config(config));

    // Create a channel for shutdown signaling
    let (shutdown_tx, _) = broadcast::channel(1);

    // Handle Ctrl+C
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            error!("Failed to listen for Ctrl+C: {}", e);
        }
        if let Err(e) = shutdown_tx_clone.send(()) {
            error!("Failed to send shutdown signal: {}", e);
        }
    });

    // Run the application
    let app_handle = tokio::spawn(eg4_bridge::app(shutdown_tx.subscribe(), config.clone()));

    // Wait for the application to complete
    if let Err(e) = app_handle.await? {
        error!("Application error: {}", e);
    }

    Ok(())
}


