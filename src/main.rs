use anyhow::Result;
use log::{info, error};
use tokio::signal::ctrl_c;
use tokio::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);
    let shutdown_rx = shutdown_tx.subscribe();

    let app_result = tokio::select! {
        result = eg4_bridge::app(shutdown_rx) => result,
        _ = async {
            if let Ok(()) = ctrl_c().await {
                info!("Received SIGINT, initiating graceful shutdown");
                let _ = shutdown_tx.send(());
            }
            info!("Waiting for app to complete shutdown...");
            match tokio::time::timeout(Duration::from_secs(30), eg4_bridge::app(shutdown_tx.subscribe())).await {
                Ok(result) => result,
                Err(_) => {
                    error!("Shutdown timed out after 30 seconds");
                    Err(anyhow::anyhow!("Shutdown timed out"))
                }
            }
        } => {
            info!("Waiting for app to complete shutdown...");
            match tokio::time::timeout(Duration::from_secs(30), eg4_bridge::app(shutdown_tx.subscribe())).await {
                Ok(result) => result,
                Err(_) => {
                    error!("Shutdown timed out after 30 seconds");
                    Err(anyhow::anyhow!("Shutdown timed out"))
                }
            }
        }
    };

    Ok(match app_result {
        Ok((_, stats)) => {
            let stats = stats.lock().map_err(|e| anyhow::anyhow!("Failed to lock stats: {}", e))?;
            info!("Final statistics:");
            info!("  Total packets received: {}", stats.packets_received);
            info!("  Total packets sent: {}", stats.packets_sent);
            info!("  MQTT messages sent: {}", stats.mqtt_messages_sent);
            info!("  InfluxDB writes: {}", stats.influx_writes);
            info!("  Database writes: {}", stats.database_writes);
            Ok(())
        }
        Err(e) => {
            error!("Application error: {}", e);
            Err(e)
        }
    }?)
}


