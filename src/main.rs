use anyhow::Result;
use log::{error, info};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::oneshot;

#[tokio::main]
async fn main() {
    // Create a shutdown channel that will be used by both paths
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    
    tokio::select! {
        result = lxp_bridge::app() => {
            if let Err(err) = result {
                error!("{:?}", err);
                std::process::exit(255);
            }
            // Wait for shutdown to complete
            let _ = shutdown_rx.await;
        }
        _ = handle_signals(shutdown_tx) => {
            // Wait for the app to complete its shutdown sequence
            let _ = shutdown_rx.await;
        }
    }
}

/// Provides a future that will terminate once a SIGINT or SIGTERM is
/// received from the host. Allows the process to be terminated
/// cleanly when running in a container (particularly Kubernetes).
async fn handle_signals(shutdown_tx: oneshot::Sender<()>) -> Result<()> {
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;

    tokio::select! {
        _ = sigterm.recv() => {
            info!("Received SIGTERM, stopping process");
        },
        _ = sigint.recv() => {
            info!("Received SIGINT, stopping process");
        },
    }

    // Send shutdown signal to trigger proper cleanup
    let _ = shutdown_tx.send(());
    Ok(())
}
