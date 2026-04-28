use tokio_util::sync::CancellationToken;
use tracing::info;

pub fn setup_shutdown_handler() -> CancellationToken {
    let token = CancellationToken::new();
    let shutdown_token = token.clone();

    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut sigterm =
                signal(SignalKind::terminate()).expect("Failed to listen for SIGTERM");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("SIGINT received, stopping gracefully...");
                }
                _ = sigterm.recv() => {
                    info!("SIGTERM received, stopping gracefully...");
                }
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to listen for Ctrl+C");
            info!("SIGINT received, stopping gracefully...");
        }
        shutdown_token.cancel();
    });

    token
}
