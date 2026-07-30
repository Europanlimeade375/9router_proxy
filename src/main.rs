mod config;
mod models;
mod proxy;
mod proxy_pool;

use crate::{config::Config, models::ModelRegistry, proxy_pool::ProxyPool};
use std::{error::Error, io};
use tokio::{net::TcpListener, sync::watch, time::timeout};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("9router_proxy failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init()?;

    let config = Config::from_env()?;
    let models = ModelRegistry::load(&config.model_config_path)?;
    tracing::info!(
        profiles = models.profile_count(),
        aliases = models.alias_count(),
        "loaded model quality profiles"
    );
    let proxy_pool = ProxyPool::initialize(&config).await?;
    proxy_pool.spawn_refresh();
    let state = proxy::build_state(&config, models, proxy_pool);
    let app = proxy::service(state);
    let listener = TcpListener::bind(config.listen_addr).await?;
    let local_addr = listener.local_addr()?;

    tracing::info!(address = %local_addr, "router proxy listening");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        match shutdown_signal().await {
            Ok(()) => {
                let _ = shutdown_tx.send(true);
            }
            Err(error) => tracing::error!(%error, "failed to listen for shutdown signal"),
        }
    });

    let mut server_shutdown = shutdown_rx.clone();
    let server = async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { wait_for_shutdown(&mut server_shutdown).await })
            .await
    };
    tokio::pin!(server);

    let mut grace_timer = shutdown_rx;
    tokio::select! {
        result = &mut server => result?,
        () = wait_for_shutdown(&mut grace_timer) => {
            tracing::info!("shutdown signal received; draining active requests");
            match timeout(config.shutdown_grace, &mut server).await {
                Ok(result) => result?,
                Err(_) => tracing::warn!("graceful shutdown deadline elapsed; stopping server"),
            }
        }
    }

    Ok(())
}

async fn wait_for_shutdown(receiver: &mut watch::Receiver<bool>) {
    if *receiver.borrow() {
        return;
    }
    while receiver.changed().await.is_ok() {
        if *receiver.borrow_and_update() {
            return;
        }
    }
}

async fn shutdown_signal() -> io::Result<()> {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = ctrl_c => result,
            _ = terminate.recv() => Ok(()),
        }
    }

    #[cfg(not(unix))]
    ctrl_c.await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn shutdown_waiter_observes_notification() {
        let (sender, mut receiver) = watch::channel(false);
        sender.send(true).expect("receiver should remain connected");

        wait_for_shutdown(&mut receiver).await;
    }

    #[test]
    fn app_state_is_thread_safe() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Arc<proxy::AppState>>();
    }
}
