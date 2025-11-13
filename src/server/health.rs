use std::sync::Arc;

use sqlx::PgPool;
use tonic_health::proto::health_server::{Health, HealthServer};

use crate::{datastore, AppContext};

pub async fn init_health_service(context: &AppContext) -> Option<HealthServer<impl Health>> {
    if context.config.health_service_enabled {
        log::info!("Starting health service...");

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();

        health_reporter
            .set_service_status("", tonic_health::ServingStatus::NotServing)
            .await;
        tokio::spawn(health_status(health_reporter, context.db_pool.clone()));

        Some(health_service)
    } else {
        None
    }
}

async fn health_status(mut reporter: tonic_health::server::HealthReporter, db: Arc<PgPool>) {
    log::info!("Starting health status check");

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        if let Err(err) = datastore::health_check(&db).await {
            log::warn!("Failed to health check the database connection {:?}", err);

            reporter
                .set_service_status("", tonic_health::ServingStatus::NotServing)
                .await;
        } else {
            log::trace!("Database health check success!");

            reporter
                .set_service_status("", tonic_health::ServingStatus::Serving)
                .await;
        }
    }
}
