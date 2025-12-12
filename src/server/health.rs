use std::sync::Arc;

use fastrace::prelude::*;
use sqlx::PgPool;
use tonic_health::proto::health_server::{Health, HealthServer};

use crate::{AppContext, datastore};

/// If [crate::Config::health_service_enabled] is true, initializes [tonic_health::server::HealthReporter] and starts periodic health check [health_status]
pub async fn init_health_service(
    context: &AppContext,
) -> Option<HealthServer<impl Health + use<>>> {
    if context.config.health_service_enabled {
        log::info!("Starting health service...");

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();

        health_reporter
            .set_service_status("", tonic_health::ServingStatus::NotServing)
            .await;

        tokio::spawn(start_database_health_check(
            health_reporter,
            context.db_pool.clone(),
        ));

        Some(health_service)
    } else {
        None
    }
}

/// Every two seconds, sets the overall service status based on database connection via [datastore::health_check]
async fn start_database_health_check(
    mut reporter: tonic_health::server::HealthReporter,
    db: Arc<PgPool>,
) {
    log::info!("Starting health status check");

    loop {
        health_check_iteration(&mut reporter, &db)
            .in_span(Span::root("database::health_check", SpanContext::random()))
            .await;

        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }
}

async fn health_check_iteration(reporter: &mut tonic_health::server::HealthReporter, db: &PgPool) {
    match datastore::health_check(db).await {
        Err(err) => {
            log::warn!("Failed to health check the database connection {:?}", err);

            reporter
                .set_service_status("", tonic_health::ServingStatus::NotServing)
                .await;
        }
        _ => {
            log::trace!("Database health check success!");

            reporter
                .set_service_status("", tonic_health::ServingStatus::Serving)
                .await;
        }
    }
}
