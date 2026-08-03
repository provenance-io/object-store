use std::sync::Arc;

use fastrace::prelude::*;
use tonic_health::{
    ServingStatus,
    pb::health_server::{Health, HealthServer},
    server::{HealthReporter, health_reporter},
};

use crate::{AppContext, datastore::Datastore};

/// If [crate::Config::health_service_enabled] is true, initializes [tonic_health::server::HealthReporter] and starts periodic health check [start_database_health_check]
pub async fn init_health_service(context: &AppContext) -> Option<HealthServer<impl Health>> {
    if context.config.health_service_enabled {
        log::info!("Starting health service...");

        let (health_reporter, health_service) = health_reporter();

        health_reporter
            .set_service_status("", ServingStatus::NotServing)
            .await;

        tokio::spawn(start_datastore_health_check(
            health_reporter.clone(),
            context.datastore.clone(),
        ));

        Some(health_service)
    } else {
        None
    }
}

/// Every two seconds, sets the overall service status based on database connection via [Datastore::health_check]
async fn start_datastore_health_check(mut reporter: HealthReporter, datastore: Arc<dyn Datastore>) {
    log::info!("Starting health status check");

    loop {
        datastore_health_check(&mut reporter, datastore.as_ref())
            .in_span(Span::root("datastore::health_check", SpanContext::random()))
            .await;

        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }
}

async fn datastore_health_check(reporter: &mut HealthReporter, datastore: &dyn Datastore) {
    let status = match datastore.health_check().await {
        Err(err) => {
            log::warn!("Failed to health check the datastore connection {:?}", err);

            ServingStatus::NotServing
        }
        _ => {
            log::trace!("Datastore health check success!");

            ServingStatus::Serving
        }
    };

    reporter.set_service_status("", status).await;
}
