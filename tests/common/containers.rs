use testcontainers_modules::postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// Start all containers needed for integration tests:
/// 1. postgres
///
/// Returns:
/// - data to help configure components in tests
/// - all containers so they don't get dropped until the docker runtime get dropped
pub async fn start_containers() -> (u16, ContainerAsync<postgres::Postgres>) {
    let postgres = postgres::Postgres::default()
        .start()
        .await
        .expect("Postgres container didn't start");

    let db_port = postgres
        .get_host_port_ipv4(5432)
        .await
        .expect("Could not map host port 5432");

    println!("Postgres container started on {}", db_port);

    (db_port, postgres)
}
