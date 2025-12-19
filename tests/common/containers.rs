use testcontainers_modules::postgres;
use testcontainers_modules::testcontainers::{Container, RunnableImage, clients::Cli};

/// Start all containers needed for integration tests:
/// 1. postgres
///
/// Returns:
/// - data to help configure components in tests
/// - all containers so they don't get dropped until the docker runtime get dropped
pub async fn start_containers(docker: &Cli) -> (u16, Container<'_, postgres::Postgres>) {
    let image = RunnableImage::from(postgres::Postgres::default()).with_tag("14-alpine");
    let postgres = docker.run(image);
    let db_port = postgres.get_host_port_ipv4(5432);
    println!("Postgres container started on {}", db_port);

    (db_port, postgres)
}
