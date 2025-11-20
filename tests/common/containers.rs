use testcontainers::{clients::Cli, images, Container, RunnableImage};

/// Start all containers needed for integration tests:
/// 1. postgres
///
/// Returns:
/// - data to help configure components in tests
/// - all containers so they don't get dropped until the docker runtime get dropped
pub async fn start_containers(docker: &Cli) -> (u16, Container<'_, images::postgres::Postgres>) {
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let postgres = docker.run(image);
    let db_port = postgres.get_host_port_ipv4(5432);
    println!("Postgres container started on {}", db_port);

    (db_port, postgres)
}
