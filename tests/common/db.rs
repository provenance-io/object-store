use std::sync::Arc;

use sqlx::{postgres::PgPoolOptions, PgPool};
use testcontainers::{clients::Cli, images, Container, RunnableImage};

pub async fn start_postgres(docker: &Cli) -> Container<'_, images::postgres::Postgres> {
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);

    container
}

// TODO: from public_key test, replace with config
pub async fn setup_postgres(container: Container<'_, images::postgres::Postgres>) -> Arc<PgPool> {
    let connection_string = &format!(
        "postgres://postgres:postgres@localhost:{}/postgres",
        container.get_host_port_ipv4(5432),
    );

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(connection_string)
        .await
        .unwrap();

    sqlx::migrate!().run(&pool).await.unwrap();

    Arc::new(pool)
}
