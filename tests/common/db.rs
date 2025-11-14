use std::sync::Arc;

use object_store::config::Config;
use sqlx::{postgres::PgPoolOptions, PgPool};
use testcontainers::{clients::Cli, images, Container, RunnableImage};

pub async fn start_postgres(docker: &Cli) -> Container<'_, images::postgres::Postgres> {
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);

    container
}

// TODO: wire up whole config
pub async fn setup_postgres(config: &Config) -> Arc<PgPool> {
    println!("Connecting to db at {}", config.db_connection_string());

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(config.db_connection_string().as_ref())
        .await
        .unwrap();

    sqlx::migrate!().run(&pool).await.unwrap();

    Arc::new(pool)
}
