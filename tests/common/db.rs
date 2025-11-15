use std::sync::Arc;

use object_store::config::Config;
use sqlx::{postgres::PgPoolOptions, PgPool};

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
