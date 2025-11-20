use std::sync::Arc;

use sqlx::{postgres::PgPoolOptions, Error, Executor, PgPool};

use crate::config::Config;

/// 1. Creates [PgPool] with default schema of [Config::db_schema]
/// 2. Connects to [Config::db_connection_string]
/// 3. Migrates database
pub async fn connect_and_migrate(config: &Config) -> Result<Arc<PgPool>, Error> {
    let schema = config.db_schema.clone();

    let pool = PgPoolOptions::new()
        .after_connect(move |conn, _meta| {
            let schema = schema.clone();
            Box::pin(async move {
                conn.execute(format!("SET search_path = '{}';", schema).as_ref())
                    .await?;

                Ok(())
            })
        })
        .max_connections(config.db_connection_pool_size.into())
        .connect(config.db_connection_string().as_ref())
        .await?;

    log::debug!("Running migrations");

    sqlx::migrate!().run(&pool).await?;

    Ok(Arc::new(pool))
}
