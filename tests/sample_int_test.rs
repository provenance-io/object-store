use std::sync::Arc;

use object_store::{datastore, server::configure_and_start_server, AppContext};
use testcontainers::clients;

use crate::common::config::test_config;
use crate::common::data::party_1;
use crate::common::{containers::start_containers, test_public_key};

use object_store::types::Result;

mod common;

#[tokio::test]
async fn sample_int_test() -> Result<()> {
    env_logger::init();

    let docker = clients::Cli::default();
    let (db_port, _postgres) = start_containers(&docker).await;
    let config = Arc::new(test_config(db_port));
    let context = AppContext::new(config).await?;
    let db_pool = context.db_pool.clone();

    // TODO:
    // 1. use same steps: env logger, (+infra setup), config, context, sever
    // 2. use same start method as main
    // 3. use for other int tests
    tokio::spawn(async move { configure_and_start_server(context).await });

    let result = datastore::get_all_public_keys(&db_pool).await?;
    assert_eq!(result.len(), 0);

    datastore::add_public_key(&db_pool, test_public_key(party_1().0.public_key)).await?;

    let result = datastore::get_all_public_keys(&db_pool).await?;
    assert_eq!(result.len(), 1);

    Ok(())
}
