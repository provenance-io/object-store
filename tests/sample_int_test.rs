use std::sync::Arc;

use object_store::{datastore, server::configure_and_start_server, AppContext};
use testcontainers::clients;

use crate::common::config::test_config;
use crate::common::data::party_1;
use crate::common::{containers::start_containers, test_public_key};

mod common;

#[tokio::test]
async fn test() {
    env_logger::init();

    let docker = clients::Cli::default();
    let (db_port, _postgres) = start_containers(&docker).await;
    let config = Arc::new(test_config(db_port));
    let context = AppContext::new(config).await.unwrap();
    context.init().await;

    // TODO:
    // 1. use same steps: env logger, (+infra setup), config, context, sever
    // 2. use same start method as main
    // 3. use for other int tests
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    tokio::spawn(async move {
        tx.send(context.db_pool.clone()).await.unwrap();

        configure_and_start_server(context).await
    });

    let db = rx.recv().await.unwrap();

    let result = datastore::get_all_public_keys(&db).await.unwrap();
    assert_eq!(result.len(), 0);

    datastore::add_public_key(&db, test_public_key(party_1().0.public_key))
        .await
        .unwrap();

    let result = datastore::get_all_public_keys(&db).await.unwrap();
    assert_eq!(result.len(), 1);
}
