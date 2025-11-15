use std::sync::Arc;

use object_store::{
    datastore::{self, get_all_public_keys},
    server::configure_and_start_server,
    AppContext,
};
use testcontainers::clients;

use crate::common::{db::start_containers, party_1, test_config, test_public_key};

pub mod common;

#[tokio::test]
async fn test() {
    env_logger::init();

    let docker = clients::Cli::default();
    let postgres = start_containers(&docker).await;
    let db_port = postgres.get_host_port_ipv4(5432);
    println!("Postgres postgres started on {}", db_port);
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

    let result = get_all_public_keys(&db).await.unwrap();
    assert_eq!(result.len(), 0);

    datastore::add_public_key(&db, test_public_key(party_1().0.public_key))
        .await
        .unwrap();

    let result = get_all_public_keys(&db).await.unwrap();
    assert_eq!(result.len(), 1);
}
