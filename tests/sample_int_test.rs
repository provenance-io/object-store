use std::sync::Arc;

use object_store::{
    datastore::{self, get_all_public_keys},
    server::configure_and_start_server,
    AppContext,
};
use testcontainers::clients;

use crate::common::{db::start_postgres, party_1, test_config, test_public_key};

pub mod common;

#[tokio::test]
async fn test() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let config = Arc::new(test_config(container.get_host_port_ipv4(5432)));
    println!("Postgres container started on {}", config.db_port);

    let app_context = AppContext::new(config).await.unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::channel(1);

    tokio::spawn(async move {
        tx.send(app_context.db_pool.clone()).await.unwrap();

        configure_and_start_server(app_context).await
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
