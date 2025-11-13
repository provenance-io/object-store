use std::sync::{Arc, Mutex};

use object_store::{
    cache::Cache,
    datastore::get_all_public_keys,
    db::connect_and_migrate,
    mailbox::MailboxGrpc,
    object::ObjectGrpc,
    public_key::PublicKeyGrpc,
    server::{configure_and_start_server, init_health_service},
    storage::FileSystem,
};
use testcontainers::{clients, images, RunnableImage};

use crate::common::test_config;

mod common;

#[tokio::test]
async fn test() {
    let docker = clients::Cli::default();
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);
    let postgres_port = container.get_host_port_ipv4(5432);

    let config = Arc::new(test_config(postgres_port));

    let storage = FileSystem::new(std::env::temp_dir());

    let pool = connect_and_migrate(config.clone()).await.unwrap();

    let cache = Arc::new(Mutex::new(Cache::default()));

    let public_key_service = PublicKeyGrpc::new(cache.clone(), pool.clone());
    let mailbox_service = MailboxGrpc::new(cache.clone(), config.clone(), pool.clone());
    let object_service = ObjectGrpc {
        cache,
        config: config.clone(),
        db_pool: pool.clone(),
        storage: Arc::new(Box::new(storage)),
    };

    let health_service = init_health_service(pool.clone()).await;

    let (tx, mut rx) = tokio::sync::mpsc::channel(1);

    tokio::spawn(async move {
        tx.send(pool.clone()).await.unwrap();

        configure_and_start_server(
            config,
            health_service,
            public_key_service,
            mailbox_service,
            object_service,
        )
        .await
    });

    let db = rx.recv().await.unwrap();

    let result = get_all_public_keys(&db).await.unwrap();

    println!("result: {}", result.len());
}
