mod consts;
mod config;
mod types;
mod dime;
mod datastore;
mod domain;
mod storage;
mod object;
mod public_key;
mod mailbox;

use crate::config::Config;
use crate::object::ObjectGrpc;
use crate::public_key::PublicKeyGrpc;
use crate::mailbox::MailboxGrpc;
use crate::types::{OsError, Result};
use crate::storage::FileSystem;

use std::sync::Arc;
use sqlx::{migrate::Migrator, postgres::PgPoolOptions, Executor};
use tonic::transport::Server;

mod pb {
    tonic::include_proto!("objectstore");
}

use pb::public_key_service_server::PublicKeyServiceServer;
use pb::object_service_server::ObjectServiceServer;
use pb::mailbox_service_server::MailboxServiceServer;

static MIGRATOR: Migrator = sqlx::migrate!();

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let config = Config::new();
    let storage = match config.storage_type.as_str() {
        "file_system" => Ok(FileSystem::new(config.storage_base_path.as_str())),
        _ => Err(OsError::InvalidApplicationState("".to_owned()))
    }?;
    let schema = Arc::new(config.db_schema.clone());
    let pool = PgPoolOptions::new()
        .after_connect(move |conn| {
            let schema = Arc::clone(&schema);
            Box::pin(async move {
                conn.execute(format!("SET search_path = '{}';", &schema).as_ref()).await?;

                Ok(())
            })
        })
        // TODO add more config fields
        .max_connections(config.db_connection_pool_size.into())
        .connect(config.db_connection_string().as_ref())
        .await?;
    let pool = Arc::new(pool);
    let config = Arc::new(config);

    MIGRATOR.run(&*pool).await?;

    let public_key_service = PublicKeyGrpc::new(Arc::clone(&pool));
    let mailbox_service = MailboxGrpc::new(Arc::clone(&pool));
    let object_service = ObjectGrpc::new(Arc::clone(&pool), Arc::clone(&config), storage);

    log::info!("Starting server on {:?}", &config.url);

    Server::builder()
        .add_service(PublicKeyServiceServer::new(public_key_service))
        .add_service(MailboxServiceServer::new(mailbox_service))
        .add_service(ObjectServiceServer::new(object_service))
        .serve(config.url)
        .await?;

    Ok(())
}
