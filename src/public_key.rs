use crate::cache::Cache;
use crate::config::Config;
use crate::datastore;
use crate::domain::PublicKeyApiResponse;
use crate::pb::public_key_request::Impl::HeaderAuth as HeaderAuthEnumRequest;
use crate::pb::public_key_service_server::PublicKeyService;
use crate::pb::{PublicKeyRequest, PublicKeyResponse};
use crate::types::GrpcResult;

use sqlx::postgres::PgPool;
use std::convert::TryInto;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};
use url::Url;

// TODO background process that updates cache on occasion

#[derive(Debug)]
pub struct PublicKeyGrpc {
    /// This cache is using a std::sync::Mutex because the tokio docs mention that this is often
    /// preferrable to the tokio Mutex when you are strictly locking data. In cases where you
    /// are locking over a database connection, or io resource, the tokio Mutex is required.
    cache: Arc<Mutex<Cache>>,
    config: Arc<Config>,
    db_pool: Arc<PgPool>,
}

impl PublicKeyGrpc {
    pub fn new(cache: Arc<Mutex<Cache>>, config: Arc<Config>, db_pool: Arc<PgPool>) -> Self {
        Self {
            cache,
            config,
            db_pool,
        }
    }
}

#[tonic::async_trait]
impl PublicKeyService for PublicKeyGrpc {
    async fn add(
        &self,
        request: Request<PublicKeyRequest>,
    ) -> GrpcResult<Response<PublicKeyResponse>> {
        if self.config.is_maintenance_state() {
            return Err(Status::unavailable("Service is in maintenance mode"));
        }

        let request = request.into_inner();

        // validate public_key
        if let Some(ref public_key) = request.public_key {
            if public_key.key.is_none() {
                return Err(Status::invalid_argument("must specify key type"));
            }
        } else {
            return Err(Status::invalid_argument("must specify public key"));
        }

        // validate auth methods
        if let Some(HeaderAuthEnumRequest(ref auth)) = request.r#impl {
            if auth.header.trim().is_empty() {
                return Err(Status::invalid_argument(
                    "must specify non empty auth header",
                ));
            }
            if auth.value.trim().is_empty() {
                return Err(Status::invalid_argument(
                    "must specify non empty auth value",
                ));
            }
        };

        // validate url if it is not empty
        if !request.url.is_empty() {
            Url::parse(&request.url).map_err(|e| {
                Status::invalid_argument(format!("Unable to parse url {} - {:?}", &request.url, e))
            })?;
        }

        let key = datastore::add_public_key(&self.db_pool, request.try_into()?).await?;
        let response = key.clone().to_response()?;

        {
            let mut cache = self.cache.lock().unwrap();

            cache.add_public_key(key);
        }

        Ok(Response::new(response))
    }
}
