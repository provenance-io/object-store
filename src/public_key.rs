use crate::cache::Cache;
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
    // This cache is using a std::sync::Mutex because the tokio docs mention that this is often
    // preferrable to the tokio Mutex when you are strictly locking data. In cases where you
    // are locking over a database connection, or io resource, the tokio Mutex is required.
    cache: Arc<Mutex<Cache>>,
    db_pool: Arc<PgPool>,
}

impl PublicKeyGrpc {
    pub fn new(cache: Arc<Mutex<Cache>>, db_pool: Arc<PgPool>) -> Self {
        Self { cache, db_pool }
    }
}

#[tonic::async_trait]
impl PublicKeyService for PublicKeyGrpc {
    async fn add(
        &self,
        request: Request<PublicKeyRequest>,
    ) -> GrpcResult<Response<PublicKeyResponse>> {
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

#[cfg(test)]
mod tests {
    // use base64::{prelude::BASE64_STANDARD, Engine};

    // use crate::proto_helpers::StringUtil;

    #[test]
    fn encode_decode() {
        // let readme_public_key = "BH6YrLjN+I7JzjGCgrIWbfXicg4C4nZaMPwzmTB2Yef/aqxiJmPmpBi1JAonlTzA6c1zU/WX4RKWzAkQBd7lWbU=";

        // from dime impl:
        // let key = std::str::from_utf8(&owner.public_key)
        //
        // from impl PublicKeyApiResponse for PublicKey {
        // fn to_response(self) -> Result<PublicKeyResponse> {
        //     let key_bytes: Vec<u8> = self
        //         .public_key
        //         .decoded()
        //         .map_err(|err| sqlx::Error::Decode(Box::new(err)))?;
        //     let public_key = match self.public_key_type {
        //         KeyType::Secp256k1 => Key::Secp256k1(key_bytes),
        //     };
        //
        // #[trace("mailbox::get")]
        // async fn get(&self, request: Request<GetRequest>) -> GrpcResult<Response<Self::GetStream>> {
        //     let metadata = request.metadata().clone();
        //     let request = request.into_inner();
        //     let public_key = request.public_key.encoded();
        //
        // object put:
        // let public_key = properties
        // .get(consts::SIGNATURE_PUBLIC_KEY_FIELD_NAME)
        // .map(|f| f.encoded())
        //
        // object get, hashrequest:
        // let public_key = request.public_key.encoded();
        //
        // test audience:
        // public_key: BASE64_STANDARD.encode("1").into_bytes()
        //
        // test public key:
        // public_key: std::str::from_utf8(&public_key).unwrap().to_owned(),
        //
        // object test:
        // properties.insert(
        //     SIGNATURE_PUBLIC_KEY_FIELD_NAME.to_owned(),
        //     "signature public key".as_bytes().to_owned(),
        // );
        //
        // replication_object_uuids(
        // &db,
        // String::from_utf8(audience1.public_key).unwrap().as_str()
        //
        //                 replication_object_uuids(&db, audience1.public_key.encoded().as_str(), 50)
        //
        // public key test:
        // assert!(cache
        // .public_keys
        // .contains_key(&vec![1u8, 2u8, 3u8].encoded()));
    }
}
