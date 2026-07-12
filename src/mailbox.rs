use crate::datastore;
use crate::pb::mailbox_service_server::MailboxService;
use crate::pb::{AckRequest, GetRequest, MailPayload};
use crate::proto_helpers::VecUtil;
use crate::types::{GrpcResult, OsError};
use crate::{
    cache::{Cache, PublicKeyState},
    config::Config,
};

use fastrace_macro::trace;
use sqlx::postgres::PgPool;
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

// TODO write test to not mailbox remote_keys

#[derive(Debug)]
pub struct MailboxGrpc {
    cache: Arc<Mutex<Cache>>,
    config: Arc<Config>,
    db_pool: Arc<PgPool>,
}

impl MailboxGrpc {
    pub fn new(cache: Arc<Mutex<Cache>>, config: Arc<Config>, db_pool: Arc<PgPool>) -> Self {
        Self {
            cache,
            config,
            db_pool,
        }
    }
}

#[tonic::async_trait]
impl MailboxService for MailboxGrpc {
    type GetStream = tokio_stream::wrappers::ReceiverStream<GrpcResult<MailPayload>>;

    #[trace(name = "mailbox::get")]
    async fn get(&self, request: Request<GetRequest>) -> GrpcResult<Response<Self::GetStream>> {
        let metadata = request.metadata().clone();
        let request = request.into_inner();
        let public_key = request.public_key.encoded();

        if self.config.user_auth_enabled {
            let cache = self.cache.lock().unwrap();

            match cache.get_public_key_state(&public_key) {
                PublicKeyState::Local => {
                    if let Some(cached_key) = cache.public_keys.get(&public_key) {
                        cached_key.auth()?.authorize(&metadata)
                    } else {
                        Err(Status::internal(
                            "this should not happen - key was resolved to Local state and then could not be fetched",
                        ))
                    }
                }
                PublicKeyState::Remote => Err(Status::permission_denied(format!(
                    "remote public key {} - fetch on its own instance",
                    &public_key
                ))),
                PublicKeyState::Unknown => Err(Status::permission_denied(format!(
                    "unknown public key {}",
                    &public_key
                ))),
            }?;
        }

        let (tx, rx) = mpsc::channel(4);
        let results =
            datastore::stream_mailbox_public_keys(&self.db_pool, &public_key, request.max_results)
                .await?;

        tokio::spawn(async move {
            for (mailbox_uuid, object) in results {
                let payload = match object.payload {
                    Some(payload) => MailPayload {
                        uuid: Some(mailbox_uuid.into()),
                        data: payload,
                    },
                    None => {
                        log::error!(
                            "mailbox object without a payload: uuid={} hash={} dime_length={}",
                            object.uuid.as_hyphenated(),
                            object.hash,
                            object.dime_length,
                        );

                        if tx
                            .send(Err(tonic::Status::new(
                                tonic::Code::Internal,
                                "mailbox object without a payload",
                            )))
                            .await
                            .is_err()
                        {
                            log::debug!("stream closed early");
                        }

                        return;
                    }
                };
                if tx.send(Ok(payload)).await.is_err() {
                    log::debug!("stream closed early");
                    return;
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    #[trace(name = "mailbox::ack")]
    async fn ack(&self, request: Request<AckRequest>) -> GrpcResult<Response<()>> {
        if self.config.is_maintenance_state() {
            return Err(Status::unavailable("Service is in maintenance mode"));
        }

        let metadata = request.metadata().clone();
        let request = request.into_inner();

        let uuid = if let Some(uuid) = &request.uuid {
            uuid::Uuid::from_str(uuid.value.as_str()).map_err(Into::<OsError>::into)?
        } else {
            return Err(tonic::Status::new(
                tonic::Code::InvalidArgument,
                "must specify uuid",
            ));
        };

        let public_key = if request.public_key.is_empty() {
            None
        } else {
            Some(request.public_key.encoded())
        };

        if self.config.user_auth_enabled {
            let public_key = public_key.clone().ok_or(Status::permission_denied(
                "auth is enabled, but a public_key wasn't sent",
            ))?;
            let cache = self.cache.lock().unwrap();

            match cache.get_public_key_state(&public_key) {
                PublicKeyState::Local => {
                    if let Some(cached_key) = cache.public_keys.get(&public_key) {
                        cached_key.auth()?.authorize(&metadata)
                    } else {
                        Err(Status::internal(
                            "this should not happen - key was resolved to Local state and then could not be fetched",
                        ))
                    }
                }
                PublicKeyState::Remote => Err(Status::permission_denied(format!(
                    "remote public key {} - ack on its own instance",
                    &public_key
                ))),
                PublicKeyState::Unknown => Err(Status::permission_denied(format!(
                    "unknown public key {}",
                    &public_key
                ))),
            }?;
        }

        datastore::ack_mailbox_public_key(&self.db_pool, &uuid, &public_key).await?;

        Ok(Response::new(()))
    }
}
