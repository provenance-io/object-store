use crate::datastore;
use crate::types::{GrpcResult, OsError};
use crate::pb::{AckRequest, GetRequest, MailPayload, Uuid };
use crate::pb::mailbox_service_server::MailboxService;

use tokio::sync::mpsc;
use std::{sync::Arc, str::FromStr};
use sqlx::postgres::PgPool;
use tonic::{Request, Response};

#[derive(Debug)]
pub struct MailboxGrpc {
    db_pool: Arc<PgPool>,
}

impl MailboxGrpc {
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self { db_pool: pool }
    }
}

#[tonic::async_trait]
impl MailboxService for MailboxGrpc {

    type GetStream = tokio_stream::wrappers::ReceiverStream<GrpcResult<MailPayload>>;

    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> GrpcResult<Response<Self::GetStream>> {
        let request = request.into_inner();
        let public_key = base64::encode(&request.public_key);
        let (tx, rx) = mpsc::channel(4);
        let results = datastore::stream_mailbox_public_keys(&self.db_pool, &public_key, request.max_results).await?;

        tokio::spawn(async move {
            for (mailbox_uuid, object) in results {
                let payload = match object.payload {
                    Some(payload) => MailPayload {
                        uuid: Some(Uuid { value: mailbox_uuid.to_hyphenated().to_string() }),
                        data: payload,
                    },
                    None => {
                        log::error!("mailbox object without a payload {}", object.uuid.to_hyphenated().to_string());

                        if let Err(_) = tx.send(Err(tonic::Status::new(tonic::Code::Internal, "mailbox object without a payload"))).await {
                            log::debug!("stream closed early");
                        }

                        return;
                    }
                };
                if let Err(_) = tx.send(Ok(payload)).await {
                    log::debug!("stream closed early");
                    return;
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    async fn ack(
        &self,
        request: Request<AckRequest>,
    ) -> GrpcResult<Response<()>> {
        let request = request.into_inner();

        let uuid = if let Some(uuid) = request.uuid {
            uuid::Uuid::from_str(uuid.value.as_str())
                .map_err(Into::<OsError>::into)?
        } else {
            return Err(tonic::Status::new(tonic::Code::InvalidArgument, "must specify uuid"));
        };

        datastore::ack_mailbox_public_key(&self.db_pool, &uuid).await?;

        Ok(Response::new(()))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;

    use crate::consts::*;
    use crate::pb::{self, AckRequest, Audience, GetRequest, mailbox_service_client::MailboxServiceClient};
    use crate::mailbox::*;
    use crate::object::*;
    use crate::object::tests::*;
    use crate::storage::FileSystem;

    use sqlx::postgres::PgPool;
    use tonic::transport::Channel;

    use serial_test::serial;
    use testcontainers::*;

    async fn start_server() -> Arc<PgPool> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        tokio::spawn(async move {
            let config = test_config();
            let url = config.url.clone();
            let docker = clients::Cli::default();
            let image = images::postgres::Postgres::default().with_version(9);
            let container = docker.run(image);
            let pool = setup_postgres(&container).await;
            let pool = Arc::new(pool);
            let storage = FileSystem::new(config.storage_base_path.as_str());
            let mailbox_service = MailboxGrpc { db_pool: pool.clone() };
            let object_service = ObjectGrpc {
                db_pool: pool.clone(),
                config: Arc::new(config),
                storage,
            };

            tx.send(pool.clone()).await.unwrap();

            tonic::transport::Server::builder()
                .add_service(pb::mailbox_service_server::MailboxServiceServer::new(mailbox_service))
                .add_service(pb::object_service_server::ObjectServiceServer::new(object_service))
                .serve(url)
                .await
                .unwrap()
        });

        rx.recv().await.unwrap()
    }

    async fn get_client() -> MailboxServiceClient<Channel> {
        // allow server to start
        tokio::time::sleep(tokio::time::Duration::from_millis(1_000)).await;

        pb::mailbox_service_client::MailboxServiceClient::connect("tcp://0.0.0.0:6789").await.unwrap()
    }

    async fn get_and_ack_helper(client: &mut MailboxServiceClient<Channel>, audience: Audience, expected_size: usize) {
        let public_key = base64::decode(audience.public_key).unwrap();
        let request = GetRequest { public_key, max_results: 50 };
        let response = client.get(request).await;

        match response {
            Ok(response) => {
                let mut response = response.into_inner();
                let mut mail = Vec::new();

                while let Some(msg) = response.message().await.unwrap() {
                    mail.push(msg);
                }

                assert_eq!(mail.len(), expected_size);

                for msg in mail {
                    let request = AckRequest { uuid: msg.uuid };
                    let response = client.ack(request).await;

                    match response {
                        Ok(_) => (),
                        _ => assert_eq!(format!("{:?}", response), ""),
                    }
                }
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn get_and_ack_flow() {
        let db = start_server().await;
        let mut client = get_client().await;

        // post fragment request
        let (audience1, signature1) = party_1();
        let (audience2, signature2) = party_2();
        let (audience3, signature3) = party_3();
        let mut dime = generate_dime(vec![audience1.clone(), audience2.clone(), audience3.clone()], vec![signature1.clone(), signature2.clone(), signature3.clone()]);
        dime.metadata.insert(MAILBOX_KEY.to_owned(), MAILBOX_FRAGMENT_REQUEST.to_owned());
        let payload: bytes::Bytes = "fragment request envelope".as_bytes().into();
        let chunk_size = 500; // full payload in one packet
        let response = put_helper(dime, payload, chunk_size).await;

        match response {
            Ok(response) => {
                let response = response.into_inner();
                let uuid = response.uuid.unwrap().value;
                let uuid = uuid::Uuid::from_str(uuid.as_str()).unwrap();

                assert_eq!(response.name, NOT_STORAGE_BACKED);
                assert_eq!(get_public_keys_by_object(&db, &uuid).await.len(), 3);
                assert_eq!(get_mailbox_keys_by_object(&db, &uuid).await.len(), 2);
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        // ack fragment request
        get_and_ack_helper(&mut client, audience1.clone(), 0).await;
        get_and_ack_helper(&mut client, audience2.clone(), 1).await;
        get_and_ack_helper(&mut client, audience3.clone(), 1).await;

        // post fragment response
        let mut dime = generate_dime(vec![audience3.clone(), audience2.clone(), audience1.clone()], vec![signature3.clone(), signature2.clone(), signature1.clone()]);
        dime.metadata.insert(MAILBOX_KEY.to_owned(), MAILBOX_FRAGMENT_RESPONSE.to_owned());
        let payload: bytes::Bytes = "fragment response envelope".as_bytes().into();
        let chunk_size = 500; // full payload in one packet
        let response = put_helper(dime, payload, chunk_size).await;

        match response {
            Ok(response) => {
                let response = response.into_inner();
                let uuid = response.uuid.unwrap().value;
                let uuid = uuid::Uuid::from_str(uuid.as_str()).unwrap();

                assert_eq!(response.name, NOT_STORAGE_BACKED);
                assert_eq!(get_public_keys_by_object(&db, &uuid).await.len(), 3);
                assert_eq!(get_mailbox_keys_by_object(&db, &uuid).await.len(), 2);
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        // ack fragment response
        get_and_ack_helper(&mut client, audience1.clone(), 1).await;
        get_and_ack_helper(&mut client, audience2.clone(), 1).await;
        get_and_ack_helper(&mut client, audience3.clone(), 0).await;

        // post envelope error
        let mut dime = generate_dime(vec![audience1.clone(), audience2.clone(), audience3.clone()], vec![signature1.clone(), signature2.clone(), signature3.clone()]);
        dime.metadata.insert(MAILBOX_KEY.to_owned(), MAILBOX_ERROR_RESPONSE.to_owned());
        let payload: bytes::Bytes = "error envelope".as_bytes().into();
        let chunk_size = 500; // full payload in one packet
        let response = put_helper(dime, payload, chunk_size).await;

        match response {
            Ok(response) => {
                let response = response.into_inner();
                let uuid = response.uuid.unwrap().value;
                let uuid = uuid::Uuid::from_str(uuid.as_str()).unwrap();

                assert_eq!(response.name, NOT_STORAGE_BACKED);
                assert_eq!(get_public_keys_by_object(&db, &uuid).await.len(), 3);
                assert_eq!(get_mailbox_keys_by_object(&db, &uuid).await.len(), 2);
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        // ack error mail
        get_and_ack_helper(&mut client, audience1.clone(), 0).await;
        get_and_ack_helper(&mut client, audience2.clone(), 1).await;
        get_and_ack_helper(&mut client, audience3.clone(), 1).await;

        // verify no mail left to read
        get_and_ack_helper(&mut client, audience1, 0).await;
        get_and_ack_helper(&mut client, audience2, 0).await;
        get_and_ack_helper(&mut client, audience3, 0).await;
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn duplicate_objects_does_not_dup_mail() {
        start_server().await;
        let mut client = get_client().await;

        let (audience1, signature1) = party_1();
        let (audience2, signature2) = party_2();
        let mut dime = generate_dime(vec![audience1, audience2.clone()], vec![signature1, signature2]);
        dime.metadata.insert(MAILBOX_KEY.to_owned(), MAILBOX_FRAGMENT_REQUEST.to_owned());
        let payload: bytes::Bytes = "testing small payload".as_bytes().into();
        let chunk_size = 500; // full payload in one packet

        for _ in 0..10 {
            let response = put_helper(dime.clone(), payload.clone(), chunk_size).await;

            match response {
                Ok(_) => (),
                _ => assert_eq!(format!("{:?}", response), ""),
            }
        }

        dime.metadata.insert(MAILBOX_KEY.to_owned(), MAILBOX_ERROR_RESPONSE.to_owned());

        for _ in 0..10 {
            let response = put_helper(dime.clone(), payload.clone(), chunk_size).await;

            match response {
                Ok(_) => (),
                _ => assert_eq!(format!("{:?}", response), ""),
            }
        }

        get_and_ack_helper(&mut client, audience2, 1).await;
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn get_and_ack_many() {
        start_server().await;
        let mut client = get_client().await;

        let (audience1, signature1) = party_1();
        let (audience2, signature2) = party_2();
        let mut dime = generate_dime(vec![audience1, audience2.clone()], vec![signature1, signature2]);
        dime.metadata.insert(MAILBOX_KEY.to_owned(), MAILBOX_FRAGMENT_REQUEST.to_owned());
        let chunk_size = 500; // full payload in one packet

        for _ in 0..10 {
            let payload: bytes::Bytes = uuid::Uuid::new_v4().to_hyphenated().to_string().into_bytes().into();
            let response = put_helper(dime.clone(), payload, chunk_size).await;

            match response {
                Ok(_) => (),
                _ => assert_eq!(format!("{:?}", response), ""),
            }
        }

        dime.metadata.insert(MAILBOX_KEY.to_owned(), MAILBOX_ERROR_RESPONSE.to_owned());

        for _ in 0..10 {
            let payload: bytes::Bytes = uuid::Uuid::new_v4().to_hyphenated().to_string().into_bytes().into();
            let response = put_helper(dime.clone(), payload.clone(), chunk_size).await;

            match response {
                Ok(_) => (),
                _ => assert_eq!(format!("{:?}", response), ""),
            }
        }

        get_and_ack_helper(&mut client, audience2.clone(), 20).await;
        get_and_ack_helper(&mut client, audience2, 0).await;
    }
}
