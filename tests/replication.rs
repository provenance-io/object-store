mod common;

use object_store::config::Config;
use object_store::datastore::{PublicKey, replication_object_uuids};
use object_store::pb;
use object_store::proto_helpers::AudienceUtil;
use object_store::types::Result;

use std::collections::HashMap;

use futures::StreamExt;
use sqlx::postgres::PgPool;

use crate::common::client::get_object_client;
use crate::common::config::{test_config_no_replication, test_config_replication};
use crate::common::containers::start_containers;
use crate::common::data::{generate_dime, party_1, party_2, party_3, test_public_key};
use crate::common::{hash, put_helper, start_test_server};

/// Get count of ALL objects
async fn get_object_count(db: &PgPool) -> i64 {
    let row: (i64,) = sqlx::query_as("SELECT count(*) as count FROM object")
        .fetch_one(db)
        .await
        .unwrap();

    row.0
}

#[tokio::test]
async fn client_caching() -> Result<()> {
    let (db_port_one, _postgres_one) = start_containers().await;
    let config_one = test_config_replication(db_port_one);

    let (db_port_two, _postgres_two) = start_containers().await;
    let config_two = test_config_replication(db_port_two);

    let (_, _, replication_state_one, config_one) =
        start_test_server(config_one, Some(&config_two)).await;

    let url_one = format!("http://{}", config_one.url);

    let mut client_cache = replication_state_one.client_cache.lock().await;

    // Client 1:
    // Check that ReplicationState connection caching works when given the same URL:

    let client_one = client_cache.request(&url_one).await.unwrap().unwrap();
    let client_one_id_first = *client_one.id();

    // the result from calling again with the same URL should be empty since the client is in use
    assert!(client_cache.request(&url_one).await.unwrap().is_none());

    // When restored, then request should get an instance again
    client_cache.restore(&url_one, client_one).await.unwrap();
    let client_one = client_cache.request(&url_one).await.unwrap().unwrap();
    let client_one_id_second = *client_one.id();

    // The IDs of both clients should be the same since the URL is the same
    assert_eq!(client_one_id_first, client_one_id_second);

    // Client 2:
    let (_, _, _, config_two) = start_test_server(config_two, None).await;
    let url_two = format!("http://{}", config_two.url);

    let client_two = client_cache.request(&url_two).await.unwrap().unwrap();

    // Clients should be distinct
    assert_ne!(client_one_id_first, client_two.id().clone());

    Ok(())
}

#[tokio::test]
async fn replication_can_be_disabled() -> Result<()> {
    let (db_port, _postgres) = start_containers().await;

    let config = test_config_no_replication(db_port);
    let (db_pool, _, _, config_one) = start_test_server(config, None).await;

    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let (audience3, signature3) = party_3();

    // put 3 objects for party_1, party_2, party_3
    let dime = generate_dime(
        vec![audience1.clone(), audience2.clone(), audience3.clone()],
        vec![signature1.clone(), signature2.clone(), signature3.clone()],
    );
    let payload: bytes::Bytes = "testing small payload 2".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let request = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );

    let mut os_client = get_object_client(config_one.url).await;
    let response = os_client.put(request).await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let dime = generate_dime(
        vec![audience1.clone(), audience2.clone(), audience3.clone()],
        vec![signature1.clone(), signature2.clone(), signature3.clone()],
    );
    let payload: bytes::Bytes = "testing small payload 3".as_bytes().into();
    let request = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );

    let response = os_client.put(request).await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let dime = generate_dime(
        vec![audience1.clone(), audience2.clone(), audience3.clone()],
        vec![signature1.clone(), signature2.clone(), signature3.clone()],
    );
    let payload: bytes::Bytes = "testing small payload 4".as_bytes().into();
    let request = put_helper(
        dime,
        payload.clone(),
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );

    let response = os_client.put(request).await;

    match response {
        Ok(_) => {
            let objects1 = replication_object_uuids(&db_pool, &audience1.public_key(), 50).await?;
            let objects2 = replication_object_uuids(&db_pool, &audience2.public_key(), 50).await?;
            let objects3 = replication_object_uuids(&db_pool, &audience3.public_key(), 50).await?;

            assert_eq!(objects1.len(), 0);
            assert_eq!(objects2.len(), 0);
            assert_eq!(objects3.len(), 0);
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    Ok(())
}

#[tokio::test]
async fn end_to_end_replication() -> Result<()> {
    let (db_port_one, _postgres_one) = start_containers().await;
    let config_one = test_config_replication(db_port_one);

    let (db_port_two, _postgres_two) = start_containers().await;
    let config_two = Config {
        url: "0.0.0.0:6790".parse().unwrap(), // hardcoded - make sure it's different in other tests with replication
        ..test_config_replication(db_port_two)
    };

    let (db_pool_one, _, mut state_one, config_one) =
        start_test_server(config_one, Some(&config_two)).await;

    let (db_pool_two, _, _, config_two) = start_test_server(config_two, None).await;

    let mut client_one = get_object_client(config_one.url).await;

    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let (audience3, signature3) = party_3();

    let payload4 = "testing small payload 4".as_bytes();

    // put object for party_1 - requires no replication
    {
        let dime = generate_dime(vec![audience1.clone()], vec![signature1.clone()]);
        let payload: bytes::Bytes = "testing small payload 1".as_bytes().into();
        let chunk_size = 500; // full payload in one packet
        let request = put_helper(
            dime,
            payload,
            chunk_size,
            HashMap::default(),
            Vec::default(),
        );

        let response = client_one.put(request).await;

        match response {
            Ok(_) => {
                let objects1 =
                    replication_object_uuids(&db_pool_one, &audience1.public_key(), 50).await?;

                assert_eq!(objects1.len(), 0);
            }
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    // put 3 objects for party_1, party_2, party_3
    {
        let dime = generate_dime(
            vec![audience1.clone(), audience2.clone(), audience3.clone()],
            vec![signature1.clone(), signature2.clone(), signature3.clone()],
        );
        let payload: bytes::Bytes = "testing small payload 2".as_bytes().into();
        let chunk_size = 500; // full payload in one packet
        let request = put_helper(
            dime,
            payload,
            chunk_size,
            HashMap::default(),
            Vec::default(),
        );
        let response = client_one.put(request).await;

        // TODO remove theses matches - hard to read and could let undefined states fall through
        match response {
            Ok(_) => (),
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        let dime = generate_dime(
            vec![audience1.clone(), audience2.clone(), audience3.clone()],
            vec![signature1.clone(), signature2.clone(), signature3.clone()],
        );
        let payload: bytes::Bytes = "testing small payload 3".as_bytes().into();
        let request = put_helper(
            dime,
            payload,
            chunk_size,
            HashMap::default(),
            Vec::default(),
        );
        let response = client_one.put(request).await;

        match response {
            Ok(_) => (),
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        let dime = generate_dime(
            vec![audience1.clone(), audience2.clone(), audience3.clone()],
            vec![signature1.clone(), signature2.clone(), signature3.clone()],
        );

        let request = put_helper(
            dime,
            payload4.into(),
            chunk_size,
            HashMap::default(),
            Vec::default(),
        );

        let response = client_one.put(request).await;

        match response {
            Ok(_) => {
                let objects1 =
                    replication_object_uuids(&db_pool_one, &audience1.public_key(), 50).await?;
                let objects2 =
                    replication_object_uuids(&db_pool_one, &audience2.public_key(), 50).await?;
                let objects3 =
                    replication_object_uuids(&db_pool_one, &audience3.public_key(), 50).await?;

                assert_eq!(objects1.len(), 0);
                assert_eq!(objects2.len(), 3);
                assert_eq!(objects3.len(), 3);

                let objects1 =
                    replication_object_uuids(&db_pool_two, &audience1.public_key(), 50).await?;
                let objects2 =
                    replication_object_uuids(&db_pool_two, &audience2.public_key(), 50).await?;
                let objects3 =
                    replication_object_uuids(&db_pool_two, &audience3.public_key(), 50).await?;

                assert_eq!(objects1.len(), 0);
                assert_eq!(objects2.len(), 0);
                assert_eq!(objects3.len(), 0);
            }
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    // run replication iteration
    // Check source server for correct counts: batch size 2, cache seeded with remote key for party_2 => (0,1,3)
    // Verify remote server is all 0
    state_one.replicate_iteration().await;
    {
        let objects1 = replication_object_uuids(&db_pool_one, &audience1.public_key(), 50).await?;
        let objects2 = replication_object_uuids(&db_pool_one, &audience2.public_key(), 50).await?;
        let objects3 = replication_object_uuids(&db_pool_one, &audience3.public_key(), 50).await?;

        assert_eq!(objects1.len(), 0);
        assert_eq!(objects2.len(), 1);
        assert_eq!(objects3.len(), 3);

        let objects1 = replication_object_uuids(&db_pool_two, &audience1.public_key(), 50).await?;
        let objects2 = replication_object_uuids(&db_pool_two, &audience2.public_key(), 50).await?;
        let objects3 = replication_object_uuids(&db_pool_two, &audience3.public_key(), 50).await?;

        assert_eq!(objects1.len(), 0);
        assert_eq!(objects2.len(), 0);
        assert_eq!(objects3.len(), 0);
    }

    // Run again
    // Check source server for correct counts: batch size 2, cache seeded with remote key for party_2 => (0,0,3)
    state_one.replicate_iteration().await;
    {
        let objects1 = replication_object_uuids(&db_pool_one, &audience1.public_key(), 50).await?;
        let objects2 = replication_object_uuids(&db_pool_one, &audience2.public_key(), 50).await?;
        let objects3 = replication_object_uuids(&db_pool_one, &audience3.public_key(), 50).await?;

        assert_eq!(objects1.len(), 0);
        assert_eq!(objects2.len(), 0);
        assert_eq!(objects3.len(), 3);

        let objects1 = replication_object_uuids(&db_pool_two, &audience1.public_key(), 50).await?;
        let objects2 = replication_object_uuids(&db_pool_two, &audience2.public_key(), 50).await?;
        let objects3 = replication_object_uuids(&db_pool_two, &audience3.public_key(), 50).await?;

        assert_eq!(objects1.len(), 0);
        assert_eq!(objects2.len(), 0);
        assert_eq!(objects3.len(), 0);
    }

    // verify db on remote instance to check for 3 objects for party_2
    assert_eq!(get_object_count(&db_pool_two).await, 3);

    // pull one object from local instance and verify all rows against the same one that was replicated to the remote
    {
        let mut client_two = get_object_client(config_two.url).await;

        let public_key = audience3.public_key_decoded();
        let request = pb::HashRequest {
            hash: hash(payload4.into()),
            public_key,
        };
        let response_one = client_one.get(tonic::Request::new(request.clone())).await;
        let response_two = client_two.get(tonic::Request::new(request)).await;

        match response_one {
            Ok(response_one) => match response_two {
                Ok(response_two) => {
                    let response_one = response_one.into_inner();
                    let response_two = response_two.into_inner();

                    if let Some((one, two)) = response_one.zip(response_two).next().await {
                        assert_eq!(one.unwrap(), two.unwrap());
                    }
                }
                _ => assert_eq!(format!("{:?}", response_two), ""),
            },
            _ => assert_eq!(format!("{:?}", response_one), ""),
        }
    }

    Ok(())
}

#[ignore = "unimplemented"]
#[tokio::test]
async fn late_remote_url_can_replicate() {}

#[tokio::test]
async fn late_local_url_can_cleanup() -> Result<()> {
    let (db_port, _postgres) = start_containers().await;
    let config = test_config_replication(db_port);

    let (db_pool, cache, replication_state, config) = start_test_server(config, None).await;

    let (audience1, signature1) = party_1();
    let (audience3, signature3) = party_3();

    // put 3 objects for party_1, party_3
    let dime = generate_dime(
        vec![audience1.clone(), audience3.clone()],
        vec![signature1.clone(), signature3.clone()],
    );
    let payload: bytes::Bytes = "testing small payload 2".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let request = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );

    let mut os_client = get_object_client(config.url).await;
    let response = os_client.put(request).await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let dime = generate_dime(
        vec![audience1.clone(), audience3.clone()],
        vec![signature1.clone(), signature3.clone()],
    );
    let payload: bytes::Bytes = "testing small payload 3".as_bytes().into();
    let request = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );

    let response = os_client.put(request).await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let dime = generate_dime(
        vec![audience1.clone(), audience3.clone()],
        vec![signature1.clone(), signature3.clone()],
    );
    let payload: bytes::Bytes = "testing small payload 4".as_bytes().into();
    let request = put_helper(
        dime,
        payload.clone(),
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );

    let response = os_client.put(request).await;

    match response {
        Ok(_) => {
            let objects1 = replication_object_uuids(&db_pool, &audience1.public_key(), 50).await?;
            let objects3 = replication_object_uuids(&db_pool, &audience3.public_key(), 50).await?;

            assert_eq!(objects1.len(), 0);
            assert_eq!(objects3.len(), 3);
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    // set unknown key to local and reap
    {
        let mut cache = cache.lock().unwrap();

        cache.add_public_key(PublicKey {
            auth_data: Some(String::from("X-Test-Header:test_value")),
            ..test_public_key(audience3.public_key.clone())
        });
    }

    replication_state.reap_unknown_keys_iteration().await;

    let objects1 = replication_object_uuids(&db_pool, &audience1.public_key(), 50).await?;
    let objects3 = replication_object_uuids(&db_pool, &audience3.public_key(), 50).await?;

    assert_eq!(objects1.len(), 0);
    assert_eq!(objects3.len(), 0);

    Ok(())
}

#[ignore = "unimplemented"]
#[tokio::test]
async fn handles_offline_remote() {}
