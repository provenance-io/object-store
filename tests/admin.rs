mod common;

use object_store::pb::{GetConfigRequest, SetConfigRequest};

use crate::common::client::get_admin_client;
use crate::common::config::test_config;
use crate::common::containers::start_containers;
use crate::common::start_test_server;

#[tokio::test]
async fn get_config_returns_default_maintenance_state() {
    let (db_port, _postgres) = start_containers().await;
    let (_, _, _, config) = start_test_server(test_config(db_port), None).await;

    let mut client = get_admin_client(config.url).await;
    let response = client.get_config(GetConfigRequest {}).await;

    match response {
        Ok(result) => {
            let result = result.into_inner();
            assert!(!result.maintenance_state);
        }
        Err(err) => panic!("Expected success, got error: {:?}", err),
    }
}

#[tokio::test]
async fn set_config_enables_maintenance_state() {
    let (db_port, _postgres) = start_containers().await;
    let (_, _, _, config) = start_test_server(test_config(db_port), None).await;

    let mut client = get_admin_client(config.url).await;
    let response = client
        .set_config(SetConfigRequest {
            maintenance_state: Some(true),
        })
        .await;

    match response {
        Ok(result) => {
            let result = result.into_inner();
            assert!(result.maintenance_state);
        }
        Err(err) => panic!("Expected success, got error: {:?}", err),
    }
}

#[tokio::test]
async fn set_config_disables_maintenance_state() {
    let (db_port, _postgres) = start_containers().await;
    let test_cfg = test_config(db_port);
    // Start with maintenance enabled
    test_cfg.set_maintenance_state(true);
    let (_, _, _, config) = start_test_server(test_cfg, None).await;

    // Verify it's enabled
    let mut client = get_admin_client(config.url).await;
    let response = client.get_config(GetConfigRequest {}).await.unwrap();
    assert!(response.into_inner().maintenance_state);

    // Disable it
    let response = client
        .set_config(SetConfigRequest {
            maintenance_state: Some(false),
        })
        .await;

    match response {
        Ok(result) => {
            let result = result.into_inner();
            assert!(!result.maintenance_state);
        }
        Err(err) => panic!("Expected success, got error: {:?}", err),
    }
}

#[tokio::test]
async fn set_config_without_maintenance_state_preserves_current() {
    let (db_port, _postgres) = start_containers().await;
    let test_cfg = test_config(db_port);
    test_cfg.set_maintenance_state(true);
    let (_, _, _, config) = start_test_server(test_cfg, None).await;

    let mut client = get_admin_client(config.url).await;

    // Send request without maintenance_state field
    let response = client
        .set_config(SetConfigRequest {
            maintenance_state: None,
        })
        .await;

    match response {
        Ok(result) => {
            let result = result.into_inner();
            // Should preserve the current state (true)
            assert!(result.maintenance_state);
        }
        Err(err) => panic!("Expected success, got error: {:?}", err),
    }
}

#[tokio::test]
async fn get_config_reflects_set_config_changes() {
    let (db_port, _postgres) = start_containers().await;
    let (_, _, _, config) = start_test_server(test_config(db_port), None).await;

    let mut client = get_admin_client(config.url).await;

    // Initially false
    let response = client.get_config(GetConfigRequest {}).await.unwrap();
    assert!(!response.into_inner().maintenance_state);

    // Set to true
    client
        .set_config(SetConfigRequest {
            maintenance_state: Some(true),
        })
        .await
        .unwrap();

    // Verify get_config returns true
    let response = client.get_config(GetConfigRequest {}).await.unwrap();
    assert!(response.into_inner().maintenance_state);

    // Set back to false
    client
        .set_config(SetConfigRequest {
            maintenance_state: Some(false),
        })
        .await
        .unwrap();

    // Verify get_config returns false
    let response = client.get_config(GetConfigRequest {}).await.unwrap();
    assert!(!response.into_inner().maintenance_state);
}
