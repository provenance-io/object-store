use std::net::SocketAddr;

use object_store::pb::{
    admin_service_client::AdminServiceClient, mailbox_service_client::MailboxServiceClient,
    object_service_client::ObjectServiceClient,
};
use tonic::transport::Channel;

pub async fn get_admin_client(addr: SocketAddr) -> AdminServiceClient<Channel> {
    AdminServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap()
}

pub async fn get_mailbox_client(addr: SocketAddr) -> MailboxServiceClient<Channel> {
    MailboxServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap()
}

pub async fn get_object_client(addr: SocketAddr) -> ObjectServiceClient<Channel> {
    ObjectServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap()
}
