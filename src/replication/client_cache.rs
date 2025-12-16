use std::cmp::min;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::mem;
use std::ops::{Deref, DerefMut};

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::pb::object_service_client::ObjectServiceClient;
use crate::replication::error::{ReplicationError, Result};

#[derive(Debug)]
pub struct ClientCache {
    clients: HashMap<String, CachedClient<ID<ObjectServiceClient<tonic::transport::Channel>>>>,
    backoff_min_wait: i64,
    backoff_max_wait: i64,
}

impl ClientCache {
    pub fn new(backoff_min_wait: i64, backoff_max_wait: i64) -> Self {
        Self {
            clients: HashMap::new(),
            backoff_min_wait,
            backoff_max_wait,
        }
    }

    /// Request a service client
    ///
    /// This assumes a one-to-one exclusive mapping between a url and an instance
    /// of `ObjectServiceClient`. Calls to `request` should be paired
    /// with an accompanying call to `restore`, which returns the client
    /// instance to the `clients` map.
    ///
    /// Potential TODO: expire clients beyond a certain age to force reconnection.
    pub async fn request(
        &mut self,
        url: &str,
    ) -> Result<Option<ID<ObjectServiceClient<tonic::transport::Channel>>>> {
        let error_url = url.to_owned();
        let min_wait_time = self.backoff_min_wait;
        let max_wait_time = self.backoff_max_wait;

        match self.clients.entry(url.to_owned()) {
            // A client was already marked as being created for the given url.
            // If it was returned via `restore`, take the client instance from
            // the cache entry and return it immediately. Otherwise, try to create a
            // new instance. If that fails, timestamp the attempt and increase the wait period.
            Entry::Occupied(mut entry) => {
                let cache_entry = entry.get_mut();
                match cache_entry.take() {
                    Some(client) => Ok(Some(client)),
                    None => {
                        // if enough time has elapsed, we can try to create another client:
                        if cache_entry.ready_to_try() {
                            match ObjectServiceClient::connect(url.to_owned()).await {
                                Ok(client) => {
                                    // mark the client in the entry as used and return it:
                                    cache_entry.used();
                                    Ok(Some(ID::new(client)))
                                }
                                Err(e) => {
                                    log::error!(
                                        "Failed to create client for {} - {:?}",
                                        error_url,
                                        e
                                    );
                                    cache_entry.wait_longer();
                                    // not ready, wait more:
                                    Ok(None)
                                }
                            }
                        } else {
                            Ok(None) // still waiting or the client is in use
                        }
                    }
                }
            }
            // If no client is present, attempt to create a new instance and
            // a new cache entry. If the client was successfully created,
            // return it immediately, otherwise record the attempt and
            // signal the initial waiting period should begin before attempting
            // to create a client again.
            Entry::Vacant(entry) => match ObjectServiceClient::connect(url.to_owned()).await {
                Ok(client) => {
                    entry.insert(CachedClient::using());
                    Ok(Some(ID::new(client)))
                }
                Err(e) => {
                    log::error!("Failed to create client for {} - {:?}", error_url, e);
                    entry.insert(CachedClient::waiting(min_wait_time, max_wait_time));
                    Ok(None)
                }
            },
        }
    }

    /// Return/restore a service client
    ///
    /// The corresponding function to `request` that returns a client
    /// to the `clients` map so it can be reused later.
    pub async fn restore(
        &mut self,
        url: &str,
        client: ID<ObjectServiceClient<tonic::transport::Channel>>,
    ) -> Result<()> {
        match self.clients.entry(url.to_owned()) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().replace(client);
                Ok(())
            }
            // Attempting to place a client into the map without having gone through
            // `request` is an error:
            Entry::Vacant(_) => Err(ReplicationError::ClientCacheError(url.to_owned())),
        }
    }
}

/// `ClientState` is used to describe the state  a `CacheEntry` can be in at
/// a given time:
///
/// - it is waiting to attempt to create a new service client instance
/// - a client has been created, but taken
/// - the client is present in the map
#[derive(Debug)]
enum ClientState<T> {
    Using,
    Waiting(DateTime<Utc>, i64, i64),
    Present(T),
}

#[derive(Debug)]
struct CachedClient<T> {
    state: ClientState<T>,
}

impl<T> CachedClient<T> {
    pub fn using() -> Self {
        Self {
            state: ClientState::Using,
        }
    }

    pub fn waiting(min_wait_time: i64, max_wait_time: i64) -> Self {
        Self {
            state: ClientState::Waiting(Utc::now(), min_wait_time, max_wait_time),
        }
    }

    pub fn take(&mut self) -> Option<T> {
        match self.state {
            ClientState::Present(_) => match mem::replace(&mut self.state, ClientState::Using) {
                ClientState::Present(client) => Some(client),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn replace(&mut self, instance: T) {
        self.state = ClientState::Present(instance);
    }

    pub fn ready_to_try(&self) -> bool {
        match self.state {
            ClientState::Using | ClientState::Present(_) => false,
            ClientState::Waiting(last_attempt, wait_time, _) => {
                (Utc::now() - last_attempt) >= chrono::Duration::seconds(wait_time)
            }
        }
    }

    pub fn used(&mut self) {
        self.state = ClientState::Using;
    }

    pub fn wait_longer(&mut self) {
        if let ClientState::Waiting(_, wait_time, max_wait_time) = self.state {
            self.state =
                ClientState::Waiting(Utc::now(), min(wait_time * 2, max_wait_time), max_wait_time)
        }
    }
}

// Grafts on an additional ID associated with the contents, but allows for
// dereferencing whatever's contained inside and also prevents cloning.
#[derive(Debug)]
pub struct ID<T>(T, Uuid);

impl<T> ID<T> {
    fn new(contents: T) -> Self {
        Self(contents, Uuid::new_v4())
    }

    #[allow(dead_code)]
    pub fn id(&self) -> &Uuid {
        &self.1
    }
}

impl<T> Deref for ID<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for ID<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
