use cloud_storage::client::Client;
use minitrace_macro::trace;

use crate::storage::{Result, Storage, StorageError, StoragePath};

#[derive(Debug)]
pub struct GoogleCloud {
    client: Client,
    base_url: Option<String>,
    bucket: String,
}

impl GoogleCloud {
    pub fn new(base_url: Option<String>, bucket: String) -> Self {
        GoogleCloud {
            client: Client::default(),
            base_url,
            bucket,
        }
    }

    fn base_url_ref(&self) -> Option<&str> {
        if let Some(base_url) = &self.base_url {
            Some(base_url.as_str())
        } else {
            None
        }
    }
}

#[async_trait::async_trait]
impl Storage for GoogleCloud {
    #[trace("google_cloud::store")]
    async fn store(&self, path: &StoragePath, content_length: u64, data: &[u8]) -> Result<()> {
        if let Err(e) = self.validate_content_length(&path, content_length, &data) {
            log::warn!("{:?}", e);
        }

        self.client
            .object(self.base_url_ref())
            .create(
                self.bucket.as_str(),
                data.to_vec(),
                &path.file, // TODO add dir? - is this putting all files at top level of bucket?
                "application/octet-stream",
            )
            .await
            .map_err(|e| StorageError::IoError(format!("{:?}", e)))?;

        Ok(())
    }

    #[trace("google_cloud::fetch")]
    async fn fetch(&self, path: &StoragePath, content_length: u64) -> Result<Vec<u8>> {
        let data = self
            .client
            .object(self.base_url_ref())
            .download(self.bucket.as_str(), &path.file) // TODO surely this needs the dir
            .await
            .map_err(|e| {
                StorageError::IoError(format!("Unable to fetch file: {} {:?}", &path.file, e))
            })?;

        if let Err(e) = self.validate_content_length(&path, content_length, &data) {
            log::warn!("{:?}", e);
        }

        Ok(data)
    }
}
