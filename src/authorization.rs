use tonic::{Status, metadata::MetadataMap};

pub trait Authorization {
    fn authorize(&self, metadata: &MetadataMap) -> Result<(), Status>;
}

#[derive(Default)]
pub struct NoAuthorization {}

impl Authorization for NoAuthorization {
    fn authorize(&self, _: &MetadataMap) -> Result<(), Status> {
        Err(Status::permission_denied("auth_type not configured"))
    }
}

pub struct HeaderAuth<'a> {
    pub header: &'a str,
    pub value: &'a str,
}

impl Authorization for HeaderAuth<'_> {
    fn authorize(&self, metadata: &MetadataMap) -> Result<(), Status> {
        match metadata.get(self.header) {
            Some(value) if value == self.value => Ok(()),
            _ => Err(Status::permission_denied("not authorized")),
        }
    }
}
