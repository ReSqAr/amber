use std::str::FromStr;
use std::sync::Arc;
use tonic::service::Interceptor;
use tonic::{Request, Status};

#[derive(Clone)]
pub struct ServerAuth {
    auth_key: Arc<String>,
}

impl ServerAuth {
    pub fn new(auth_key: String) -> Self {
        Self {
            auth_key: Arc::new(auth_key),
        }
    }
}

impl Interceptor for ServerAuth {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        match request.metadata().get("authorization") {
            Some(auth_token) => match auth_token.to_str() {
                Ok(token) => {
                    if token == self.auth_key.as_str() {
                        Ok(request)
                    } else {
                        Err(Status::unauthenticated("Invalid authorization token"))
                    }
                }
                Err(_) => Err(Status::unauthenticated(
                    "Invalid authorization token format",
                )),
            },
            None => Err(Status::unauthenticated("Missing authorization token")),
        }
    }
}

#[derive(Clone)]
pub struct ClientAuth {
    auth_header: tonic::metadata::MetadataValue<tonic::metadata::Ascii>,
}

impl ClientAuth {
    pub fn new(auth_key: &str) -> Result<Self, tonic::metadata::errors::InvalidMetadataValue> {
        Ok(Self {
            auth_header: tonic::metadata::MetadataValue::from_str(auth_key)?,
        })
    }
}

impl Interceptor for ClientAuth {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        req.metadata_mut()
            .insert("authorization", self.auth_header.clone());
        Ok(req)
    }
}
