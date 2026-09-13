use std::str::FromStr;
use std::sync::Arc;
use subtle::ConstantTimeEq;
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

/// Compares two tokens without leaking how far they matched.
fn tokens_match(presented: &str, expected: &str) -> bool {
    let presented = presented.as_bytes();
    let expected = expected.as_bytes();
    // `ct_eq` needs equal lengths; the length of the expected token is not a
    // secret, so comparing it first is fine.
    presented.len() == expected.len() && bool::from(presented.ct_eq(expected))
}

impl Interceptor for ServerAuth {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        match request.metadata().get("authorization") {
            Some(auth_token) => match auth_token.to_str() {
                Ok(token) => {
                    if tokens_match(token, self.auth_key.as_str()) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_exact_token_is_accepted() {
        let key = "s3cret-token";
        assert!(tokens_match(key, key));
        assert!(!tokens_match("", key));
        assert!(!tokens_match("s3cret-toke", key), "a prefix must not match");
        assert!(
            !tokens_match("s3cret-token-and-more", key),
            "an extension must not match"
        );
        assert!(!tokens_match("S3cret-token", key));
    }

    #[test]
    fn a_request_without_a_token_is_rejected() {
        let mut auth = ServerAuth::new("key".to_string());
        let result = auth.call(Request::new(()));
        assert!(result.is_err(), "a missing token must be rejected");
    }

    #[test]
    fn a_request_with_the_right_token_is_accepted() {
        let mut auth = ServerAuth::new("key".to_string());
        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            tonic::metadata::MetadataValue::from_static("key"),
        );
        assert!(auth.call(request).is_ok());
    }

    #[test]
    fn a_request_with_a_wrong_token_is_rejected() {
        let mut auth = ServerAuth::new("key".to_string());
        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            tonic::metadata::MetadataValue::from_static("nope"),
        );
        assert!(auth.call(request).is_err());
    }
}
