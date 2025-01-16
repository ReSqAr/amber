use invariable::invariable_client::InvariableClient;
use invariable::{RepositoryIdRequest, RepositoryIdResponse};

pub mod invariable {
    tonic::include_proto!("invariable");
}