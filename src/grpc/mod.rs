pub(crate) mod auth;
pub(crate) mod service;
pub(crate) mod types;

pub mod definitions {
    tonic::include_proto!("grpc");
}
