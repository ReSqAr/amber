pub(crate) mod auth;
pub(crate) mod service;
pub(crate) mod types;

pub mod definitions {
    pub mod grpc {
        include!("generated/grpc.rs");
    }

    pub use grpc::*;
}
