use tonic::{Request, Response, Status};

use invariable::invariable_server::Invariable;
use invariable::{RepositoryIdRequest, RepositoryIdResponse};
use crate::db::db::DB;
use crate::db::models::CurrentRepository;

pub mod invariable {
    tonic::include_proto!("invariable"); // The string specified here must match the proto package name
}

pub struct MyServer {
    pub(crate) db: DB,
}

#[tonic::async_trait]
impl Invariable for MyServer {
    async fn repository_id(&self, request: Request<RepositoryIdRequest>) -> Result<Response<RepositoryIdResponse>, Status> {
        match self.db.get_or_create_current_repository().await {
            Ok(CurrentRepository { id, repo_id }) => {
                let reply = RepositoryIdResponse { repo_id };
                Ok(Response::new(reply))
            }
            Err(err) => Err(Status::unavailable(format!("{}", err))),
        }
    }
}
