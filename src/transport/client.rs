use crate::repository::local_repository::{
    LastIndices, LastIndicesSyncer, Metadata, Syncer, SyncerParams,
};
use crate::transport::server::invariable::invariable_client::InvariableClient;
use crate::transport::server::invariable::{Blob, File, Repository, RepositoryIdRequest, SelectBlobsRequest, SelectFilesRequest, SelectRepositoriesRequest, UpdateLastIndicesRequest, LookupLastIndicesRequest, LookupLastIndicesResponse};
use crate::utils::app_error::AppError;
use futures::TryStreamExt;
use futures::{FutureExt, TryFutureExt};
use std::sync::Arc;
use log::debug;
use tokio::sync::RwLock;

#[derive(Clone)]
pub(crate) struct Client {
    client: Arc<RwLock<InvariableClient<tonic::transport::Channel>>>,
}

impl Client {
    pub fn new(client: InvariableClient<tonic::transport::Channel>) -> Self {
        Self {
            client: Arc::new(client.into()),
        }
    }

    pub async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
        debug!("connecting to {}", &addr);
        let client = InvariableClient::connect(addr.clone()).await?;
        debug!("connected to {}", &addr);

        Ok(Self::new(client))
    }
}

impl Metadata for Client {
    async fn repo_id(&self) -> Result<String, AppError> {
        let repo_id_request = tonic::Request::new(RepositoryIdRequest {});
        let remote_repo = self.client.write().await.repository_id(repo_id_request).await?.into_inner();
        Ok(remote_repo.repo_id)
    }
}

impl LastIndicesSyncer for Client {
    async fn lookup(&self, repo_id: String) -> Result<LastIndices, AppError> {
        let LookupLastIndicesResponse {
            file,
            blob,
        } = self
            .client
            .write()
            .await
            .lookup_last_indices( LookupLastIndicesRequest  { repo_id: repo_id.clone() })
            .await?
            .into_inner();
        Ok(LastIndices { file, blob })
    }

    async fn refresh(&self) -> Result<(), AppError> {
        self.client.write().await.update_last_indices(UpdateLastIndicesRequest {})
            .await?;
        Ok(())
    }
}

impl SyncerParams for Repository {
    type Params = ();
}

impl Syncer<Repository> for Client {
    fn select(
        &self,
        _params: (),
    ) -> impl std::future::Future<
        Output = impl futures::Stream<Item = Result<Repository, AppError>> + Unpin + Send + 'static,
    > {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .select_repositories(SelectRepositoriesRequest {})
                .map_ok(tonic::Response::into_inner)
                .map(|r| r.unwrap().err_into())
                .await
        }
    }

    fn merge<S>(&self, s: S) -> impl std::future::Future<Output = Result<(), AppError>> + Send
    where
        S: futures::Stream<Item = Repository> + Unpin + Send + 'static,
    {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard.merge_repositories(s).err_into().map_ok(|_| ()).await
        }
    }
}

impl SyncerParams for File {
    type Params = i32;
}

impl Syncer<File> for Client {
    fn select(
        &self,
        last_index: i32,
    ) -> impl std::future::Future<
        Output = impl futures::Stream<Item = Result<File, AppError>> + Unpin + Send + 'static,
    > {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .select_files(SelectFilesRequest { last_index })
                .map_ok(tonic::Response::into_inner)
                .map(|r| r.unwrap().err_into())
                .await
        }
    }

    fn merge<S>(&self, s: S) -> impl std::future::Future<Output = Result<(), AppError>> + Send
    where
        S: futures::Stream<Item = File> + Unpin + Send + 'static,
    {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard.merge_files(s).err_into().map_ok(|_| ()).await
        }
    }
}

impl SyncerParams for Blob {
    type Params = i32;
}

impl Syncer<Blob> for Client {
    fn select(
        &self,
        last_index: i32,
    ) -> impl std::future::Future<
        Output = impl futures::Stream<Item = Result<Blob, AppError>> + Unpin + Send + 'static,
    > {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .select_blobs(SelectBlobsRequest { last_index })
                .map_ok(tonic::Response::into_inner)
                .map(|r| r.unwrap().err_into())
                .await
        }
    }

    fn merge<S>(&self, s: S) -> impl std::future::Future<Output = Result<(), AppError>> + Send
    where
        S: futures::Stream<Item = Blob> + Unpin + Send + 'static,
    {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard.merge_blobs(s).err_into().map_ok(|_| ()).await
        }
    }
}
