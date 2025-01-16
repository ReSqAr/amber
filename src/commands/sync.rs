use crate::commands::errors::InvariableError;
use crate::db::db::DB;
use crate::db::establish_connection;
use crate::db::models::Repository as DbRepository;
use crate::db::schema::run_migrations;
use crate::transport::server::invariable::invariable_client::InvariableClient;
use crate::transport::server::invariable::{
    Blob, File, LookupRepositoryRequest, Repository, RepositoryIdRequest, SelectBlobsRequest,
    SelectFilesRequest, SelectRepositoriesRequest, UpdateLastIndicesRequest,
};
use anyhow::{Context, Result};
use futures::TryStreamExt;
use log::{debug, info};
use tokio::fs;
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

pub async fn sync(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let current_path = fs::canonicalize(".").await?;
    let invariable_path = current_path.join(".inv");
    if !fs::metadata(&invariable_path)
        .await
        .map(|m| m.is_dir())
        .unwrap_or(false)
    {
        return Err(InvariableError::NotInitialised().into());
    };

    let db_path = invariable_path.join("db.sqlite");
    let pool = establish_connection(db_path.to_str().unwrap())
        .await
        .context("failed to establish connection")?;
    run_migrations(&pool)
        .await
        .context("failed to run migrations")?;

    let db = DB::new(pool.clone());
    debug!("db connected");

    let local_repo = db.get_or_create_current_repository().await?;
    debug!("local repo_id={}", local_repo.repo_id);

    let addr = format!("http://127.0.0.1:{}", port);
    debug!("connecting to {}", &addr);
    let mut client = InvariableClient::connect(addr.clone()).await?;
    debug!("connected to {}", &addr);

    let repo_id_request = tonic::Request::new(RepositoryIdRequest {});
    let remote_repo = client.repository_id(repo_id_request).await?.into_inner();
    info!("remote repo_id={}", remote_repo.repo_id);

    let lookup_repo_request = tonic::Request::new(LookupRepositoryRequest {
        repo_id: local_repo.repo_id.clone(),
    });
    let Repository {
        last_file_index: remote_last_file_index,
        last_blob_index: remote_last_blob_index,
        ..
    } = client
        .lookup_repository(lookup_repo_request)
        .await?
        .into_inner()
        .repo
        .unwrap_or(Repository {
            repo_id: local_repo.repo_id.clone(),
            last_file_index: -1,
            last_blob_index: -1,
        });
    debug!(
        "remote remote_last_file_index={} remote_last_blob_index={}",
        remote_last_file_index, remote_last_blob_index
    );

    let files = db.select_files(&remote_last_file_index).await?;
    let (files_tx, files_rx) = mpsc::channel(1000);
    let files_producer = tokio::spawn(async move {
        for file in files {
            if files_tx.send(file.into()).await.is_err() {
                break;
            }
        }
    });
    client.merge_files(ReceiverStream::new(files_rx)).await?;
    files_producer.await?;
    debug!("remote: merged files");

    let blobs = db.select_blobs(&remote_last_blob_index).await?;
    let (blobs_tx, blobs_rx) = mpsc::channel(1000);
    let blobs_producer = tokio::spawn(async move {
        for blob in blobs {
            if blobs_tx.send(blob.into()).await.is_err() {
                break;
            }
        }
    });
    client.merge_blobs(ReceiverStream::new(blobs_rx)).await?;
    blobs_producer.await?;
    debug!("remote: merged blobs");

    let DbRepository {
        last_file_index: local_last_file_index,
        last_blob_index: local_last_blob_index,
        ..
    } = db.lookup_repository(remote_repo.repo_id).await?;
    debug!(
        "local local_last_file_index={} local_last_blob_index={}",
        local_last_file_index, local_last_blob_index
    );

    let files_response = client
        .select_files(SelectFilesRequest {
            last_index: local_last_file_index,
        })
        .await?;
    let files = files_response
        .into_inner()
        .map_ok(File::into)
        .try_collect()
        .await?;
    db.merge_files(files).await?;
    debug!("local: merged files");

    let blobs_response = client
        .select_blobs(SelectBlobsRequest {
            last_index: local_last_blob_index,
        })
        .await?;
    let blobs = blobs_response
        .into_inner()
        .map_ok(Blob::into)
        .try_collect()
        .await?;
    db.merge_blobs(blobs).await?;
    debug!("local: merged blobs");

    client
        .update_last_indices(UpdateLastIndicesRequest {})
        .await?;
    debug!("remote: updated last indices");

    db.update_last_indices().await?;
    debug!("local: updated last indices");

    let repos = db.select_repositories().await?;
    let (repos_tx, repos_rx) = mpsc::channel(1000);
    let repositories_producer = tokio::spawn(async move {
        for repo in repos {
            if repos_tx.send(repo.into()).await.is_err() {
                break;
            }
        }
    });
    client
        .merge_repositories(ReceiverStream::new(repos_rx))
        .await
        .context("client failed to merge repositories")?;
    repositories_producer.await?;
    debug!("remote: merged repositories");

    let repositories_response = client
        .select_repositories(SelectRepositoriesRequest {})
        .await?;
    let repos = repositories_response
        .into_inner()
        .map_ok(Repository::into)
        .try_collect()
        .await?;
    db.merge_repositories(repos).await?;
    debug!("local: merged repositories");

    Ok(())
}
