use crate::commands::errors::InvariableError;
use crate::commands::pipe::TryForwardIntoExt;
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
use tonic::Status;

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

    db.select_files(remote_last_file_index.clone())
        .map_err(|db_err| Status::internal(format!("Database error: {db_err}"))) // TODO
        .map_ok(File::from)
        .try_forward_into(|s| client.merge_files(s))
        .await?;
    debug!("remote: merged files");

    db.select_blobs(remote_last_blob_index.clone())
        .map_err(|db_err| Status::internal(format!("Database error: {db_err}"))) // TODO
        .map_ok(Blob::from)
        .try_forward_into(|s| client.merge_blobs(s))
        .await?;
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

    db.select_repositories()
        .map_err(|db_err| Status::internal(format!("Database error: {db_err}"))) // TODO
        .map_ok(Repository::from)
        .try_forward_into(|s| client.merge_repositories(s))
        .await?;
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
