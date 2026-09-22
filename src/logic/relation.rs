use crate::db::models::{Blob, File, RepoID, RepositoryMetadata};
use crate::repository::traits::{Metadata, Syncer};
use crate::utils::errors::{AppError, InternalError};
use futures::{StreamExt, TryStreamExt};
use std::collections::HashSet;

/// All repositories `repo` has ever heard of, itself included.
async fn known_repositories<R>(repo: &R) -> Result<HashSet<RepoID>, InternalError>
where
    R: Metadata + Syncer<RepositoryMetadata> + Sync,
{
    let mut known = HashSet::from([repo.current().await?.id]);
    let mut s = <R as Syncer<RepositoryMetadata>>::select(repo, None).await;
    while let Some(m) = s.try_next().await? {
        known.insert(m.repo_id);
    }
    Ok(known)
}

/// A repository is virgin if nothing was ever recorded in its file or blob log.
/// The logs are append-only, so removing every file does not make a repository virgin again.
async fn is_virgin<R>(repo: &R) -> Result<bool, InternalError>
where
    R: Syncer<File> + Syncer<Blob> + Sync,
{
    let first_file = <R as Syncer<File>>::select(repo, None).await.next().await;
    if first_file.transpose()?.is_some() {
        return Ok(false);
    }
    let first_blob = <R as Syncer<Blob>>::select(repo, None).await.next().await;
    Ok(first_blob.transpose()?.is_none())
}

/// Refuses to pair `local` with `remote` unless they share history or one of them is virgin.
///
/// Two repositories that have never met and both hold files would have their file logs merged
/// on the first sync - which cannot be undone and spreads to every other peer.
/// A virgin repository on either side is fine: it has nothing that could leak into the other one,
/// which covers both pushing into a freshly initialised server repository
/// and cloning into a freshly initialised local one.
pub async fn ensure_related<L, R>(
    local: &L,
    remote: &R,
    connection_name: &str,
) -> Result<(), InternalError>
where
    L: Metadata + Syncer<RepositoryMetadata> + Syncer<File> + Syncer<Blob> + Sync,
    R: Metadata + Syncer<RepositoryMetadata> + Syncer<File> + Syncer<Blob> + Sync,
{
    let (local_known, remote_known) =
        futures::try_join!(known_repositories(local), known_repositories(remote))?;
    if !local_known.is_disjoint(&remote_known) {
        return Ok(());
    }

    let (local_virgin, remote_virgin) = futures::try_join!(is_virgin(local), is_virgin(remote))?;
    if local_virgin || remote_virgin {
        return Ok(());
    }

    let remote_meta = remote.current().await?;
    Err(AppError::UnrelatedRepositories {
        connection_name: connection_name.to_string(),
        remote_name: remote_meta.name,
        remote_id: remote_meta.id.0,
    }
    .into())
}
