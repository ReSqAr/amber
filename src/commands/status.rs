use crate::db::models::VirtualFileState;
use crate::repository::local::LocalRepository;
use crate::repository::logic::state;
use crate::repository::traits::{Adder, Config, Local, Metadata, VirtualFilesystem};
use crate::utils::errors::InternalError;
use crate::utils::walker::WalkerConfig;
use futures::StreamExt;
use log::error;
use std::collections::HashMap;
use std::path::PathBuf;

pub async fn status(maybe_root: Option<PathBuf>, details: bool) -> Result<(), InternalError> {
    let local_repository = LocalRepository::new(maybe_root).await?;

    show_status(local_repository, details).await
}
pub async fn show_status(
    local: impl Metadata + Config + Local + Adder + VirtualFilesystem + Clone + Send + Sync + 'static,
    details: bool,
) -> Result<(), InternalError> {
    let (handle, mut stream) = state::state(local, WalkerConfig::default()).await?;
    let mut count = HashMap::new();

    while let Some(file_result) = stream.next().await {
        match file_result {
            Ok(file) => {
                let state = file.state.unwrap_or(VirtualFileState::NeedsCheck);
                *count.entry(state.clone()).or_insert(0) += 1;
                if details {
                    match state {
                        VirtualFileState::New => {
                            println!("new: {}", file.path);
                        }
                        VirtualFileState::Deleted => {
                            println!("deleted: {}", file.path);
                        }
                        VirtualFileState::Dirty | VirtualFileState::NeedsCheck => {
                            println!("BROKEN: {}", file.path);
                        }
                        VirtualFileState::Ok => {}
                    }
                }
            }
            Err(e) => {
                error!("error during traversal: {e}");
            }
        }
    }

    let new_count = *count.entry(VirtualFileState::New).or_default();
    let deleted_count = *count.entry(VirtualFileState::Deleted).or_default();
    let ok_count = *count.entry(VirtualFileState::Ok).or_default();
    let dirty_count = *count.entry(VirtualFileState::Dirty).or_default();
    let needs_check_count = *count.entry(VirtualFileState::NeedsCheck).or_default();
    println!(
        "new: {} deleted: {} ok: {} broken: {}",
        new_count,
        deleted_count,
        ok_count,
        dirty_count + needs_check_count
    );

    handle.await??;

    Ok(())
}
