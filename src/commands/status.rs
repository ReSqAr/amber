use crate::repository::local_repository::LocalRepository;
use log::error;
use std::collections::HashMap;

use crate::db::models::VirtualFileState;
use crate::repository::logic::state;
use crate::repository::logic::state::StateConfig;
use crate::repository::traits::{Adder, Local, Metadata, VirtualFilesystem};
use futures::StreamExt;

pub async fn status(details: bool) -> Result<(), Box<dyn std::error::Error>> {
    let local_repository = LocalRepository::new(None).await?;

    show_status(local_repository, details).await
}
pub async fn show_status(
    repository: impl Metadata + Local + Adder + VirtualFilesystem + Clone + Sync + Send + 'static,
    details: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (handle, mut stream) = state::state(repository, StateConfig::default()).await?;
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
                error!("error during traversal: {}", e);
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
