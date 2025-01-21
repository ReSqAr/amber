use crate::repository::local::LocalRepository;
use crate::repository::traits::Missing;
use log::error;
use tokio_stream::StreamExt;

pub async fn missing(files_only: bool) -> Result<(), Box<dyn std::error::Error>> {
    let local_repository = LocalRepository::new(None).await?;

    list_missing_blobs(local_repository, files_only).await
}

pub async fn list_missing_blobs(
    repository: impl Missing,
    files_only: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut missing_blobs = repository.missing();

    let mut count_files = 0usize;
    let mut count_blobs = 0usize;
    while let Some(blob_result) = missing_blobs.next().await {
        match blob_result {
            Ok(blob) => {
                count_files += blob.paths.len();
                count_blobs += 1;

                if files_only {
                    for path in blob.paths {
                        println!("{}", path);
                    }
                } else {
                    for path in blob.paths {
                        println!("{} {}", blob.blob_id, path);
                    }
                }
            }
            Err(e) => {
                error!("error during traversal: {}", e);
            }
        }
    }

    println!(
        "missing files: {} missing blobs: {}",
        count_files, count_blobs
    );

    Ok(())
}
