use crate::db::error::DBError;
use crate::db::models::{
    MaterialisationProjection, MissingFile, Observation, VirtualFile, VirtualFileState,
};
use crate::db::redb_store::RedbStore;
use crate::utils::stream::BoundedWaitChunksExt;
use futures::StreamExt;
use futures_core::stream::BoxStream;
use redb::{ReadableDatabase, ReadableTable, TableDefinition, TableError};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

const VFS_MATERIALISATION_CHUNK_SIZE: usize = 1024;
const VFS_MATERIALISATION_MAX_WAIT: Duration = Duration::from_millis(25);

const VIRTUAL_FILESYSTEM_TABLE: TableDefinition<'static, &'static str, Vec<u8>> =
    TableDefinition::new("virtual_filesystem");

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct VirtualFilesystemEntry {
    pub(crate) fs_last_seen_id: Option<i64>,
    pub(crate) fs_last_seen_dttm: Option<i64>,
    pub(crate) fs_last_modified_dttm: Option<i64>,
    pub(crate) fs_last_size: Option<i64>,
    pub(crate) materialisation_last_blob_id: Option<String>,
    pub(crate) materialisation_batch_id: i64,
    pub(crate) target_blob_id: Option<String>,
    pub(crate) target_blob_size: Option<i64>,
    pub(crate) local_has_target_blob: bool,
    pub(crate) check_last_dttm: Option<i64>,
    pub(crate) check_last_hash: Option<String>,
}

impl VirtualFilesystemEntry {
    fn compute_state(&self) -> VirtualFileState {
        if self.target_blob_id.is_none() && self.materialisation_last_blob_id.is_none() {
            return VirtualFileState::New;
        }

        match (self.fs_last_modified_dttm, self.check_last_dttm) {
            (Some(fs_last_modified), Some(check_last_dttm))
                if fs_last_modified <= check_last_dttm =>
            {
                let check_hash = self.check_last_hash.as_deref();
                let target_blob = self.target_blob_id.as_deref();
                let materialisation = self.materialisation_last_blob_id.as_deref();

                if check_hash != target_blob {
                    if check_hash.is_some() && check_hash == materialisation {
                        VirtualFileState::Outdated
                    } else {
                        VirtualFileState::Altered
                    }
                } else if let (Some(target_size), Some(fs_size)) =
                    (self.target_blob_size, self.fs_last_size)
                    && fs_size != target_size
                {
                    VirtualFileState::CorruptionDetected
                } else if check_hash != materialisation {
                    VirtualFileState::OkMaterialisationMissing
                } else {
                    VirtualFileState::Ok
                }
            }
            _ => VirtualFileState::NeedsCheck,
        }
    }
}

fn encode_entry(entry: &VirtualFilesystemEntry) -> Result<Vec<u8>, DBError> {
    Ok(bincode::serialize(entry)?)
}

fn decode_entry(bytes: &[u8]) -> Result<VirtualFilesystemEntry, DBError> {
    Ok(bincode::deserialize(bytes)?)
}

#[derive(Clone)]
pub struct VirtualFilesystemStore {
    store: RedbStore,
}

impl VirtualFilesystemStore {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, DBError> {
        let store = RedbStore::open(path).await?;

        Ok(Self { store })
    }

    pub(crate) fn redb_store(&self) -> RedbStore {
        self.store.clone()
    }

    pub async fn truncate(&self) -> Result<(), DBError> {
        let db = self.store.database();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_write()?;
            if let Ok(mut table) = txn.open_table(VIRTUAL_FILESYSTEM_TABLE) {
                table.retain(|_, _| false)?;
            }
            txn.commit()?;
            Ok(())
        })
        .await?
    }

    pub(crate) async fn update_materialisation_data<'a>(
        &self,
        entries: BoxStream<'a, Result<MaterialisationProjection, DBError>>,
        batch_id: i64,
    ) -> Result<(), DBError> {
        let mut chunks = entries
            .bounded_wait_chunks(VFS_MATERIALISATION_CHUNK_SIZE, VFS_MATERIALISATION_MAX_WAIT)
            .boxed();
        while let Some(chunk) = chunks.next().await {
            let db = self.store.database();
            tokio::task::spawn_blocking(move || {
                let mut ok_items = Vec::with_capacity(chunk.len());
                for item in chunk {
                    match item {
                        Ok(it) => ok_items.push(it),
                        Err(e) => return Err(e),
                    }
                }
                if ok_items.is_empty() {
                    return Ok(());
                }

                let txn = db.begin_write()?;
                {
                    let mut table = txn.open_table(VIRTUAL_FILESYSTEM_TABLE)?;
                    for item in ok_items {
                        let path = item.path;
                        let current_entry = table
                            .get(path.as_str())?
                            .map(|e| {
                                let bytes = e.value();
                                decode_entry(&bytes)
                            })
                            .transpose()?
                            .unwrap_or_default();

                        let mut updated = current_entry;
                        updated.materialisation_last_blob_id = item.materialisation_last_blob_id;
                        updated.target_blob_id = item.target_blob_id;
                        updated.target_blob_size = item.target_blob_size;
                        updated.local_has_target_blob = item.local_has_target_blob;
                        updated.materialisation_batch_id = batch_id;

                        table.insert(path.as_str(), encode_entry(&updated)?)?;
                    }
                }
                txn.commit()?;
                Ok::<(), DBError>(())
            })
            .await??;
        }
        self.cleanup_materialisation_batch(batch_id).await
    }

    pub(crate) async fn cleanup_materialisation_batch(&self, batch_id: i64) -> Result<(), DBError> {
        let db = self.store.database();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_write()?;
            {
                let mut table = txn.open_table(VIRTUAL_FILESYSTEM_TABLE)?;
                const CHUNK: usize = 1024;

                loop {
                    let mut keys: Vec<String> = Vec::with_capacity(CHUNK);
                    {
                        // Iterator is scoped to this block and will be dropped before we mutate the table.
                        for item in table.iter()? {
                            let (k, v) = item?;
                            let entry = decode_entry(&v.value())?;
                            if entry.materialisation_batch_id != batch_id {
                                keys.push(k.value().to_string());
                                if keys.len() >= CHUNK {
                                    break;
                                }
                            }
                        }
                    }

                    if keys.is_empty() {
                        break;
                    }

                    for key in keys {
                        let bytes = match table.get(key.as_str())? {
                            Some(e) => e.value(),
                            None => continue,
                        };
                        let mut entry = decode_entry(&bytes)?;
                        if entry.materialisation_batch_id != batch_id {
                            entry.materialisation_last_blob_id = None;
                            entry.target_blob_id = None;
                            entry.target_blob_size = None;
                            entry.local_has_target_blob = false;
                            entry.materialisation_batch_id = batch_id;

                            table.insert(key.as_str(), encode_entry(&entry)?)?;
                        }
                    }
                }
            }
            txn.commit()?;
            Ok(())
        })
        .await?
    }

    pub async fn cleanup(&self, last_seen_id: i64) -> Result<(), DBError> {
        let db = self.store.database();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_write()?;
            {
                let mut table = txn.open_table(VIRTUAL_FILESYSTEM_TABLE)?;
                table.retain(|_, v| {
                    let bytes = v;
                    match decode_entry(&bytes) {
                        Ok(entry) => {
                            entry.fs_last_seen_id == Some(last_seen_id)
                                || entry.target_blob_id.is_some()
                                || entry.materialisation_last_blob_id.is_some()
                        }
                        Err(_) => false,
                    }
                })?;
            }
            txn.commit()?;
            Ok(())
        })
        .await?
    }

    pub fn select_missing_files(
        &self,
        last_seen_id: i64,
    ) -> BoxStream<'static, Result<MissingFile, DBError>> {
        let (tx, rx) = mpsc::unbounded_channel::<Result<MissingFile, DBError>>();
        let db = self.store.database();

        tokio::task::spawn_blocking(move || {
            let send_ok = |m: MissingFile| {
                let _ = tx.send(Ok(m));
            };
            let send_err = |e: DBError| {
                let _ = tx.send(Err(e));
            };

            let txn = match db.begin_read() {
                Ok(t) => t,
                Err(err) => {
                    return send_err(err.into());
                }
            };

            let table = match txn.open_table(VIRTUAL_FILESYSTEM_TABLE) {
                Ok(t) => t,
                Err(TableError::TableDoesNotExist(_)) => return, // nothing to send
                Err(err) => return send_err(err.into()),
            };

            match table.iter() {
                Ok(iter) => {
                    for item in iter {
                        match item {
                            Ok((key, value)) => {
                                let path = key.value();
                                match decode_entry(&value.value()) {
                                    Ok(entry)
                                        if entry.target_blob_id.is_some()
                                            && entry
                                                .fs_last_seen_id
                                                .map(|id| id != last_seen_id)
                                                .unwrap_or(true) =>
                                    {
                                        send_ok(MissingFile {
                                            path: path.to_string(),
                                            target_blob_id: entry.target_blob_id.clone().unwrap(),
                                            local_has_target_blob: entry.local_has_target_blob,
                                        });
                                    }
                                    Ok(_) => {}
                                    Err(e) => send_err(e),
                                }
                            }
                            Err(e) => send_err(e.into()),
                        }
                    }
                }
                Err(e) => send_err(e.into()),
            }
        });

        UnboundedReceiverStream::new(rx).boxed()
    }

    pub async fn apply_observations(
        &self,
        observations: Vec<Observation>,
    ) -> Result<Vec<VirtualFile>, DBError> {
        let db = self.store.database();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_write()?;
            let output = {
                let mut table = txn.open_table(VIRTUAL_FILESYSTEM_TABLE)?;
                let mut output: Vec<VirtualFile> = Vec::with_capacity(observations.len());

                for obs in observations {
                    match obs {
                        Observation::FileSeen(fs) => {
                            let mut entry = table
                                .get(fs.path.as_str())?
                                .map(|e| decode_entry(&e.value()))
                                .transpose()?
                                .unwrap_or_default();

                            entry.fs_last_seen_id = Some(fs.seen_id);
                            entry.fs_last_seen_dttm = Some(fs.seen_dttm);
                            entry.fs_last_modified_dttm = Some(fs.last_modified_dttm);
                            entry.fs_last_size = Some(fs.size);

                            table.insert(fs.path.as_str(), encode_entry(&entry)?)?;

                            output.push(VirtualFile {
                                path: fs.path,
                                materialisation_last_blob_id: entry
                                    .materialisation_last_blob_id
                                    .clone(),
                                local_has_target_blob: entry.local_has_target_blob,
                                target_blob_id: entry.target_blob_id.clone(),
                                state: entry.compute_state(),
                            });
                        }
                        Observation::FileCheck(fc) => {
                            let mut entry = table
                                .get(fc.path.as_str())?
                                .map(|e| decode_entry(&e.value()))
                                .transpose()?
                                .unwrap_or_default();

                            entry.check_last_dttm = Some(fc.check_dttm);
                            entry.check_last_hash = Some(fc.hash);

                            table.insert(fc.path.as_str(), encode_entry(&entry)?)?;

                            output.push(VirtualFile {
                                path: fc.path,
                                materialisation_last_blob_id: entry
                                    .materialisation_last_blob_id
                                    .clone(),
                                local_has_target_blob: entry.local_has_target_blob,
                                target_blob_id: entry.target_blob_id.clone(),
                                state: entry.compute_state(),
                            });
                        }
                    }
                }
                output
            };
            txn.commit()?;
            Ok(output)
        })
        .await?
    }
}
