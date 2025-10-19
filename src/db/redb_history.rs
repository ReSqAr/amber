use crate::db::error::DBError;
use crate::db::models::{AvailableBlob, Blob, File};
use crate::db::redb_store::RedbStore;
use chrono::{DateTime, Utc};
use redb::{ReadableDatabase, ReadableTable, TableDefinition, TableError, WriteTransaction};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

const FILES_HISTORY_TABLE: TableDefinition<'static, &'static str, Vec<u8>> =
    TableDefinition::new("history_files");
const BLOBS_HISTORY_TABLE: TableDefinition<'static, &'static str, Vec<u8>> =
    TableDefinition::new("history_blobs");
const MATERIALISATIONS_HISTORY_TABLE: TableDefinition<'static, i64, Vec<u8>> =
    TableDefinition::new("history_materialisations");
const FILES_INDEX_TABLE: TableDefinition<'static, i64, Vec<u8>> =
    TableDefinition::new("history_files_index");
const BLOBS_INDEX_TABLE: TableDefinition<'static, i64, Vec<u8>> =
    TableDefinition::new("history_blobs_index");
const SEQUENCE_TABLE: TableDefinition<'static, &'static str, i64> =
    TableDefinition::new("history_sequences");
const LATEST_AVAILABLE_BLOBS_TABLE: TableDefinition<
    'static,
    (&'static str, &'static str),
    Vec<u8>,
> = TableDefinition::new("shadow_latest_available_blobs");
const KNOWN_BLOBS_TABLE: TableDefinition<'static, &'static str, u8> =
    TableDefinition::new("shadow_known_blobs");
const LATEST_VALID_FILES_TABLE: TableDefinition<'static, &'static str, Vec<u8>> =
    TableDefinition::new("shadow_latest_valid_files");
const LATEST_FILESYSTEM_FILES_TABLE: TableDefinition<'static, &'static str, Vec<u8>> =
    TableDefinition::new("shadow_latest_filesystem_files");
const PENDING_FILES_BY_BLOB_TABLE: TableDefinition<'static, &'static str, Vec<u8>> =
    TableDefinition::new("shadow_pending_files_by_blob");
const VALID_FILE_KEYS_TABLE: TableDefinition<'static, (&'static str, &'static str), u8> =
    TableDefinition::new("shadow_valid_file_keys");
const PENDING_MATERIALISATIONS_TABLE: TableDefinition<
    'static,
    (&'static str, &'static str),
    Vec<u8>,
> = TableDefinition::new("shadow_pending_materialisations");
const LATEST_MATERIALISATIONS_TABLE: TableDefinition<'static, &'static str, Vec<u8>> =
    TableDefinition::new("shadow_latest_materialisations");
const TRANSFERS_TABLE: TableDefinition<'static, (i64, &'static str), Vec<u8>> =
    TableDefinition::new("transfers");

const NULL_BLOB_SENTINEL: &str = "\0";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FileRecord {
    #[serde(default)]
    pub(crate) id: i64,
    pub(crate) uuid: String,
    pub(crate) path: String,
    pub(crate) blob_id: Option<String>,
    pub(crate) valid_from: DateTime<Utc>,
}

impl From<File> for FileRecord {
    fn from(file: File) -> Self {
        Self {
            id: 0,
            uuid: file.uuid,
            path: file.path,
            blob_id: file.blob_id,
            valid_from: file.valid_from,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BlobRecord {
    #[serde(default)]
    pub(crate) id: i64,
    pub(crate) uuid: String,
    pub(crate) repo_id: String,
    pub(crate) blob_id: String,
    pub(crate) blob_size: i64,
    pub(crate) has_blob: bool,
    pub(crate) path: Option<String>,
    pub(crate) valid_from: DateTime<Utc>,
}

impl From<Blob> for BlobRecord {
    fn from(blob: Blob) -> Self {
        Self {
            id: 0,
            uuid: blob.uuid,
            repo_id: blob.repo_id,
            blob_id: blob.blob_id,
            blob_size: blob.blob_size,
            has_blob: blob.has_blob,
            path: blob.path,
            valid_from: blob.valid_from,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MaterialisationRecord {
    pub(crate) id: i64,
    pub(crate) path: String,
    pub(crate) blob_id: Option<String>,
    pub(crate) valid_from: DateTime<Utc>,
}

impl MaterialisationRecord {
    pub(crate) fn new(
        id: i64,
        path: String,
        blob_id: Option<String>,
        valid_from: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            path,
            blob_id,
            valid_from,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TransferRecord {
    pub(crate) transfer_id: i64,
    pub(crate) blob_id: String,
    pub(crate) blob_size: i64,
    pub(crate) path: String,
}

#[derive(Clone)]
pub(crate) struct RedbHistoryStore {
    store: RedbStore,
}

impl RedbHistoryStore {
    pub(crate) fn new(store: RedbStore) -> Self {
        Self { store }
    }

    pub(crate) async fn append_files(&self, records: Vec<FileRecord>) -> Result<(), DBError> {
        self.write_files(records, false).await.map(|_| ())
    }

    pub(crate) async fn merge_files(&self, records: Vec<FileRecord>) -> Result<usize, DBError> {
        self.write_files(records, true).await
    }

    pub(crate) async fn append_blobs(&self, records: Vec<BlobRecord>) -> Result<(), DBError> {
        self.write_blobs(records, false).await.map(|_| ())
    }

    pub(crate) async fn merge_blobs(&self, records: Vec<BlobRecord>) -> Result<usize, DBError> {
        self.write_blobs(records, true).await
    }

    pub(crate) async fn append_materialisations(
        &self,
        records: Vec<MaterialisationRecord>,
    ) -> Result<(), DBError> {
        if records.is_empty() {
            return Ok(());
        }

        let db = self.store.database();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_write()?;
            {
                let mut table = txn.open_table(MATERIALISATIONS_HISTORY_TABLE)?;
                let mut sequence_table = txn.open_table(SEQUENCE_TABLE)?;
                let valid_file_keys_table = txn.open_table(VALID_FILE_KEYS_TABLE)?;
                let mut latest_table = txn.open_table(LATEST_MATERIALISATIONS_TABLE)?;
                let mut pending_table = txn.open_table(PENDING_MATERIALISATIONS_TABLE)?;

                let mut current_sequence = sequence_table
                    .get("materialisations")?
                    .map(|guard| guard.value())
                    .unwrap_or(0);
                let mut sequence_dirty = false;

                for mut record in records {
                    if record.id == 0 {
                        current_sequence += 1;
                        record.id = current_sequence;
                        sequence_dirty = true;
                    } else if record.id > current_sequence {
                        current_sequence = record.id;
                        sequence_dirty = true;
                    }
                    let bytes = bincode::serialize(&record)?;
                    table.insert(record.id, bytes)?;

                    if materialisation_record_is_valid(&record, &valid_file_keys_table)? {
                        apply_latest_materialisation_update(&mut latest_table, &record)?;
                    } else {
                        add_pending_materialisation(&mut pending_table, &record)?;
                    }
                }

                if sequence_dirty {
                    sequence_table.insert("materialisations", current_sequence)?;
                }
            }
            txn.commit()?;
            Ok::<(), DBError>(())
        })
        .await??;

        Ok(())
    }

    pub(crate) async fn store_transfers(
        &self,
        records: Vec<TransferRecord>,
    ) -> Result<(), DBError> {
        if records.is_empty() {
            return Ok(());
        }

        let db = self.store.database();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_write()?;
            {
                let mut table = txn.open_table(TRANSFERS_TABLE)?;
                for record in records {
                    let encoded = bincode::serialize(&record)?;
                    table.insert((record.transfer_id, record.path.as_str()), encoded)?;
                }
            }
            txn.commit()?;
            Ok::<(), DBError>(())
        })
        .await??;

        Ok(())
    }

    pub(crate) async fn lookup_transfers(
        &self,
        keys: &[(i64, String)],
    ) -> Result<Vec<TransferRecord>, DBError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let lookup_keys: Vec<(i64, String)> = keys.to_vec();
        let db = self.store.database();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read()?;
            let table = match txn.open_table(TRANSFERS_TABLE) {
                Ok(table) => table,
                Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(e) => return Err(DBError::from(e)),
            };

            let mut results = Vec::new();
            for (transfer_id, path) in lookup_keys {
                if let Some(value) = table.get((transfer_id, path.as_str()))? {
                    let record: TransferRecord = bincode::deserialize(value.value().as_slice())?;
                    results.push(record);
                }
            }

            Ok(results)
        })
        .await?
    }

    pub(crate) async fn clear_transfers(&self) -> Result<(), DBError> {
        let db = self.store.database();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_write()?;
            clear_table(&txn, TRANSFERS_TABLE)?;
            txn.commit()?;
            Ok::<(), DBError>(())
        })
        .await??;

        Ok(())
    }

    pub(crate) async fn files_after(&self, last_index: i64) -> Result<Vec<FileRecord>, DBError> {
        self.records_after(FILES_INDEX_TABLE, last_index).await
    }

    pub(crate) async fn blobs_after(&self, last_index: i64) -> Result<Vec<BlobRecord>, DBError> {
        self.records_after(BLOBS_INDEX_TABLE, last_index).await
    }

    pub(crate) async fn current_file_sequence(&self) -> Result<i64, DBError> {
        self.sequence_value("files").await
    }

    pub(crate) async fn current_blob_sequence(&self) -> Result<i64, DBError> {
        self.sequence_value("blobs").await
    }

    pub(crate) async fn latest_available_blobs_for_repo(
        &self,
        repo_id: &str,
    ) -> Result<Vec<AvailableBlob>, DBError> {
        let repo_id = repo_id.to_string();
        let db = self.store.database();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read()?;
            let table = match txn.open_table(LATEST_AVAILABLE_BLOBS_TABLE) {
                Ok(table) => table,
                Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(e) => return Err(DBError::from(e)),
            };

            let mut results = Vec::new();
            for item in table.iter()? {
                let (key_guard, value_guard) = item?;
                let (repo_key, _) = key_guard.value();
                if repo_key != repo_id {
                    continue;
                }

                let record: BlobRecord = bincode::deserialize(value_guard.value().as_slice())?;
                if record.has_blob {
                    results.push(AvailableBlob {
                        repo_id: record.repo_id,
                        blob_id: record.blob_id,
                        blob_size: record.blob_size,
                        path: record.path,
                    });
                }
            }

            Ok(results)
        })
        .await?
    }

    pub(crate) async fn latest_available_blobs_for_repo_paths(
        &self,
        repo_id: &str,
        paths: &[String],
    ) -> Result<HashMap<String, BlobRecord>, DBError> {
        if paths.is_empty() {
            return Ok(HashMap::new());
        }

        let repo_id = repo_id.to_string();
        let targets: Vec<String> = paths.iter().cloned().collect();
        let db = self.store.database();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read()?;
            let table = match txn.open_table(LATEST_AVAILABLE_BLOBS_TABLE) {
                Ok(table) => table,
                Err(TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
                Err(e) => return Err(DBError::from(e)),
            };

            let mut remaining: HashSet<String> = targets.into_iter().collect();
            let mut results: HashMap<String, BlobRecord> = HashMap::new();
            if remaining.is_empty() {
                return Ok(results);
            }

            let mut iter = table.range((repo_id.as_str(), "")..)?;
            while let Some(entry) = iter.next() {
                let (key_guard, value_guard) = entry?;
                let (repo_key, _) = key_guard.value();
                if repo_key != repo_id {
                    break;
                }

                let record: BlobRecord = bincode::deserialize(value_guard.value().as_slice())?;
                if let Some(path) = record.path.as_ref() {
                    if remaining.remove(path) {
                        results.insert(path.clone(), record);
                        if remaining.is_empty() {
                            break;
                        }
                    }
                }
            }

            Ok(results)
        })
        .await?
    }

    pub(crate) async fn repositories_with_blob(
        &self,
    ) -> Result<HashMap<String, Vec<String>>, DBError> {
        let db = self.store.database();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read()?;
            let table = match txn.open_table(LATEST_AVAILABLE_BLOBS_TABLE) {
                Ok(table) => table,
                Err(TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
                Err(e) => return Err(DBError::from(e)),
            };

            let mut map: HashMap<String, Vec<String>> = HashMap::new();
            for entry in table.iter()? {
                let (_key_guard, value_guard) = entry?;
                let record: BlobRecord = bincode::deserialize(value_guard.value().as_slice())?;
                if !record.has_blob {
                    continue;
                }

                map.entry(record.blob_id.clone())
                    .or_default()
                    .push(record.repo_id.clone());
            }

            Ok(map)
        })
        .await?
    }

    pub(crate) async fn latest_filesystem_files_snapshot(
        &self,
    ) -> Result<Vec<(String, String)>, DBError> {
        self.latest_filesystem_files().await
    }

    pub(crate) async fn latest_materialisations_snapshot(
        &self,
    ) -> Result<Vec<(String, Option<String>)>, DBError> {
        self.latest_materialisations().await
    }

    pub(crate) async fn latest_file_for_path(
        &self,
        path: &str,
    ) -> Result<Option<FileRecord>, DBError> {
        let mut paths = Vec::with_capacity(1);
        paths.push(path.to_string());
        let map = self.latest_filesystem_records_for_paths(&paths).await?;
        Ok(map.into_values().next())
    }

    pub(crate) async fn latest_filesystem_records_for_paths(
        &self,
        paths: &[String],
    ) -> Result<HashMap<String, FileRecord>, DBError> {
        if paths.is_empty() {
            return Ok(HashMap::new());
        }

        let keys: Vec<String> = paths.iter().cloned().collect();
        let db = self.store.database();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read()?;
            let table = match txn.open_table(LATEST_FILESYSTEM_FILES_TABLE) {
                Ok(table) => table,
                Err(TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
                Err(e) => return Err(DBError::from(e)),
            };

            let mut map = HashMap::new();
            for key in keys {
                if let Some(value_guard) = table.get(key.as_str())? {
                    let record: FileRecord = bincode::deserialize(value_guard.value().as_slice())?;
                    map.insert(record.path.clone(), record);
                }
            }

            Ok(map)
        })
        .await?
    }

    pub(crate) async fn latest_filesystem_records_with_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<FileRecord>, DBError> {
        let prefix = prefix.to_string();
        let db = self.store.database();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read()?;
            let table = match txn.open_table(LATEST_FILESYSTEM_FILES_TABLE) {
                Ok(table) => table,
                Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(e) => return Err(DBError::from(e)),
            };

            let mut results = Vec::new();
            let mut iter = table.range(prefix.as_str()..)?;
            while let Some(entry) = iter.next() {
                let (key_guard, value_guard) = entry?;
                let key = key_guard.value();
                if !key.starts_with(prefix.as_str()) {
                    break;
                }

                let record: FileRecord = bincode::deserialize(value_guard.value().as_slice())?;
                results.push(record);
            }

            Ok(results)
        })
        .await?
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn apply_sql_mutation(&self, sql: &str) -> Result<(), DBError> {
        let normalized = sql.trim().trim_end_matches(';').to_lowercase();

        // TODO: kinda wrong
        if normalized.starts_with("delete from blobs") {
            self.reset_blobs().await?;
        } else if normalized.starts_with("delete from materialisations") {
            self.reset_materialisations().await?;
        } else if normalized.starts_with("delete from files") {
            self.reset_files().await?;
        } else if normalized.starts_with("delete from transfers") {
            self.clear_transfers().await?;
        }

        Ok(())
    }

    async fn latest_filesystem_files(&self) -> Result<Vec<(String, String)>, DBError> {
        let db = self.store.database();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read()?;
            let table = match txn.open_table(LATEST_FILESYSTEM_FILES_TABLE) {
                Ok(table) => table,
                Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(e) => return Err(DBError::from(e)),
            };

            let mut results = Vec::new();
            for entry in table.iter()? {
                let (_key_guard, value_guard) = entry?;
                let record: FileRecord = bincode::deserialize(value_guard.value().as_slice())?;
                if let Some(blob_id) = record.blob_id {
                    results.push((record.path, blob_id));
                }
            }

            Ok(results)
        })
        .await?
    }

    async fn latest_materialisations(&self) -> Result<Vec<(String, Option<String>)>, DBError> {
        let db = self.store.database();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read()?;
            let table = match txn.open_table(LATEST_MATERIALISATIONS_TABLE) {
                Ok(table) => table,
                Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(e) => return Err(DBError::from(e)),
            };

            let mut results = Vec::new();
            for entry in table.iter()? {
                let (_key_guard, value_guard) = entry?;
                let record: MaterialisationRecord =
                    bincode::deserialize(value_guard.value().as_slice())?;
                results.push((record.path, record.blob_id));
            }

            Ok(results)
        })
        .await?
    }

    #[cfg_attr(not(test), allow(dead_code))]
    async fn reset_files(&self) -> Result<(), DBError> {
        let db = self.store.database();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_write()?;
            clear_table(&txn, FILES_HISTORY_TABLE)?;
            clear_table(&txn, FILES_INDEX_TABLE)?;
            clear_table(&txn, LATEST_VALID_FILES_TABLE)?;
            clear_table(&txn, LATEST_FILESYSTEM_FILES_TABLE)?;
            clear_table(&txn, PENDING_FILES_BY_BLOB_TABLE)?;
            clear_table(&txn, VALID_FILE_KEYS_TABLE)?;
            clear_table(&txn, PENDING_MATERIALISATIONS_TABLE)?;
            clear_table(&txn, LATEST_MATERIALISATIONS_TABLE)?;
            txn.commit()?;
            Ok::<(), DBError>(())
        })
        .await??;

        Ok(())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    async fn reset_blobs(&self) -> Result<(), DBError> {
        let db = self.store.database();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_write()?;
            clear_table(&txn, BLOBS_HISTORY_TABLE)?;
            clear_table(&txn, BLOBS_INDEX_TABLE)?;
            clear_table(&txn, LATEST_AVAILABLE_BLOBS_TABLE)?;
            clear_table(&txn, KNOWN_BLOBS_TABLE)?;
            clear_table(&txn, PENDING_FILES_BY_BLOB_TABLE)?;
            clear_table(&txn, LATEST_VALID_FILES_TABLE)?;
            clear_table(&txn, LATEST_FILESYSTEM_FILES_TABLE)?;
            clear_table(&txn, VALID_FILE_KEYS_TABLE)?;
            clear_table(&txn, PENDING_MATERIALISATIONS_TABLE)?;
            clear_table(&txn, LATEST_MATERIALISATIONS_TABLE)?;
            txn.commit()?;
            Ok::<(), DBError>(())
        })
        .await??;

        Ok(())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    async fn reset_materialisations(&self) -> Result<(), DBError> {
        let db = self.store.database();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_write()?;
            clear_table(&txn, MATERIALISATIONS_HISTORY_TABLE)?;
            clear_table(&txn, PENDING_MATERIALISATIONS_TABLE)?;
            clear_table(&txn, LATEST_MATERIALISATIONS_TABLE)?;
            txn.commit()?;
            Ok::<(), DBError>(())
        })
        .await??;

        Ok(())
    }
}

impl RedbHistoryStore {
    async fn write_files(
        &self,
        records: Vec<FileRecord>,
        skip_existing: bool,
    ) -> Result<usize, DBError> {
        if records.is_empty() {
            return Ok(0);
        }

        let db = self.store.database();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_write()?;
            let inserted = {
                let mut history_table = txn.open_table(FILES_HISTORY_TABLE)?;
                let mut index_table = txn.open_table(FILES_INDEX_TABLE)?;
                let mut sequence_table = txn.open_table(SEQUENCE_TABLE)?;
                let blob_presence_table = txn.open_table(KNOWN_BLOBS_TABLE)?;
                let mut latest_valid_table = txn.open_table(LATEST_VALID_FILES_TABLE)?;
                let mut latest_filesystem_table = txn.open_table(LATEST_FILESYSTEM_FILES_TABLE)?;
                let mut pending_table = txn.open_table(PENDING_FILES_BY_BLOB_TABLE)?;
                let mut valid_file_keys_table = txn.open_table(VALID_FILE_KEYS_TABLE)?;
                let mut pending_materialisations_table =
                    txn.open_table(PENDING_MATERIALISATIONS_TABLE)?;
                let mut latest_materialisations_table =
                    txn.open_table(LATEST_MATERIALISATIONS_TABLE)?;

                let mut current_sequence = sequence_table
                    .get("files")?
                    .map(|guard| guard.value())
                    .unwrap_or(0);
                let mut sequence_dirty = false;
                let mut inserted = 0usize;
                for mut record in records {
                    if skip_existing && history_table.get(record.uuid.as_str())?.is_some() {
                        continue;
                    }

                    if record.id == 0 {
                        current_sequence += 1;
                        record.id = current_sequence;
                        sequence_dirty = true;
                    } else if record.id > current_sequence {
                        current_sequence = record.id;
                        sequence_dirty = true;
                    }
                    let bytes = bincode::serialize(&record)?;
                    history_table.insert(record.uuid.as_str(), bytes.clone())?;
                    index_table.insert(record.id, bytes.clone())?;

                    if file_record_is_valid(&record, &blob_presence_table)? {
                        mark_valid_file(
                            &mut valid_file_keys_table,
                            record.path.as_str(),
                            record.blob_id.as_deref(),
                        )?;
                        apply_latest_file_update(
                            &mut latest_valid_table,
                            &mut latest_filesystem_table,
                            &record,
                        )?;
                        process_pending_materialisations(
                            record.path.as_str(),
                            record.blob_id.as_deref(),
                            &mut pending_materialisations_table,
                            &mut latest_materialisations_table,
                        )?;
                    } else if let Some(blob_id) = &record.blob_id {
                        add_pending_file(&mut pending_table, blob_id, &record)?;
                    }

                    inserted += 1;
                }

                if sequence_dirty {
                    sequence_table.insert("files", current_sequence)?;
                }

                inserted
            };
            txn.commit()?;
            Ok::<usize, DBError>(inserted)
        })
        .await?
    }

    async fn write_blobs(
        &self,
        records: Vec<BlobRecord>,
        skip_existing: bool,
    ) -> Result<usize, DBError> {
        if records.is_empty() {
            return Ok(0);
        }

        let db = self.store.database();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_write()?;
            let inserted = {
                let mut history_table = txn.open_table(BLOBS_HISTORY_TABLE)?;
                let mut index_table = txn.open_table(BLOBS_INDEX_TABLE)?;
                let mut sequence_table = txn.open_table(SEQUENCE_TABLE)?;
                let mut latest_table = txn.open_table(LATEST_AVAILABLE_BLOBS_TABLE)?;
                let mut known_blob_table = txn.open_table(KNOWN_BLOBS_TABLE)?;
                let mut pending_table = txn.open_table(PENDING_FILES_BY_BLOB_TABLE)?;
                let mut latest_valid_table = txn.open_table(LATEST_VALID_FILES_TABLE)?;
                let mut latest_filesystem_table = txn.open_table(LATEST_FILESYSTEM_FILES_TABLE)?;
                let mut valid_file_keys_table = txn.open_table(VALID_FILE_KEYS_TABLE)?;
                let mut pending_materialisations_table =
                    txn.open_table(PENDING_MATERIALISATIONS_TABLE)?;
                let mut latest_materialisations_table =
                    txn.open_table(LATEST_MATERIALISATIONS_TABLE)?;
                let mut touched_blob_ids = HashSet::new();
                let mut current_sequence = sequence_table
                    .get("blobs")?
                    .map(|guard| guard.value())
                    .unwrap_or(0);
                let mut sequence_dirty = false;
                let mut inserted = 0usize;

                for mut record in records {
                    if skip_existing && history_table.get(record.uuid.as_str())?.is_some() {
                        continue;
                    }

                    if record.id == 0 {
                        current_sequence += 1;
                        record.id = current_sequence;
                        sequence_dirty = true;
                    } else if record.id > current_sequence {
                        current_sequence = record.id;
                        sequence_dirty = true;
                    }
                    let bytes = bincode::serialize(&record)?;
                    history_table.insert(record.uuid.as_str(), bytes.clone())?;
                    index_table.insert(record.id, bytes.clone())?;
                    update_latest_available_blob(&mut latest_table, &record, bytes)?;
                    known_blob_table.insert(record.blob_id.as_str(), 1)?;
                    touched_blob_ids.insert(record.blob_id.clone());
                    inserted += 1;
                }

                for blob_id in touched_blob_ids.iter() {
                    process_pending_files(
                        blob_id,
                        &mut pending_table,
                        &mut latest_valid_table,
                        &mut latest_filesystem_table,
                        &mut valid_file_keys_table,
                        &mut pending_materialisations_table,
                        &mut latest_materialisations_table,
                    )?;
                }

                if sequence_dirty {
                    sequence_table.insert("blobs", current_sequence)?;
                }

                inserted
            };
            txn.commit()?;
            Ok::<usize, DBError>(inserted)
        })
        .await?
    }

    async fn records_after<T>(
        &self,
        table_def: TableDefinition<'static, i64, Vec<u8>>,
        last_index: i64,
    ) -> Result<Vec<T>, DBError>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let db = self.store.database();
        let start_key = last_index.saturating_add(1);
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read()?;
            let table = match txn.open_table(table_def) {
                Ok(table) => table,
                Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(e) => return Err(DBError::from(e)),
            };

            let mut results = Vec::new();
            for entry in table.range(start_key..)? {
                let (_key_guard, value_guard) = entry?;
                let record: T = bincode::deserialize(value_guard.value().as_slice())?;
                results.push(record);
            }

            Ok(results)
        })
        .await?
    }

    async fn sequence_value(&self, key: &'static str) -> Result<i64, DBError> {
        let db = self.store.database();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read()?;
            let table = match txn.open_table(SEQUENCE_TABLE) {
                Ok(table) => table,
                Err(TableError::TableDoesNotExist(_)) => return Ok(-1),
                Err(e) => return Err(DBError::from(e)),
            };

            let value = table.get(key)?.map(|guard| guard.value()).unwrap_or(-1);
            Ok(value)
        })
        .await?
    }
}

fn update_latest_available_blob(
    table: &mut redb::Table<(&str, &str), Vec<u8>>,
    record: &BlobRecord,
    encoded: Vec<u8>,
) -> Result<(), DBError> {
    let key = (record.repo_id.as_str(), record.blob_id.as_str());
    let action = match table.get(key)? {
        Some(existing) => {
            let existing_record: BlobRecord = bincode::deserialize(existing.value().as_slice())?;
            decide_latest_action(Some(&existing_record), record)
        }
        None => decide_latest_action(None, record),
    };

    match action {
        LatestAction::Keep => {}
        LatestAction::Delete => {
            table.remove(key)?;
        }
        LatestAction::Upsert => {
            table.insert(key, encoded)?;
        }
    }

    Ok(())
}

fn file_record_is_valid(
    record: &FileRecord,
    known_blob_table: &redb::Table<&str, u8>,
) -> Result<bool, DBError> {
    match &record.blob_id {
        None => Ok(true),
        Some(blob_id) => Ok(known_blob_table.get(blob_id.as_str())?.is_some()),
    }
}

fn add_pending_file(
    pending_table: &mut redb::Table<&str, Vec<u8>>,
    blob_id: &str,
    record: &FileRecord,
) -> Result<(), DBError> {
    let mut pending_records: Vec<FileRecord> = match pending_table.get(blob_id)? {
        Some(existing) => bincode::deserialize(existing.value().as_slice())?,
        None => Vec::new(),
    };
    pending_records.push(record.clone());

    let encoded = bincode::serialize(&pending_records)?;
    pending_table.insert(blob_id, encoded)?;

    Ok(())
}

fn add_pending_materialisation(
    pending_table: &mut redb::Table<(&str, &str), Vec<u8>>,
    record: &MaterialisationRecord,
) -> Result<(), DBError> {
    let key = (record.path.as_str(), blob_key(record.blob_id.as_deref()));
    let mut records: Vec<MaterialisationRecord> = match pending_table.get(key)? {
        Some(existing) => bincode::deserialize(existing.value().as_slice())?,
        None => Vec::new(),
    };
    records.push(record.clone());

    let encoded = bincode::serialize(&records)?;
    pending_table.insert(key, encoded)?;

    Ok(())
}

fn mark_valid_file(
    table: &mut redb::Table<(&str, &str), u8>,
    path: &str,
    blob_id: Option<&str>,
) -> Result<(), DBError> {
    let key = (path, blob_key(blob_id));
    table.insert(key, 1)?;
    Ok(())
}

fn process_pending_files(
    blob_id: &str,
    pending_table: &mut redb::Table<&str, Vec<u8>>,
    latest_valid_table: &mut redb::Table<&str, Vec<u8>>,
    latest_filesystem_table: &mut redb::Table<&str, Vec<u8>>,
    valid_file_keys_table: &mut redb::Table<(&str, &str), u8>,
    pending_materialisations_table: &mut redb::Table<(&str, &str), Vec<u8>>,
    latest_materialisations_table: &mut redb::Table<&str, Vec<u8>>,
) -> Result<(), DBError> {
    let Some(pending_bytes) = pending_table.remove(blob_id)? else {
        return Ok(());
    };

    let records: Vec<FileRecord> = bincode::deserialize(pending_bytes.value().as_slice())?;
    for record in records {
        mark_valid_file(
            valid_file_keys_table,
            record.path.as_str(),
            record.blob_id.as_deref(),
        )?;
        apply_latest_file_update(latest_valid_table, latest_filesystem_table, &record)?;
        process_pending_materialisations(
            record.path.as_str(),
            record.blob_id.as_deref(),
            pending_materialisations_table,
            latest_materialisations_table,
        )?;
    }

    Ok(())
}

fn process_pending_materialisations(
    path: &str,
    blob_id: Option<&str>,
    pending_table: &mut redb::Table<(&str, &str), Vec<u8>>,
    latest_table: &mut redb::Table<&str, Vec<u8>>,
) -> Result<(), DBError> {
    let key = (path, blob_key(blob_id));
    let Some(existing) = pending_table.remove(key)? else {
        return Ok(());
    };

    let records: Vec<MaterialisationRecord> = bincode::deserialize(existing.value().as_slice())?;
    for record in records {
        apply_latest_materialisation_update(latest_table, &record)?;
    }

    Ok(())
}

fn apply_latest_file_update(
    latest_valid_table: &mut redb::Table<&str, Vec<u8>>,
    latest_filesystem_table: &mut redb::Table<&str, Vec<u8>>,
    record: &FileRecord,
) -> Result<(), DBError> {
    let key = record.path.as_str();
    let should_replace = match latest_valid_table.get(key)? {
        Some(existing) => {
            let current: FileRecord = bincode::deserialize(existing.value().as_slice())?;
            should_replace_latest_file(&current, record)
        }
        None => true,
    };

    if should_replace {
        let encoded = bincode::serialize(record)?;
        latest_valid_table.insert(key, encoded.clone())?;
        if record.blob_id.is_some() {
            latest_filesystem_table.insert(key, encoded)?;
        } else {
            latest_filesystem_table.remove(key)?;
        }
    }

    Ok(())
}

fn apply_latest_materialisation_update(
    table: &mut redb::Table<&str, Vec<u8>>,
    record: &MaterialisationRecord,
) -> Result<(), DBError> {
    let key = record.path.as_str();
    let action = match table.get(key)? {
        Some(existing) => {
            let current: MaterialisationRecord = bincode::deserialize(existing.value().as_slice())?;
            if should_replace_latest_materialisation(&current, record) {
                LatestAction::Upsert
            } else {
                LatestAction::Keep
            }
        }
        None => LatestAction::Upsert,
    };

    if matches!(action, LatestAction::Upsert) {
        let encoded = bincode::serialize(record)?;
        table.insert(key, encoded)?;
    }

    Ok(())
}

fn should_replace_latest_file(current: &FileRecord, candidate: &FileRecord) -> bool {
    if candidate.valid_from > current.valid_from {
        return true;
    }

    if candidate.valid_from < current.valid_from {
        return false;
    }

    match (&candidate.blob_id, &current.blob_id) {
        (Some(candidate_blob), Some(current_blob)) => {
            if candidate_blob > current_blob {
                return true;
            }
            if candidate_blob < current_blob {
                return false;
            }
        }
        (Some(_), None) => return true,
        (None, Some(_)) => return false,
        (None, None) => {}
    }

    candidate.uuid > current.uuid
}

fn should_replace_latest_materialisation(
    current: &MaterialisationRecord,
    candidate: &MaterialisationRecord,
) -> bool {
    if candidate.valid_from > current.valid_from {
        return true;
    }

    if candidate.valid_from < current.valid_from {
        return false;
    }

    match (&candidate.blob_id, &current.blob_id) {
        (Some(candidate_blob), Some(current_blob)) => {
            if candidate_blob > current_blob {
                return true;
            }
            if candidate_blob < current_blob {
                return false;
            }
        }
        (Some(_), None) => return true,
        (None, Some(_)) => return false,
        (None, None) => {}
    }

    candidate.id > current.id
}

enum LatestAction {
    Keep,
    Delete,
    Upsert,
}

fn decide_latest_action(current: Option<&BlobRecord>, candidate: &BlobRecord) -> LatestAction {
    match current {
        None => {
            if candidate.has_blob {
                LatestAction::Upsert
            } else {
                LatestAction::Delete
            }
        }
        Some(existing) => {
            if !should_replace_latest(existing, candidate) {
                return LatestAction::Keep;
            }

            if candidate.has_blob {
                LatestAction::Upsert
            } else {
                LatestAction::Delete
            }
        }
    }
}

fn should_replace_latest(current: &BlobRecord, candidate: &BlobRecord) -> bool {
    if candidate.valid_from > current.valid_from {
        return true;
    }

    if candidate.valid_from < current.valid_from {
        return false;
    }

    if candidate.has_blob != current.has_blob {
        return candidate.has_blob;
    }

    candidate.uuid > current.uuid
}

fn materialisation_record_is_valid(
    record: &MaterialisationRecord,
    valid_file_table: &redb::Table<(&str, &str), u8>,
) -> Result<bool, DBError> {
    let key = (record.path.as_str(), blob_key(record.blob_id.as_deref()));
    Ok(valid_file_table.get(key)?.is_some())
}

fn blob_key(blob_id: Option<&str>) -> &str {
    blob_id.unwrap_or(NULL_BLOB_SENTINEL)
}

#[cfg_attr(not(test), allow(dead_code))]
fn clear_table<K, V>(
    txn: &WriteTransaction,
    definition: TableDefinition<'static, K, V>,
) -> Result<(), DBError>
where
    K: redb::Key + 'static,
    V: redb::Value + 'static,
{
    match txn.open_table(definition) {
        Ok(mut table) => {
            table.retain(|_, _| false)?;
            Ok(())
        }
        Err(TableError::TableDoesNotExist(_)) => Ok(()),
        Err(e) => Err(DBError::from(e)),
    }
}
