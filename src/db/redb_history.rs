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
const FILE_STATE_TABLE: TableDefinition<'static, &'static str, Vec<u8>> =
    TableDefinition::new("shadow_file_states");
const BLOB_STATE_TABLE: TableDefinition<'static, &'static str, Vec<u8>> =
    TableDefinition::new("shadow_blob_states");
const REPO_BLOB_STATE_TABLE: TableDefinition<'static, &'static str, Vec<u8>> =
    TableDefinition::new("shadow_repo_blob_states");
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct FileState {
    latest: Option<FileRecord>,
    latest_with_blob: Option<FileRecord>,
    valid_blob_keys: HashSet<String>,
    pending_materialisations: HashMap<String, Vec<MaterialisationRecord>>,
    latest_materialisation: Option<MaterialisationRecord>,
}

impl FileState {
    fn is_empty(&self) -> bool {
        self.latest.is_none()
            && self.latest_with_blob.is_none()
            && self.latest_materialisation.is_none()
            && self.valid_blob_keys.is_empty()
            && self.pending_materialisations.is_empty()
    }

    fn clear_materialisations(&mut self) {
        self.pending_materialisations.clear();
        self.latest_materialisation = None;
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct BlobState {
    ever_had_blob: bool,
    pending_files: Vec<FileRecord>,
    repositories: HashMap<String, BlobRecord>,
}

impl BlobState {
    fn is_empty(&self) -> bool {
        !self.ever_had_blob && self.pending_files.is_empty() && self.repositories.is_empty()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct RepoBlobState {
    by_blob: HashMap<String, BlobRecord>,
    paths: HashMap<String, String>,
}

impl RepoBlobState {
    fn is_empty(&self) -> bool {
        self.by_blob.is_empty() && self.paths.is_empty()
    }
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
                let mut file_state_table = txn.open_table(FILE_STATE_TABLE)?;

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

                    let mut state = get_file_state(&mut file_state_table, record.path.as_str())?;
                    if materialisation_record_is_valid(&record, &state) {
                        apply_latest_materialisation(&mut state, &record)?;
                    } else {
                        add_pending_materialisation_to_state(&mut state, &record)?;
                    }
                    put_file_state(&mut file_state_table, record.path.as_str(), &state)?;
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

    pub(crate) async fn append_all(
        &self,
        files: Vec<FileRecord>,
        blobs: Vec<BlobRecord>,
        materialisations: Vec<MaterialisationRecord>,
    ) -> Result<(), DBError> {
        if files.is_empty() && blobs.is_empty() && materialisations.is_empty() {
            return Ok(());
        }

        let db = self.store.database();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_write()?;
            {
                let mut files_history_table = txn.open_table(FILES_HISTORY_TABLE)?;
                let mut files_index_table = txn.open_table(FILES_INDEX_TABLE)?;
                let mut blobs_history_table = txn.open_table(BLOBS_HISTORY_TABLE)?;
                let mut blobs_index_table = txn.open_table(BLOBS_INDEX_TABLE)?;
                let mut materialisations_table = txn.open_table(MATERIALISATIONS_HISTORY_TABLE)?;
                let mut sequence_table = txn.open_table(SEQUENCE_TABLE)?;
                let mut blob_state_table = txn.open_table(BLOB_STATE_TABLE)?;
                let mut file_state_table = txn.open_table(FILE_STATE_TABLE)?;
                let mut repo_blob_state_table = txn.open_table(REPO_BLOB_STATE_TABLE)?;

                let mut current_file_sequence = sequence_table
                    .get("files")?
                    .map(|guard| guard.value())
                    .unwrap_or(0);
                let mut current_blob_sequence = sequence_table
                    .get("blobs")?
                    .map(|guard| guard.value())
                    .unwrap_or(0);
                let mut current_mat_sequence = sequence_table
                    .get("materialisations")?
                    .map(|guard| guard.value())
                    .unwrap_or(0);

                let mut file_sequence_dirty = false;
                let mut blob_sequence_dirty = false;
                let mut mat_sequence_dirty = false;

                for mut record in files {
                    if record.id == 0 {
                        current_file_sequence += 1;
                        record.id = current_file_sequence;
                        file_sequence_dirty = true;
                    } else if record.id > current_file_sequence {
                        current_file_sequence = record.id;
                        file_sequence_dirty = true;
                    }
                    let bytes = bincode::serialize(&record)?;
                    files_history_table.insert(record.uuid.as_str(), bytes.clone())?;
                    files_index_table.insert(record.id, bytes.clone())?;

                    if file_record_is_valid(&record, &mut blob_state_table)? {
                        apply_valid_file_record(&record, &mut file_state_table)?;
                    } else if let Some(blob_id) = &record.blob_id {
                        add_pending_file_to_state(&mut blob_state_table, blob_id, &record)?;
                    }
                }

                let mut pending_wakeups: HashMap<String, Vec<FileRecord>> = HashMap::new();

                for mut record in blobs {
                    if record.id == 0 {
                        current_blob_sequence += 1;
                        record.id = current_blob_sequence;
                        blob_sequence_dirty = true;
                    } else if record.id > current_blob_sequence {
                        current_blob_sequence = record.id;
                        blob_sequence_dirty = true;
                    }
                    let bytes = bincode::serialize(&record)?;
                    blobs_history_table.insert(record.uuid.as_str(), bytes.clone())?;
                    blobs_index_table.insert(record.id, bytes.clone())?;

                    let mut blob_state =
                        get_blob_state(&mut blob_state_table, record.blob_id.as_str())?;
                    let mut repo_state =
                        get_repo_blob_state(&mut repo_blob_state_table, record.repo_id.as_str())?;

                    if let Some(existing) = repo_state.by_blob.get(record.blob_id.as_str()) {
                        if let Some(existing_path) = &existing.path {
                            repo_state.paths.remove(existing_path);
                        }
                    }
                    if let Some(existing) = blob_state.repositories.get(record.repo_id.as_str()) {
                        if let Some(existing_path) = &existing.path {
                            repo_state.paths.remove(existing_path);
                        }
                    }

                    if record.has_blob {
                        blob_state.ever_had_blob = true;
                    }
                    blob_state
                        .repositories
                        .insert(record.repo_id.clone(), record.clone());

                    repo_state
                        .by_blob
                        .insert(record.blob_id.clone(), record.clone());
                    if let Some(path) = &record.path {
                        repo_state
                            .paths
                            .insert(path.clone(), record.blob_id.clone());
                    }

                    let mut newly_valid = Vec::new();
                    if blob_state.ever_had_blob && !blob_state.pending_files.is_empty() {
                        newly_valid.extend(blob_state.pending_files.drain(..));
                    }

                    put_blob_state(&mut blob_state_table, record.blob_id.as_str(), &blob_state)?;
                    put_repo_blob_state(
                        &mut repo_blob_state_table,
                        record.repo_id.as_str(),
                        &repo_state,
                    )?;

                    if !newly_valid.is_empty() {
                        pending_wakeups
                            .entry(record.blob_id.clone())
                            .or_default()
                            .extend(newly_valid);
                    }
                }

                for records in pending_wakeups.into_values() {
                    for record in records {
                        apply_valid_file_record(&record, &mut file_state_table)?;
                    }
                }

                for mut record in materialisations {
                    if record.id == 0 {
                        current_mat_sequence += 1;
                        record.id = current_mat_sequence;
                        mat_sequence_dirty = true;
                    } else if record.id > current_mat_sequence {
                        current_mat_sequence = record.id;
                        mat_sequence_dirty = true;
                    }
                    let bytes = bincode::serialize(&record)?;
                    materialisations_table.insert(record.id, bytes)?;

                    let mut state = get_file_state(&mut file_state_table, record.path.as_str())?;
                    if materialisation_record_is_valid(&record, &state) {
                        apply_latest_materialisation(&mut state, &record)?;
                    } else {
                        add_pending_materialisation_to_state(&mut state, &record)?;
                    }
                    put_file_state(&mut file_state_table, record.path.as_str(), &state)?;
                }

                if file_sequence_dirty {
                    sequence_table.insert("files", current_file_sequence)?;
                }
                if blob_sequence_dirty {
                    sequence_table.insert("blobs", current_blob_sequence)?;
                }
                if mat_sequence_dirty {
                    sequence_table.insert("materialisations", current_mat_sequence)?;
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
            let table = match txn.open_table(REPO_BLOB_STATE_TABLE) {
                Ok(table) => table,
                Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(e) => return Err(DBError::from(e)),
            };

            let Some(value) = table.get(repo_id.as_str())? else {
                return Ok(Vec::new());
            };
            let state: RepoBlobState = bincode::deserialize(value.value().as_slice())?;

            let mut results = Vec::new();
            for record in state.by_blob.values() {
                if record.has_blob {
                    results.push(AvailableBlob {
                        repo_id: record.repo_id.clone(),
                        blob_id: record.blob_id.clone(),
                        blob_size: record.blob_size,
                        path: record.path.clone(),
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
            let table = match txn.open_table(REPO_BLOB_STATE_TABLE) {
                Ok(table) => table,
                Err(TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
                Err(e) => return Err(DBError::from(e)),
            };

            let Some(value) = table.get(repo_id.as_str())? else {
                return Ok(HashMap::new());
            };
            let state: RepoBlobState = bincode::deserialize(value.value().as_slice())?;

            let mut results: HashMap<String, BlobRecord> = HashMap::new();
            for path in targets {
                if let Some(blob_id) = state.paths.get(&path) {
                    if let Some(record) = state.by_blob.get(blob_id) {
                        results.insert(path.clone(), record.clone());
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
            let table = match txn.open_table(BLOB_STATE_TABLE) {
                Ok(table) => table,
                Err(TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
                Err(e) => return Err(DBError::from(e)),
            };

            let mut map: HashMap<String, Vec<String>> = HashMap::new();
            for entry in table.iter()? {
                let (key_guard, value_guard) = entry?;
                let blob_id = key_guard.value();
                let state: BlobState = bincode::deserialize(value_guard.value().as_slice())?;
                let mut repos = Vec::new();
                for (repo, record) in state.repositories.iter() {
                    if record.has_blob {
                        repos.push(repo.clone());
                    }
                }
                if !repos.is_empty() {
                    map.insert(blob_id.to_string(), repos);
                }
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
            let table = match txn.open_table(FILE_STATE_TABLE) {
                Ok(table) => table,
                Err(TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
                Err(e) => return Err(DBError::from(e)),
            };

            let mut map = HashMap::new();
            for key in keys {
                if let Some(value_guard) = table.get(key.as_str())? {
                    let state: FileState = bincode::deserialize(value_guard.value().as_slice())?;
                    if let Some(record) = state.latest_with_blob.as_ref() {
                        map.insert(record.path.clone(), record.clone());
                    }
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
            let table = match txn.open_table(FILE_STATE_TABLE) {
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

                let state: FileState = bincode::deserialize(value_guard.value().as_slice())?;
                if let Some(record) = state.latest_with_blob.as_ref() {
                    results.push(record.clone());
                }
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
            let table = match txn.open_table(FILE_STATE_TABLE) {
                Ok(table) => table,
                Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(e) => return Err(DBError::from(e)),
            };

            let mut results = Vec::new();
            for entry in table.iter()? {
                let (_key_guard, value_guard) = entry?;
                let state: FileState = bincode::deserialize(value_guard.value().as_slice())?;
                if let Some(record) = state.latest_with_blob.as_ref() {
                    if let Some(blob_id) = record.blob_id.as_ref() {
                        results.push((record.path.clone(), blob_id.clone()));
                    }
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
            let table = match txn.open_table(FILE_STATE_TABLE) {
                Ok(table) => table,
                Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(e) => return Err(DBError::from(e)),
            };

            let mut results = Vec::new();
            for entry in table.iter()? {
                let (_key_guard, value_guard) = entry?;
                let state: FileState = bincode::deserialize(value_guard.value().as_slice())?;
                if let Some(record) = state.latest_materialisation.as_ref() {
                    results.push((record.path.clone(), record.blob_id.clone()));
                }
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
            clear_table(&txn, FILE_STATE_TABLE)?;
            if let Ok(mut table) = txn.open_table(BLOB_STATE_TABLE) {
                let mut updates: Vec<(String, BlobState)> = Vec::new();
                {
                    let mut iter = table.iter()?;
                    while let Some(entry) = iter.next() {
                        let (key_guard, value_guard) = entry?;
                        let mut state: BlobState =
                            bincode::deserialize(value_guard.value().as_slice())?;
                        if state.pending_files.is_empty() {
                            continue;
                        }
                        state.pending_files.clear();
                        updates.push((key_guard.value().to_string(), state));
                    }
                }
                for (key, state) in updates {
                    put_blob_state(&mut table, key.as_str(), &state)?;
                }
            }
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
            clear_table(&txn, BLOB_STATE_TABLE)?;
            clear_table(&txn, REPO_BLOB_STATE_TABLE)?;
            clear_table(&txn, FILE_STATE_TABLE)?;
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
            if let Ok(mut table) = txn.open_table(FILE_STATE_TABLE) {
                let mut updates: Vec<(String, FileState)> = Vec::new();
                {
                    let mut iter = table.iter()?;
                    while let Some(entry) = iter.next() {
                        let (key_guard, value_guard) = entry?;
                        let mut state: FileState =
                            bincode::deserialize(value_guard.value().as_slice())?;
                        if state.pending_materialisations.is_empty()
                            && state.latest_materialisation.is_none()
                        {
                            continue;
                        }
                        state.clear_materialisations();
                        updates.push((key_guard.value().to_string(), state));
                    }
                }
                for (key, state) in updates {
                    put_file_state(&mut table, key.as_str(), &state)?;
                }
            }
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
                let mut blob_state_table = txn.open_table(BLOB_STATE_TABLE)?;
                let mut file_state_table = txn.open_table(FILE_STATE_TABLE)?;

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

                    if file_record_is_valid(&record, &mut blob_state_table)? {
                        apply_valid_file_record(&record, &mut file_state_table)?;
                    } else if let Some(blob_id) = &record.blob_id {
                        add_pending_file_to_state(&mut blob_state_table, blob_id, &record)?;
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
                let mut blob_state_table = txn.open_table(BLOB_STATE_TABLE)?;
                let mut repo_blob_state_table = txn.open_table(REPO_BLOB_STATE_TABLE)?;
                let mut file_state_table = txn.open_table(FILE_STATE_TABLE)?;

                let mut current_sequence = sequence_table
                    .get("blobs")?
                    .map(|guard| guard.value())
                    .unwrap_or(0);
                let mut sequence_dirty = false;
                let mut inserted = 0usize;
                let mut pending_wakeups: HashMap<String, Vec<FileRecord>> = HashMap::new();

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

                    let mut blob_state =
                        get_blob_state(&mut blob_state_table, record.blob_id.as_str())?;
                    let mut repo_state =
                        get_repo_blob_state(&mut repo_blob_state_table, record.repo_id.as_str())?;

                    if let Some(existing) = repo_state.by_blob.get(record.blob_id.as_str()) {
                        if let Some(existing_path) = &existing.path {
                            repo_state.paths.remove(existing_path);
                        }
                    }
                    if let Some(existing) = blob_state.repositories.get(record.repo_id.as_str()) {
                        if let Some(existing_path) = &existing.path {
                            repo_state.paths.remove(existing_path);
                        }
                    }

                    if record.has_blob {
                        blob_state.ever_had_blob = true;
                    }
                    blob_state
                        .repositories
                        .insert(record.repo_id.clone(), record.clone());

                    repo_state
                        .by_blob
                        .insert(record.blob_id.clone(), record.clone());
                    if let Some(path) = &record.path {
                        repo_state
                            .paths
                            .insert(path.clone(), record.blob_id.clone());
                    }

                    let mut newly_valid = Vec::new();
                    if blob_state.ever_had_blob && !blob_state.pending_files.is_empty() {
                        newly_valid.extend(blob_state.pending_files.drain(..));
                    }

                    put_blob_state(&mut blob_state_table, record.blob_id.as_str(), &blob_state)?;
                    put_repo_blob_state(
                        &mut repo_blob_state_table,
                        record.repo_id.as_str(),
                        &repo_state,
                    )?;

                    if !newly_valid.is_empty() {
                        pending_wakeups
                            .entry(record.blob_id.clone())
                            .or_default()
                            .extend(newly_valid);
                    }

                    inserted += 1;
                }

                for records in pending_wakeups.into_values() {
                    for record in records {
                        apply_valid_file_record(&record, &mut file_state_table)?;
                    }
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

fn blob_key_owned(blob_id: Option<&str>) -> String {
    blob_id.unwrap_or(NULL_BLOB_SENTINEL).to_string()
}

fn get_file_state(
    table: &mut redb::Table<&str, Vec<u8>>,
    path: &str,
) -> Result<FileState, DBError> {
    match table.get(path)? {
        Some(value) => Ok(bincode::deserialize(value.value().as_slice())?),
        None => Ok(FileState::default()),
    }
}

fn put_file_state(
    table: &mut redb::Table<&str, Vec<u8>>,
    path: &str,
    state: &FileState,
) -> Result<(), DBError> {
    if state.is_empty() {
        table.remove(path)?;
    } else {
        let encoded = bincode::serialize(state)?;
        table.insert(path, encoded)?;
    }

    Ok(())
}

fn get_blob_state(
    table: &mut redb::Table<&str, Vec<u8>>,
    blob_id: &str,
) -> Result<BlobState, DBError> {
    match table.get(blob_id)? {
        Some(value) => Ok(bincode::deserialize(value.value().as_slice())?),
        None => Ok(BlobState::default()),
    }
}

fn put_blob_state(
    table: &mut redb::Table<&str, Vec<u8>>,
    blob_id: &str,
    state: &BlobState,
) -> Result<(), DBError> {
    if state.is_empty() {
        table.remove(blob_id)?;
    } else {
        let encoded = bincode::serialize(state)?;
        table.insert(blob_id, encoded)?;
    }

    Ok(())
}

fn get_repo_blob_state(
    table: &mut redb::Table<&str, Vec<u8>>,
    repo_id: &str,
) -> Result<RepoBlobState, DBError> {
    match table.get(repo_id)? {
        Some(value) => Ok(bincode::deserialize(value.value().as_slice())?),
        None => Ok(RepoBlobState::default()),
    }
}

fn put_repo_blob_state(
    table: &mut redb::Table<&str, Vec<u8>>,
    repo_id: &str,
    state: &RepoBlobState,
) -> Result<(), DBError> {
    if state.is_empty() {
        table.remove(repo_id)?;
    } else {
        let encoded = bincode::serialize(state)?;
        table.insert(repo_id, encoded)?;
    }

    Ok(())
}

fn file_record_is_valid(
    record: &FileRecord,
    blob_state_table: &mut redb::Table<&str, Vec<u8>>,
) -> Result<bool, DBError> {
    match &record.blob_id {
        None => Ok(true),
        Some(blob_id) => {
            let state = get_blob_state(blob_state_table, blob_id.as_str())?;
            Ok(state.ever_had_blob)
        }
    }
}

fn add_pending_file_to_state(
    blob_state_table: &mut redb::Table<&str, Vec<u8>>,
    blob_id: &str,
    record: &FileRecord,
) -> Result<(), DBError> {
    let mut state = get_blob_state(blob_state_table, blob_id)?;
    state.pending_files.push(record.clone());
    put_blob_state(blob_state_table, blob_id, &state)
}

fn apply_valid_file_record(
    record: &FileRecord,
    file_state_table: &mut redb::Table<&str, Vec<u8>>,
) -> Result<(), DBError> {
    let mut state = get_file_state(file_state_table, record.path.as_str())?;
    let key = blob_key_owned(record.blob_id.as_deref());
    state.valid_blob_keys.insert(key.clone());
    update_latest_file_state(&mut state, record);

    if let Some(pending) = state.pending_materialisations.remove(&key) {
        for pending_record in pending {
            apply_latest_materialisation(&mut state, &pending_record)?;
        }
    }

    put_file_state(file_state_table, record.path.as_str(), &state)
}

fn update_latest_file_state(state: &mut FileState, record: &FileRecord) {
    let should_replace = state
        .latest
        .as_ref()
        .map(|current| should_replace_latest_file(current, record))
        .unwrap_or(true);

    if should_replace {
        state.latest = Some(record.clone());
        if record.blob_id.is_some() {
            state.latest_with_blob = Some(record.clone());
        } else {
            state.latest_with_blob = None;
        }
    }
}

fn add_pending_materialisation_to_state(
    state: &mut FileState,
    record: &MaterialisationRecord,
) -> Result<(), DBError> {
    let key = blob_key_owned(record.blob_id.as_deref());
    state
        .pending_materialisations
        .entry(key)
        .or_default()
        .push(record.clone());
    Ok(())
}

fn apply_latest_materialisation(
    state: &mut FileState,
    record: &MaterialisationRecord,
) -> Result<(), DBError> {
    let should_replace = state
        .latest_materialisation
        .as_ref()
        .map(|current| should_replace_latest_materialisation(current, record))
        .unwrap_or(true);

    if should_replace {
        state.latest_materialisation = Some(record.clone());
    }

    Ok(())
}

fn materialisation_record_is_valid(record: &MaterialisationRecord, state: &FileState) -> bool {
    let key = blob_key_owned(record.blob_id.as_deref());
    state.valid_blob_keys.contains(&key)
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
