use crate::db::error::DBError;
use crate::db::kv::KVStores;
use crate::db::logs::Logs;
use crate::db::models::{
    AvailableBlob, Blob, BlobAssociatedToFiles, BlobID, BlobRef, BlobTransferItem, Connection,
    ConnectionName, CurrentBlob, CurrentFile, CurrentRepository, File, FileCheck, FileSeen,
    FileTransferItem, InsertBlob, InsertFile, InsertFileBundle, InsertMaterialisation,
    InsertRepositoryName, LogOffset, MissingFile, Path, RepoID, Repository, RepositoryMetadata,
    RepositoryName, VirtualFile,
};
use crate::flightdeck::base::BaseObserver;
use crate::flightdeck::tracer::Tracer;
use crate::flightdeck::tracked::stream::Trackable;
use crate::utils::stream::group_by_key;
use behemoth::{Offset, StreamError, TailFrom};
use chrono::Utc;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use log::debug;
use roaring::RoaringTreemap;
use std::collections::HashMap;
use std::fmt::Debug;
use std::future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::task::JoinHandle;
use uuid::Uuid;

#[derive(Clone)]
pub struct Database {
    kv: KVStores,
    logs: Logs,
}

pub(crate) type DBOutputStream<'a, T> = BoxStream<'a, Result<T, DBError>>;

const FILE_TABLE_NAME: &str = "files";
const BLOB_TABLE_NAME: &str = "blobs";
const MAT_TABLE_NAME: &str = "materialisations";
const NAME_TABLE_NAME: &str = "repository_names";

impl Database {
    pub fn new(kv: KVStores, logs: Logs) -> Self {
        Self { kv, logs }
    }

    pub async fn clean(&self) -> Result<(), DBError> {
        let reductions: HashMap<_, _> = self.kv.stream_current_reductions().try_collect().await?;
        let tracer = Tracer::new_on("clean");

        let mut of = BaseObserver::without_id("Database::clean::files");
        let file_offset = Self::next_offset(reductions.get(&FILE_TABLE_NAME.into()).copied());
        let f = self.file_log_stream(self.logs.files_writer.reader().from(file_offset));
        let mut ob = BaseObserver::without_id("Database::clean::blobs");
        let blob_offset = Self::next_offset(reductions.get(&BLOB_TABLE_NAME.into()).copied());
        let b = self.blob_log_stream(self.logs.blobs_writer.reader().from(blob_offset));
        let mut om = BaseObserver::without_id("Database::clean::materialisations");
        let mat_offset = Self::next_offset(reductions.get(&MAT_TABLE_NAME.into()).copied());
        let mat_reader = self.logs.materialisations_writer.reader();
        let m = self.materialisation_log_stream(mat_reader.from(mat_offset));
        let mut orn = BaseObserver::without_id("Database::clean::repository_names");
        let name_offset = Self::next_offset(reductions.get(&NAME_TABLE_NAME.into()).copied());
        let repo_name_reader = self.logs.repository_names_writer.reader();
        let rn = self.repository_name_log_stream(repo_name_reader.from(name_offset));

        let f_count = f.await??;
        let b_count = b.await??;
        let m_count = m.await??;
        let rn_count = rn.await??;

        of.observe_termination_ext(
            log::Level::Debug,
            "reduced",
            [("count".into(), f_count as u64)],
        );
        ob.observe_termination_ext(
            log::Level::Debug,
            "reduced",
            [("count".into(), b_count as u64)],
        );
        om.observe_termination_ext(
            log::Level::Debug,
            "reduced",
            [("count".into(), m_count as u64)],
        );
        orn.observe_termination_ext(
            log::Level::Debug,
            "reduced",
            [("count".into(), rn_count as u64)],
        );

        tracer.measure();

        Ok(())
    }

    fn next_offset(i: Option<impl Into<Offset>>) -> Offset {
        match i {
            None => Offset::start(),
            Some(last_index) => Into::<Offset>::into(last_index).increment(),
        }
    }

    pub async fn get_or_create_current_repository(&self) -> Result<CurrentRepository, DBError> {
        match self.kv.load_current_repository().await? {
            Some(current) => Ok(current),
            None => {
                let repo_id = RepoID(Uuid::new_v4().to_string());
                let current = CurrentRepository { repo_id };
                self.kv.store_current_repository(current).await?;
                Ok(self.kv.load_current_repository().await?.expect("was set"))
            }
        }
    }

    pub async fn lookup_current_repository_name(
        &self,
        repo_id: RepoID,
    ) -> Result<Option<String>, DBError> {
        let s = self.kv.left_join_current_repository_names(
            stream::iter(vec![Ok::<_, DBError>(repo_id)]),
            |r| r,
        );
        let r: Vec<_> = s.collect().await;

        assert_eq!(r.len(), 1, "expected precisely 1 repository lookup result");
        let (_, result) = r.into_iter().next().unwrap()?;
        Ok(result.map(|r| r.name))
    }

    pub async fn add_files<S>(&self, s: S) -> Result<u64, DBError>
    where
        S: Stream<Item = InsertFile> + Send + Unpin,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;
        let txn = self.logs.files_writer.transaction()?;
        let tail = txn
            .tail(TailFrom::Head)
            .track("lookup_current_repository_name::log_stream");
        let bg = self.file_log_stream(tail);

        let ts = Utc::now().timestamp_nanos_opt().unwrap() as u64;
        let mut s = s.enumerate();
        while let Some((i, file)) = s.next().await {
            total_attempted += 1;
            let uid = ts + (i as u64);
            txn.push(&File {
                uid,
                path: file.path.clone(),
                blob_id: file.blob_id.clone(),
                valid_from: file.valid_from,
            })
            .await
            .inspect_err(|e| log::error!("Database::add_files failed to add log: {e}"))?;
            total_inserted += 1;

            if i % self.logs.flush_size == 0 {
                txn.flush().await?;
            }
        }

        txn.close().await?;
        bg.await??;

        debug!(
            "files added: attempted={} inserted={}",
            total_attempted, total_inserted
        );
        Ok(total_inserted)
    }

    pub async fn add_blobs<S>(&self, s: S) -> Result<u64, DBError>
    where
        S: Stream<Item = InsertBlob> + Send + Unpin,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;
        let txn = self.logs.blobs_writer.transaction()?;
        let tail = txn.tail(TailFrom::Head).track("add_blobs::log_stream");
        let bg = self.blob_log_stream(tail);

        let ts = Utc::now().timestamp_nanos_opt().unwrap() as u64;
        let mut s = s.enumerate();
        while let Some((i, blob)) = s.next().await {
            total_attempted += 1;
            let uid = ts + (i as u64);
            txn.push(&Blob {
                uid,
                repo_id: blob.repo_id.clone(),
                blob_id: blob.blob_id.clone(),
                blob_size: blob.blob_size,
                has_blob: blob.has_blob,
                path: blob.path.clone(),
                valid_from: blob.valid_from,
            })
            .await
            .inspect_err(|e| log::error!("Database::add_blobs failed to add log: {e}"))?;
            total_inserted += 1;

            if i % self.logs.flush_size == 0 {
                txn.flush().await?;
            }
        }

        txn.close().await?;
        bg.await??;

        debug!(
            "blobs added: attempted={} inserted={}",
            total_attempted, total_inserted
        );
        Ok(total_inserted)
    }

    pub async fn add_file_bundles<S>(&self, s: S) -> Result<u64, DBError>
    where
        S: Stream<Item = InsertFileBundle> + Send + Unpin,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;

        let blob_txn = self.logs.blobs_writer.transaction()?;
        let file_txn = self.logs.files_writer.transaction()?;
        let mat_txn = self.logs.materialisations_writer.transaction()?;

        let blob_tail = blob_txn
            .tail(TailFrom::Head)
            .track("add_file_bundles::blob::log_stream");
        let file_tail = file_txn
            .tail(TailFrom::Head)
            .track("add_file_bundles::file::log_stream");
        let mat_tail = mat_txn
            .tail(TailFrom::Head)
            .track("add_file_bundles::mat::log_stream");
        let blob_bg = self.blob_log_stream(blob_tail);
        let file_bg = self.file_log_stream(file_tail);
        let mat_bg = self.materialisation_log_stream(mat_tail);

        let ts = Utc::now().timestamp_nanos_opt().unwrap() as u64;

        let mut tracer_b = Tracer::new_off("add_file_bundles::blob_txn::push");
        let mut tracer_f = Tracer::new_off("add_file_bundles::file_txn::push");
        let mut tracer_m = Tracer::new_off("add_file_bundles::mat_txn::push");

        let mut s = s.enumerate();
        while let Some((i, bundle)) = s.next().await {
            total_attempted += 1;
            let InsertFileBundle {
                file,
                blob,
                materialisation,
            } = bundle;

            let uid = ts + (i as u64);

            tracer_b.on();
            blob_txn
                .push(&Blob::from_insert_blob(blob, uid))
                .await
                .inspect_err(|e| {
                    log::error!("Database::add_file_bundles failed to add blob log: {e}")
                })?;
            tracer_b.off();

            tracer_f.on();
            file_txn
                .push(&File::from_insert_file(file, uid))
                .await
                .inspect_err(|e| {
                    log::error!("Database::add_file_bundles failed to add file log: {e}")
                })?;
            tracer_f.off();

            tracer_m.on();
            mat_txn.push(&materialisation).await.inspect_err(|e| {
                log::error!("Database::add_file_bundles failed to add materialisation log: {e}")
            })?;
            tracer_m.off();

            total_inserted += 1;

            if i % self.logs.flush_size == 0 && i > 0 {
                // in this order
                tracer_b.on();
                blob_txn.flush().await?;
                tracer_b.off();

                tracer_b.on();
                file_txn.flush().await?;
                tracer_b.off();

                tracer_b.on();
                mat_txn.flush().await?;
                tracer_b.off();
            }
        }

        tracer_b.measure();
        tracer_f.measure();
        tracer_m.measure();

        // in this order
        let tracer = Tracer::new_on("add_file_bundles::blob_txn::close");
        blob_txn.close().await?;
        tracer.measure();

        let tracer = Tracer::new_on("add_file_bundles::file_txn::close");
        file_txn.close().await?;
        tracer.measure();

        let tracer = Tracer::new_on("add_file_bundles::mat_txn::close");
        mat_txn.close().await?;
        tracer.measure();

        let tracer = Tracer::new_on("add_file_bundles::blob_bg::wait");
        blob_bg.await??;
        tracer.measure();

        let tracer = Tracer::new_on("add_file_bundles::file_bg::wait");
        file_bg.await??;
        tracer.measure();

        let tracer = Tracer::new_on("add_file_bundles::file_bg::wait");
        mat_bg.await??;
        tracer.measure();

        debug!(
            "file bundles added: attempted={} inserted={}",
            total_attempted, total_inserted
        );

        Ok(total_inserted)
    }

    pub async fn add_repository_names<S>(&self, s: S) -> Result<u64, DBError>
    where
        S: Stream<Item = InsertRepositoryName> + Send + Unpin,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;
        let txn = self.logs.repository_names_writer.transaction()?;
        let tail = txn
            .tail(TailFrom::Head)
            .track("add_repository_names::log_stream");
        let bg = self.repository_name_log_stream(tail);

        let ts = Utc::now().timestamp_nanos_opt().unwrap() as u64;
        let mut s = s.enumerate();
        while let Some((i, blob)) = s.next().await {
            total_attempted += 1;
            let uid = ts + (i as u64);
            txn.push(&RepositoryName {
                uid,
                repo_id: blob.repo_id.clone(),
                name: blob.name.clone(),
                valid_from: blob.valid_from,
            })
            .await
            .inspect_err(|e| log::error!("Database::add_blobs failed to add log: {e}"))?;
            total_inserted += 1;

            if i % self.logs.flush_size == 0 {
                txn.flush().await?;
            }
        }

        txn.close().await?;
        bg.await??;

        debug!(
            "repository_names added: attempted={} inserted={}",
            total_attempted, total_inserted
        );
        Ok(total_inserted)
    }

    pub async fn select_repositories(&self) -> DBOutputStream<'static, Repository> {
        self.kv
            .stream_repositories()
            .map_ok(|(repo_id, m)| Repository {
                repo_id,
                last_file_index: m.last_file_index,
                last_blob_index: m.last_blob_index,
                last_name_index: m.last_name_index,
            })
            .boxed()
    }

    pub async fn select_files(&self, last_index: Option<u64>) -> DBOutputStream<'static, File> {
        let reader = self.logs.files_writer.reader();
        let watermark = reader.snapshot_watermark();
        let from = Self::next_offset(last_index);
        reader
            .from(from)
            .take_while(move |r| {
                future::ready(match r {
                    Ok((idx, _)) => watermark.as_ref().is_some_and(|w| idx <= w),
                    Err(_) => true,
                })
            })
            .map(|r| match r {
                Ok((_, f)) => Ok(f),
                Err(e) => Err(e.into()),
            })
            .boxed()
            .track("db::select_files")
            .boxed()
    }

    pub async fn select_blobs(&self, last_index: Option<u64>) -> DBOutputStream<'static, Blob> {
        let reader = self.logs.blobs_writer.reader();
        let watermark = reader.snapshot_watermark();
        let from = match last_index {
            None => Offset::start(),
            Some(last_index) => Into::<Offset>::into(last_index).increment(),
        };
        reader
            .from(from)
            .take_while(move |r| {
                future::ready(match r {
                    Ok((idx, _)) => watermark.as_ref().is_some_and(|w| idx <= w),
                    Err(_) => true,
                })
            })
            .map(|r| match r {
                Ok((_, f)) => Ok(f),
                Err(e) => Err(e.into()),
            })
            .boxed()
            .track("db::select_blobs")
            .boxed()
    }

    pub async fn select_repository_names(
        &self,
        last_index: Option<u64>,
    ) -> DBOutputStream<'static, RepositoryName> {
        let reader = self.logs.repository_names_writer.reader();
        let watermark = reader.snapshot_watermark();
        let from = match last_index {
            None => Offset::start(),
            Some(last_index) => Into::<Offset>::into(last_index).increment(),
        };
        reader
            .from(from)
            .take_while(move |r| {
                future::ready(match r {
                    Ok((idx, _)) => watermark.as_ref().is_some_and(|w| idx <= w),
                    Err(_) => true,
                })
            })
            .map(|r| match r {
                Ok((_, name)) => Ok(name),
                Err(e) => Err(e.into()),
            })
            .boxed()
            .track("db::select_repository_names")
            .boxed()
    }

    pub async fn merge_repositories<S>(&self, s: S) -> Result<(), DBError>
    where
        S: Stream<Item = Repository> + Unpin + Send + 'static,
    {
        self.kv.apply_repositories(s.map(Ok)).await?;
        Ok(())
    }

    pub async fn merge_files<S>(&self, s: S) -> Result<(), DBError>
    where
        S: Stream<Item = File> + Unpin + Send,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;
        let txn = self.logs.files_writer.transaction()?;
        let tail = txn.tail(TailFrom::Head).track("merge_files::log_stream");
        let bg = self.file_log_stream(tail);

        let mut rb = RoaringTreemap::new();
        let mut s = s.enumerate();
        while let Some((i, file)) = s.next().await {
            total_attempted += 1;
            if rb.insert(file.uid) {
                txn.push(&file)
                    .await
                    .inspect_err(|e| log::error!("Database::merge_files failed to add log: {e}"))?;
                total_inserted += 1;
            }
            if i % self.logs.flush_size == 0 {
                txn.flush().await?;
            }
        }

        txn.close().await?;
        bg.await??;

        debug!(
            "files merged: attempted={} inserted={}",
            total_attempted, total_inserted
        );
        Ok(())
    }

    pub async fn merge_blobs<S>(&self, s: S) -> Result<(), DBError>
    where
        S: Stream<Item = Blob> + Unpin + Send,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;
        let txn = self.logs.blobs_writer.transaction()?;
        let tail = txn.tail(TailFrom::Head).track("merge_blobs::log_stream");
        let bg = self.blob_log_stream(tail);

        let mut rb = RoaringTreemap::new();
        let mut s = s.enumerate();
        while let Some((i, blob)) = s.next().await {
            total_attempted += 1;
            if rb.insert(blob.uid) {
                txn.push(&blob)
                    .await
                    .inspect_err(|e| log::error!("Database::merge_blobs failed to add log: {e}"))?;
                total_inserted += 1;
            }
            if i % self.logs.flush_size == 0 {
                txn.flush().await?;
            }
        }

        txn.close().await?;
        bg.await??;

        debug!(
            "blobs merged: attempted={} inserted={}",
            total_attempted, total_inserted
        );
        Ok(())
    }

    pub async fn merge_repository_names<S>(&self, s: S) -> Result<(), DBError>
    where
        S: Stream<Item = RepositoryName> + Unpin + Send,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;
        let txn = self.logs.repository_names_writer.transaction()?;
        let tail = txn
            .tail(TailFrom::Head)
            .track("merge_repository_names::log_stream");
        let bg = self.repository_name_log_stream(tail);

        let mut rb = RoaringTreemap::new();
        let mut s = s.enumerate();
        while let Some((i, repository_name)) = s.next().await {
            total_attempted += 1;
            if rb.insert(repository_name.uid) {
                txn.push(&repository_name).await.inspect_err(|e| {
                    log::error!("Database::merge_repository_names failed to add log: {e}")
                })?;
                total_inserted += 1;
            }
            if i % self.logs.flush_size == 0 {
                txn.flush().await?;
            }
        }

        txn.close().await?;
        bg.await??;

        debug!(
            "repository_names merged: attempted={} inserted={}",
            total_attempted, total_inserted
        );
        Ok(())
    }

    pub async fn lookup_repository(&self, repo_id: RepoID) -> Result<Repository, DBError> {
        let c: Vec<_> = self
            .kv
            .left_join_repositories(stream::iter([Ok::<_, DBError>(repo_id.clone())]), |r| r)
            .try_collect()
            .await?;
        let (_, rm) = c.into_iter().next().expect("at least one element");
        let rm = rm.unwrap_or(RepositoryMetadata {
            last_file_index: None,
            last_blob_index: None,
            last_name_index: None,
        });
        Ok(Repository {
            repo_id,
            last_file_index: rm.last_file_index,
            last_blob_index: rm.last_blob_index,
            last_name_index: rm.last_name_index,
        })
    }

    pub async fn update_last_indices(&self) -> Result<(), DBError> {
        let last_file_index = self.logs.files_writer.watermark();
        let last_blob_index = self.logs.blobs_writer.watermark();
        let last_name_index = self.logs.repository_names_writer.watermark();
        self.kv
            .apply_repositories(stream::iter([Ok(Repository {
                repo_id: self.get_or_create_current_repository().await?.repo_id,
                last_file_index: last_file_index.map(Into::into),
                last_blob_index: last_blob_index.map(Into::into),
                last_name_index: last_name_index.map(Into::into),
            })]))
            .await?;
        Ok(())
    }

    pub(crate) fn available_blobs<'a>(
        &self,
        repo_id: RepoID,
    ) -> impl Stream<Item = Result<AvailableBlob, DBError>> + Unpin + Send + 'static {
        use tokio_stream::StreamExt as TokioStreamExt;

        let s = self.kv.stream_current_blobs();
        TokioStreamExt::filter_map(s, {
            move |r| match r {
                Ok((blob_ref, blob)) => {
                    if blob_ref.repo_id == repo_id {
                        Some(Ok(AvailableBlob {
                            repo_id: repo_id.clone(),
                            blob_id: blob_ref.blob_id,
                            blob_size: blob.blob_size,
                            path: blob.blob_path.map(|p| p.0),
                        }))
                    } else {
                        None
                    }
                }
                Err(e) => Some(Err(e)),
            }
        })
        .boxed()
    }

    pub(crate) async fn missing_blobs(
        &self,
        repo_id: RepoID,
    ) -> impl Stream<Item = Result<BlobAssociatedToFiles, DBError>> + Unpin + Send + Sized + 'static
    {
        let repo_names: HashMap<_, _> = match self
            .kv
            .stream_current_repository_names()
            .map(|e| e.map(|(k, v)| (k, v.name)))
            .try_collect()
            .await
        {
            Ok(r) => r,
            Err(e) => return stream::iter(vec![Err(e)]).boxed(),
        };

        let s = self.kv.stream_current_files();

        // filter down to blobs which are not available locally
        let s = self.kv.left_join_current_blobs(s, move |(_, f)| BlobRef {
            blob_id: f.blob_id,
            repo_id: repo_id.clone(),
        });
        let s = s.filter_map(async |x| match x {
            Ok(((p, cf), None)) => Some(Ok((p, cf.blob_id))),
            Ok((_, Some(_))) => None,
            Err(e) => Some(Err(e)),
        });

        // get repo IDs with the blobs
        let repo_ids: Vec<_> = repo_names.keys().cloned().collect();
        let s = s.map(move |e: Result<(Path, BlobID), DBError>| {
            stream::iter(match e {
                Ok((p, b)) => repo_ids
                    .clone()
                    .into_iter()
                    .map(|r| Ok((p.clone(), b.clone(), r)))
                    .collect(),
                Err(e) => vec![Err(e)],
            })
        });
        let s = s.flatten();
        let s = self
            .kv
            .left_join_current_blobs(s, move |(_, b, r): (Path, BlobID, RepoID)| BlobRef {
                blob_id: b,
                repo_id: r,
            });

        // collect repo IDs per blob IDs again
        let s = group_by_key(s.boxed(), |e| match e {
            Ok(((p, b, _), _)) => Ok((p.clone(), b.clone())),
            Err(_) => Err(()),
        });

        let s = s
            .map(move |(e, group)| match e {
                Ok((p, b)) => Ok(BlobAssociatedToFiles {
                    blob_id: b,
                    path: p,
                    repositories_with_blob: group
                        .into_iter()
                        .map(Result::unwrap)
                        .filter_map(|((_, _, r), cf)| match cf {
                            Some(_) => Some(repo_names.get(&r).cloned().unwrap_or(r.0.clone())),
                            None => None,
                        })
                        .collect(),
                }),
                Err(()) => Err(group.into_iter().next().unwrap().unwrap_err()),
            })
            .boxed();

        s
    }

    pub async fn add_materialisations<S>(&self, s: S) -> Result<u64, DBError>
    where
        S: Stream<Item = InsertMaterialisation> + Send + Unpin,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;
        let txn = self.logs.materialisations_writer.transaction()?;
        let tail = txn
            .tail(TailFrom::Head)
            .track("add_materialisations::log_stream");
        let bg = self.materialisation_log_stream(tail);

        let mut s = s.enumerate();
        while let Some((i, mat)) = s.next().await {
            total_attempted += 1;
            txn.push(&mat).await.inspect_err(|e| {
                log::error!("Database::add_materialisations failed to add log: {e}")
            })?;
            total_inserted += 1;

            if i % self.logs.flush_size == 0 {
                txn.flush().await?;
            }
        }

        txn.close().await?;
        bg.await??;

        debug!(
            "materialisations added: attempted={} inserted={}",
            total_attempted, total_inserted
        );
        Ok(total_inserted)
    }

    pub async fn select_missing_files_on_virtual_filesystem(
        &self,
        last_seen_id: i64,
    ) -> DBOutputStream<'static, MissingFile> {
        use tokio_stream::StreamExt as TokioStreamExt;

        let repo_id = match self.get_or_create_current_repository().await {
            Ok(r) => r.repo_id,
            Err(e) => return stream::iter(vec![Err(e)]).boxed(),
        };

        let s = self.kv.stream_current_files();
        let s = self
            .kv
            .left_join_current_observations(s, |(p, _)| p.clone());
        let s = TokioStreamExt::filter_map(s, move |e| match e {
            Ok(((p, cf), co)) => {
                let was_file_observed = co.is_some_and(|o| o.fs_last_seen_id == last_seen_id);
                if !was_file_observed {
                    Some(Ok((p, cf.blob_id)))
                } else {
                    None
                }
            }
            Err(e) => Some(Err(e)),
        });
        let s = self.kv.left_join_current_blobs(s, move |(_, b)| BlobRef {
            blob_id: b.clone(),
            repo_id: repo_id.clone(),
        });
        TokioStreamExt::map(s, |e| {
            e.map(|((p, b), local_cb)| MissingFile {
                path: p,
                target_blob_id: b,
                local_has_target_blob: local_cb.is_some(),
            })
        })
        .boxed()
    }

    pub async fn add_virtual_filesystem_file_checked_events(
        &self,
        s: impl Stream<Item = FileCheck> + Unpin + Send + 'static,
    ) -> Result<u64, DBError> {
        let s = s.map(|e| Ok(e));
        Ok(self.kv.apply_file_checks(s).await?)
    }

    pub async fn add_virtual_filesystem_file_seen_events(
        &self,
        s: impl Stream<Item = FileSeen> + Unpin + Send + 'static,
    ) -> Result<u64, DBError> {
        let s = s.map(|e| Ok(e));
        Ok(self.kv.apply_file_seen(s).await?)
    }

    pub fn select_virtual_filesystem(
        &self,
        s: impl Stream<Item = FileSeen> + Unpin + Send + 'static,
        repo_id: RepoID,
    ) -> impl Stream<Item = Result<VirtualFile, DBError>> + Unpin + Send + 'static {
        let s = s.map(Ok);
        let s = self.kv.left_join_current_files(s, move |f| f.path);
        let s = self
            .kv
            .left_join_current_blobs(s, move |(_, cf)| BlobRef {
                blob_id: cf.map(|c| c.blob_id).unwrap_or(BlobID("".into())), // yz - this is wrong - needs some missing value
                repo_id: repo_id.clone(),
            })
            .map_ok(|((f, cf), cb)| (f, cf, cb));
        let s = self
            .kv
            .left_join_current_materialisations(s, move |(f, _, _)| f.path)
            .map_ok(|((f, cf, cb), cm)| (f, cf, cb, cm));
        let s = self
            .kv
            .left_join_current_check(s, move |(f, _, _, _)| f.path)
            .map_ok(|((f, cf, cb, cm), cc)| VirtualFile {
                file_seen: f,
                current_file: cf,
                current_blob: cb,
                current_materialisation: cm,
                current_check: cc,
            });

        s.boxed().track("Database::select_virtual_filesystem")
    }

    pub async fn add_connection(&self, connection: Connection) -> Result<(), DBError> {
        self.kv.store_connection(connection).await?;
        Ok(())
    }

    pub async fn connection_by_name(
        &self,
        name: ConnectionName,
    ) -> Result<Option<Connection>, DBError> {
        let s = stream::iter([Ok::<_, DBError>(name.clone())]);
        let c: Vec<_> = self
            .kv
            .left_join_connections(s, |n| n)
            .try_collect()
            .await?;
        let (_, cm) = c.into_iter().next().expect("left join");
        Ok(cm.map(|m| Connection {
            name,
            connection_type: m.connection_type,
            parameter: m.parameter,
        }))
    }

    pub async fn list_all_connections(&self) -> Result<Vec<Connection>, DBError> {
        self.kv
            .stream_connections()
            .map_ok(|(name, m)| Connection {
                name,
                connection_type: m.connection_type,
                parameter: m.parameter,
            })
            .try_collect()
            .await
    }

    pub(crate) async fn select_missing_blobs_for_transfer(
        &self,
        transfer_id: u32,
        remote_repo_id: RepoID,
        prefixes: Vec<String>,
    ) -> DBOutputStream<'static, BlobTransferItem> {
        use tokio_stream::StreamExt as TokioStreamExt;

        let repo_id = match self.get_or_create_current_repository().await {
            Ok(r) => r.repo_id,
            Err(e) => return stream::iter(vec![Err(e)]).boxed(),
        };

        let s = self.kv.stream_current_files();
        let s = TokioStreamExt::filter_map(s, move |e| match e {
            Ok((p, CurrentFile { blob_id, .. })) => {
                if prefixes.is_empty() || prefixes.iter().any(|prefix| p.0.starts_with(prefix)) {
                    Some(Ok((p, blob_id)))
                } else {
                    None
                }
            }
            Err(e) => Some(Err(e)),
        });

        let s = self.kv.left_join_current_blobs(s, move |(_, b)| BlobRef {
            blob_id: b.clone(),
            repo_id: repo_id.clone(),
        });
        let s = TokioStreamExt::filter_map(s, |e| match e {
            Ok(((p, b), None)) => Some(Ok((p, b))),
            Ok((_, Some(_))) => None,
            Err(e) => Some(Err(e)),
        });
        let s = self.kv.left_join_current_blobs(s, move |(_, b)| BlobRef {
            blob_id: b.clone(),
            repo_id: remote_repo_id.clone(),
        });
        TokioStreamExt::filter_map(s, move |e| match e {
            Ok(((_, b), Some(cb))) => {
                let blob_path = Path(b.path().to_string_lossy().to_string());
                Some(Ok(BlobTransferItem {
                    transfer_id,
                    blob_id: b,
                    path: blob_path,
                    blob_size: cb.blob_size,
                }))
            }
            Ok((_, None)) => None,
            Err(e) => Some(Err(e)),
        })
        .boxed()
    }

    pub(crate) async fn select_missing_files_for_transfer(
        &self,
        transfer_id: u32,
        local_repo_id: RepoID,
        remote_repo_id: RepoID,
        prefixes: Vec<String>,
    ) -> DBOutputStream<'static, FileTransferItem> {
        use tokio_stream::StreamExt as TokioStreamExt;

        let s = self.kv.stream_current_files();
        let s = TokioStreamExt::filter_map(s, move |e| match e {
            Ok((p, CurrentFile { blob_id, .. })) => {
                if prefixes.is_empty() || prefixes.iter().any(|prefix| p.0.starts_with(prefix)) {
                    Some(Ok((p, blob_id)))
                } else {
                    None
                }
            }
            Err(e) => Some(Err(e)),
        });

        let s = self.kv.left_join_current_blobs(s, move |(_, b)| BlobRef {
            blob_id: b.clone(),
            repo_id: local_repo_id.clone(),
        });
        let s = TokioStreamExt::filter_map(s, |e| match e {
            Ok(((p, b), None)) => Some(Ok((p, b))),
            Ok((_, Some(_))) => None,
            Err(e) => Some(Err(e)),
        });
        let s = self.kv.left_join_current_blobs(s, move |(_, b)| BlobRef {
            blob_id: b.clone(),
            repo_id: remote_repo_id.clone(),
        });

        TokioStreamExt::filter_map(s, move |e| match e {
            Ok(((p, b), Some(CurrentBlob { blob_size, .. }))) => Some(Ok(FileTransferItem {
                transfer_id,
                blob_id: b,
                blob_size,
                path: p,
            })),
            Ok((_, None)) => None,
            Err(e) => Some(Err(e)),
        })
        .boxed()
    }

    pub async fn select_current_files<'a>(
        &self,
        file_or_dir: String,
    ) -> DBOutputStream<'a, (Path, BlobID)> {
        use tokio_stream::StreamExt as TokioStreamExt;

        let s = stream::iter([Ok(Path(file_or_dir.clone()))]);
        let s = self.kv.left_join_current_files(s, |p| p);
        let s: Vec<_> = TokioStreamExt::collect(s).await;
        let (p, cf) = match s.into_iter().next().unwrap() {
            Ok(e) => e,
            Err(e) => return stream::iter([Err(e)]).boxed(),
        };

        match cf {
            Some(cf) => stream::iter([Ok((p, cf.blob_id))]).boxed(),
            None => {
                let s = self.kv.stream_current_files();
                let s = TokioStreamExt::filter_map(s, move |p| match p {
                    Ok((p, cf)) => match p.0.starts_with(&file_or_dir) {
                        true => Some(Ok((p, cf.blob_id))),
                        false => None,
                    },
                    Err(e) => Some(Err(e)),
                });
                s.boxed()
            }
        }
    }

    pub fn left_join_current_files<
        'a,
        K: Clone + Send + Sync + 'static,
        E: From<DBError> + Debug + Send + Sync + 'static,
    >(
        &self,
        s: impl Stream<Item = Result<K, E>> + Unpin + Send + 'static,
        key_func: impl Fn(K) -> Path + Sync + Send + 'static,
    ) -> BoxStream<'a, Result<(K, Option<CurrentFile>), E>> {
        self.kv.left_join_current_files(s, key_func).boxed()
    }

    fn blob_log_stream(
        &self,
        s: impl Stream<Item = Result<(Offset, Blob), StreamError>> + Unpin + Send + 'static,
    ) -> JoinHandle<Result<usize, DBError>> {
        use tokio_stream::StreamExt as TokioStreamExt;

        let inner = self.clone();
        tokio::spawn(async move {
            let counter = Arc::new(AtomicUsize::new(0));
            let max_offset = Arc::new(AtomicU64::new(0));

            let counter_clone = counter.clone();
            let max_offset_clone = max_offset.clone();
            let s = TokioStreamExt::map(s, move |e| match e {
                Ok((o, b)) => {
                    counter_clone.fetch_add(1, Ordering::Relaxed);
                    max_offset_clone.store(o.into(), Ordering::SeqCst);
                    Ok(b)
                }
                Err(e) => Err(e.into()),
            });
            inner.kv.apply_blobs(s).await?;

            let counter = counter.load(Ordering::Relaxed);
            if counter > 0 {
                let max_offset = max_offset.load(Ordering::SeqCst);
                inner
                    .kv
                    .apply_current_reductions(stream::iter([(
                        BLOB_TABLE_NAME.into(),
                        LogOffset(max_offset),
                    )]))
                    .await?;
            }

            Ok(counter)
        })
    }

    fn file_log_stream(
        &self,
        s: impl Stream<Item = Result<(Offset, File), StreamError>> + Unpin + Send + 'static,
    ) -> JoinHandle<Result<usize, DBError>> {
        use tokio_stream::StreamExt as TokioStreamExt;

        let inner = self.clone();
        tokio::spawn(async move {
            let counter = Arc::new(AtomicUsize::new(0));
            let max_offset = Arc::new(AtomicU64::new(0));

            let counter_clone = counter.clone();
            let max_offset_clone = max_offset.clone();
            let s = TokioStreamExt::map(s, move |e| match e {
                Ok((o, f)) => {
                    counter_clone.fetch_add(1, Ordering::Relaxed);
                    max_offset_clone.store(o.into(), Ordering::SeqCst);
                    Ok(f)
                }
                Err(e) => Err(e.into()),
            });
            inner.kv.apply_files(s).await?;

            let counter = counter.load(Ordering::Relaxed);
            if counter > 0 {
                let max_offset = max_offset.load(Ordering::SeqCst);
                inner
                    .kv
                    .apply_current_reductions(stream::iter([(
                        FILE_TABLE_NAME.into(),
                        LogOffset(max_offset),
                    )]))
                    .await?;
            }

            Ok(counter)
        })
    }

    fn materialisation_log_stream(
        &self,
        s: impl Stream<Item = Result<(Offset, InsertMaterialisation), StreamError>>
        + Unpin
        + Send
        + 'static,
    ) -> JoinHandle<Result<usize, DBError>> {
        use tokio_stream::StreamExt as TokioStreamExt;

        let inner = self.clone();
        tokio::spawn(async move {
            let counter = Arc::new(AtomicUsize::new(0));
            let max_offset = Arc::new(AtomicU64::new(0));

            let counter_clone = counter.clone();
            let max_offset_clone = max_offset.clone();
            let s = TokioStreamExt::map(s, move |e| match e {
                Ok((o, m)) => {
                    counter_clone.fetch_add(1, Ordering::Relaxed);
                    max_offset_clone.store(o.into(), Ordering::SeqCst);
                    Ok(m)
                }
                Err(e) => Err(e.into()),
            });
            inner.kv.apply_materialisations(s).await?;

            let counter = counter.load(Ordering::Relaxed);
            if counter > 0 {
                let max_offset = max_offset.load(Ordering::SeqCst);
                inner
                    .kv
                    .apply_current_reductions(stream::iter([(
                        MAT_TABLE_NAME.into(),
                        LogOffset(max_offset),
                    )]))
                    .await?;
            }

            Ok(counter)
        })
    }

    fn repository_name_log_stream(
        &self,
        s: impl Stream<Item = Result<(Offset, RepositoryName), StreamError>> + Unpin + Send + 'static,
    ) -> JoinHandle<Result<usize, DBError>> {
        use tokio_stream::StreamExt as TokioStreamExt;

        let inner = self.clone();
        tokio::spawn(async move {
            let counter = Arc::new(AtomicUsize::new(0));
            let max_offset = Arc::new(AtomicU64::new(0));

            let counter_clone = counter.clone();
            let max_offset_clone = max_offset.clone();
            let s = TokioStreamExt::map(s, move |e| match e {
                Ok((o, r)) => {
                    counter_clone.fetch_add(1, Ordering::Relaxed);
                    max_offset_clone.store(o.into(), Ordering::SeqCst);
                    Ok(r)
                }
                Err(e) => Err(e.into()),
            });
            inner.kv.apply_repository_names(s).await?;

            let counter = counter.load(Ordering::Relaxed);
            if counter > 0 {
                let max_offset = max_offset.load(Ordering::SeqCst);
                inner
                    .kv
                    .apply_current_reductions(stream::iter([(
                        NAME_TABLE_NAME.into(),
                        LogOffset(max_offset),
                    )]))
                    .await?;
            }

            Ok(counter)
        })
    }
}
