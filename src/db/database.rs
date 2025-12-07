use crate::db::error::DBError;
use crate::db::kv::KVStores;
use crate::db::models::{
    AvailableBlob, Blob, BlobAssociatedToFiles, BlobID, BlobMeta, BlobRef, BlobTransferItem,
    Connection, ConnectionName, CurrentFile, File, FileBlobID, FileCheck, FileSeen,
    FileTransferItem, HasBlob, InsertBlob, InsertFile, InsertFileBundle, InsertMaterialisation,
    InsertRepositoryName, LocalRepository, MissingFile, Path, RepoID, Repository,
    RepositoryMetadata, RepositoryName, Uid, VirtualFile,
};
use crate::db::reduced;
use crate::db::reduced::Reduced;
use crate::db::stores::log::Offset;
use crate::db::stores::reduced::{Transaction, TransactionLike, UidState, ValidFrom};
use crate::flightdeck;
use crate::flightdeck::tracer::Tracer;
use crate::flightdeck::tracked::sender::{Adapter, TrackedSender};
use crate::flightdeck::tracked::stream::Trackable;
use crate::utils::stream::group_by_key;
use chrono::Utc;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt, stream};
use std::collections::HashMap;
use std::fmt::Debug;
use std::future;
use tokio::try_join;
use uuid::Uuid;

#[derive(Clone)]
pub struct Database {
    kv: KVStores,
    logs: Reduced,
}

impl Database {
    pub fn new(kv: KVStores, logs: Reduced) -> Self {
        Self { kv, logs }
    }

    pub async fn clean(&self) -> Result<(), DBError> {
        let tracer = Tracer::new_on("Database::clean");

        let b = async {
            let txn = self.logs.blobs.transaction().await?;
            txn.close().await?;
            Ok::<_, DBError>(())
        };
        let f = async {
            let txn = self.logs.files.transaction().await?;
            txn.close().await?;
            Ok::<_, DBError>(())
        };
        let rn = async {
            let txn = self.logs.repository_names.transaction().await?;
            txn.close().await?;
            Ok::<_, DBError>(())
        };

        try_join!(b, f, rn)?;

        tracer.measure();
        Ok(())
    }

    pub async fn close(&self) -> Result<(), DBError> {
        try_join!(self.kv.close(), self.logs.close())?;
        Ok(())
    }

    pub async fn compact(&self) -> Result<(), DBError> {
        try_join!(self.kv.compact(), self.logs.compact())?;
        Ok(())
    }

    fn next_offset(i: Option<impl Into<Offset>>) -> Offset {
        match i {
            None => Offset::start(),
            Some(last_index) => Into::<Offset>::into(last_index).increment(),
        }
    }

    pub async fn get_or_create_current_repository(&self) -> Result<LocalRepository, DBError> {
        match self.kv.load_local_repository().await? {
            Some(current) => Ok(current),
            None => {
                let repo_id = RepoID(Uuid::new_v4().to_string());
                let current = LocalRepository { repo_id };
                self.kv.store_current_repository(current).await?;
                Ok(self.kv.load_local_repository().await?.expect("was set"))
            }
        }
    }

    pub async fn lookup_current_repository_name(
        &self,
        repo_id: RepoID,
    ) -> Result<Option<String>, DBError> {
        let s = self
            .logs
            .repository_names
            .left_join_current(stream::iter(vec![Ok::<_, DBError>(repo_id)]).boxed(), |r| r);
        let r: Vec<_> = s.collect().await;

        assert_eq!(r.len(), 1, "expected precisely 1 repository lookup result");
        let (_, result) = r.into_iter().next().unwrap()?;
        Ok(result.map(|(r, _)| r.name))
    }

    pub async fn add_blobs(&self, s: BoxStream<'_, InsertBlob>) -> Result<u64, DBError> {
        use tokio_stream::StreamExt as TokioStreamExt;

        let ts = Utc::now().timestamp_nanos_opt().unwrap() as u64;
        let s = TokioStreamExt::map(s.enumerate(), |(i, b)| {
            let (k, v, s, vf) = b.into();
            Ok((Uid(ts + (i as u64)), k, v, s, vf))
        });

        reduced::add(
            s.boxed(),
            self.logs.blobs.transaction().await?,
            self.logs.blobs.name().clone(),
            self.logs.flush_size,
        )
        .await
    }

    pub async fn add_files(&self, s: BoxStream<'_, InsertFile>) -> Result<u64, DBError> {
        use tokio_stream::StreamExt as TokioStreamExt;

        let ts = Utc::now().timestamp_nanos_opt().unwrap() as u64;
        let s = TokioStreamExt::map(s.enumerate(), |(i, f)| {
            let (k, v, s, vf) = f.into();
            Ok((Uid(ts + (i as u64)), k, v, s, vf))
        });

        reduced::add(
            s.boxed(),
            self.logs.files.transaction().await?,
            self.logs.files.name().clone(),
            self.logs.flush_size,
        )
        .await
    }

    pub async fn add_file_bundles(
        &self,
        s: BoxStream<'_, InsertFileBundle>,
    ) -> Result<u64, DBError> {
        use tokio_stream::StreamExt as TokioStreamExt;

        let ts = Utc::now().timestamp_nanos_opt().unwrap() as u64;
        let s = TokioStreamExt::map(
            s.enumerate(),
            |(
                i,
                InsertFileBundle {
                    file,
                    blob,
                    materialisation,
                },
            )| {
                let (k, v, s, vf) = blob.into();
                let blob = (Uid(ts + (i as u64)), k, v, s, vf);
                let (k, v, s, vf) = file.into();
                let file = (Uid(ts + (i as u64)), k, v, s, vf);
                Ok((blob, file, materialisation))
            },
        );

        let (tx, rx) = flightdeck::tracked::mpsc_channel("Database::add_file_bundles::mat", 100);
        let bg_mat = self.kv.apply_materialisations(rx.boxed());

        let txn = FileBundleTransaction {
            blob_txn: self.logs.blobs.transaction().await?,
            file_txn: self.logs.files.transaction().await?,
            mat_tx: tx,
        };
        let bg_add = reduced::add(
            s.boxed(),
            txn,
            "file_bundles".to_string(),
            self.logs.flush_size,
        );

        let (c, _) = try_join!(bg_add, bg_mat)?;
        Ok(c)
    }

    pub async fn add_repository_names(
        &self,
        s: BoxStream<'_, InsertRepositoryName>,
    ) -> Result<u64, DBError> {
        use tokio_stream::StreamExt as TokioStreamExt;

        let ts = Utc::now().timestamp_nanos_opt().unwrap() as u64;
        let s = TokioStreamExt::map(s.enumerate(), |(i, rn)| {
            let (k, v, s, vf) = rn.into();
            Ok((Uid(ts + (i as u64)), k, v, s, vf))
        });

        reduced::add(
            s.boxed(),
            self.logs.repository_names.transaction().await?,
            self.logs.repository_names.name().clone(),
            self.logs.flush_size,
        )
        .await
    }

    pub async fn select_blobs(
        &self,
        last_index: Option<u64>,
    ) -> BoxStream<'static, Result<Blob, DBError>> {
        let watermark = self.logs.blobs.watermark();
        let from = Self::next_offset(last_index);
        self.logs
            .blobs
            .select(from)
            .await
            .take_while(move |r| {
                future::ready(match r {
                    Ok((idx, _)) => watermark.as_ref().is_some_and(|w| idx <= w),
                    Err(_) => true,
                })
            })
            .map_ok(|(_, t)| t.into())
            .boxed()
            .track("db::select_blobs")
            .boxed()
    }

    pub async fn select_files(
        &self,
        last_index: Option<u64>,
    ) -> BoxStream<'static, Result<File, DBError>> {
        let watermark = self.logs.files.watermark();
        let from = Self::next_offset(last_index);
        self.logs
            .files
            .select(from)
            .await
            .take_while(move |r| {
                future::ready(match r {
                    Ok((idx, _)) => watermark.as_ref().is_some_and(|w| idx <= w),
                    Err(_) => true,
                })
            })
            .map_ok(|(_, t)| t.into())
            .boxed()
            .track("db::select_files")
            .boxed()
    }

    pub async fn select_repository_names(
        &self,
        last_index: Option<u64>,
    ) -> BoxStream<'static, Result<RepositoryName, DBError>> {
        let watermark = self.logs.repository_names.watermark();
        let from = Self::next_offset(last_index);
        self.logs
            .repository_names
            .select(from)
            .await
            .take_while(move |r| {
                future::ready(match r {
                    Ok((idx, _)) => watermark.as_ref().is_some_and(|w| idx <= w),
                    Err(_) => true,
                })
            })
            .map_ok(|(_, t)| t.into())
            .boxed()
            .track("db::select_repository_names")
            .boxed()
    }

    pub async fn merge_blobs(&self, s: BoxStream<'static, Blob>) -> Result<(), DBError> {
        use tokio_stream::StreamExt as TokioStreamExt;

        let s = TokioStreamExt::map(s, Ok);
        let s = self
            .logs
            .blobs
            .left_join_known_uids(s.boxed(), |rn: Blob| rn.uid)
            .boxed();
        let s = TokioStreamExt::filter_map(s, |e| match e {
            Ok((_, UidState::Known)) => None,
            Ok((b, UidState::Unknown)) => Some(Ok(b.into())),
            Err(e) => Some(Err(e)),
        });

        reduced::add(
            s.boxed(),
            self.logs.blobs.transaction().await?,
            self.logs.blobs.name().clone(),
            self.logs.flush_size,
        )
        .await?;
        Ok(())
    }

    pub async fn merge_files(&self, s: BoxStream<'static, File>) -> Result<(), DBError> {
        use tokio_stream::StreamExt as TokioStreamExt;

        let s = TokioStreamExt::map(s, Ok);
        let s = self
            .logs
            .files
            .left_join_known_uids(s.boxed(), |rn: File| rn.uid)
            .boxed();
        let s = TokioStreamExt::filter_map(s, |e| match e {
            Ok((_, UidState::Known)) => None,
            Ok((f, UidState::Unknown)) => Some(Ok(f.into())),
            Err(e) => Some(Err(e)),
        });

        reduced::add(
            s.boxed(),
            self.logs.files.transaction().await?,
            self.logs.files.name().clone(),
            self.logs.flush_size,
        )
        .await?;
        Ok(())
    }

    pub async fn merge_repository_names(
        &self,
        s: BoxStream<'static, RepositoryName>,
    ) -> Result<(), DBError> {
        use tokio_stream::StreamExt as TokioStreamExt;

        let s = TokioStreamExt::map(s, Ok);
        let s = self
            .logs
            .repository_names
            .left_join_known_uids(s.boxed(), |rn: RepositoryName| rn.uid)
            .boxed();
        let s = TokioStreamExt::filter_map(s, |e| match e {
            Ok((_, UidState::Known)) => None,
            Ok((rn, UidState::Unknown)) => Some(Ok(rn.into())),
            Err(e) => Some(Err(e)),
        });

        reduced::add(
            s.boxed(),
            self.logs.repository_names.transaction().await?,
            self.logs.repository_names.name().clone(),
            self.logs.flush_size,
        )
        .await?;
        Ok(())
    }

    pub async fn select_repositories(&self) -> BoxStream<'static, Result<Repository, DBError>> {
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

    pub async fn merge_repositories(&self, s: BoxStream<'_, Repository>) -> Result<(), DBError> {
        self.kv.apply_repositories(s.map(Ok).boxed()).await?;
        Ok(())
    }

    pub async fn lookup_repository(&self, repo_id: RepoID) -> Result<Repository, DBError> {
        let c: Vec<_> = self
            .kv
            .left_join_repositories(
                stream::iter([Ok::<_, DBError>(repo_id.clone())]).boxed(),
                |r| r,
            )
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
        let tracer = Tracer::new_on("Database::update_last_indices");
        let last_file_index = self.logs.files.watermark();
        let last_blob_index = self.logs.blobs.watermark();
        let last_name_index = self.logs.repository_names.watermark();
        self.kv
            .apply_repositories(
                stream::iter([Ok(Repository {
                    repo_id: self.get_or_create_current_repository().await?.repo_id,
                    last_file_index: last_file_index.map(Into::into),
                    last_blob_index: last_blob_index.map(Into::into),
                    last_name_index: last_name_index.map(Into::into),
                })])
                .boxed(),
            )
            .await?;

        tracer.measure();
        Ok(())
    }

    pub(crate) fn available_blobs(
        &self,
        repo_id: RepoID,
    ) -> BoxStream<'static, Result<AvailableBlob, DBError>> {
        use tokio_stream::StreamExt as TokioStreamExt;

        let s = self.logs.blobs.current();
        TokioStreamExt::filter_map(s, {
            move |r| match r {
                Ok((
                    blob_ref,
                    BlobMeta {
                        path: blob_path,
                        size: blob_size,
                    },
                    (),
                )) => {
                    if blob_ref.repo_id == repo_id {
                        Some(Ok(AvailableBlob {
                            repo_id: repo_id.clone(),
                            blob_id: blob_ref.blob_id,
                            blob_size,
                            path: blob_path.map(|p| p.0),
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
    ) -> BoxStream<'static, Result<BlobAssociatedToFiles, DBError>> {
        let repo_names: HashMap<_, _> = match self
            .logs
            .repository_names
            .current()
            .map(|e| e.map(|(k, v, _)| (k, v.name)))
            .try_collect()
            .await
        {
            Ok(r) => r,
            Err(e) => return stream::iter(vec![Err(e)]).boxed(),
        };

        let s = self.logs.files.current();

        // filter down to blobs which are not available locally
        let s = self
            .logs
            .blobs
            .left_join_current(s, move |(_, _, blob_id)| BlobRef {
                blob_id,
                repo_id: repo_id.clone(),
            });
        let s = s.filter_map(async |x| match x {
            Ok(((p, _, blob_id), None)) => Some(Ok((p, blob_id))),
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
        let s = s.flatten().boxed();
        let s = self
            .logs
            .blobs
            .left_join_current(s, move |(_, b, r): (Path, BlobID, RepoID)| BlobRef {
                blob_id: b,
                repo_id: r,
            });

        // collect repo IDs per blob IDs again
        let s = group_by_key(s.boxed(), |e| match e {
            Ok(((p, b, _), _)) => Ok((p.clone(), b.clone())),
            Err(_) => Err(()),
        });

        s.map(move |(e, group)| match e {
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
        .boxed()
    }

    pub async fn add_materialisations(
        &self,
        s: BoxStream<'_, InsertMaterialisation>,
    ) -> Result<u64, DBError> {
        self.kv.apply_materialisations(s.map(Ok).boxed()).await
    }

    pub async fn select_missing_files_on_virtual_filesystem(
        &self,
        last_seen_id: i64,
    ) -> BoxStream<'static, Result<MissingFile, DBError>> {
        use tokio_stream::StreamExt as TokioStreamExt;

        let repo_id = match self.get_or_create_current_repository().await {
            Ok(r) => r.repo_id,
            Err(e) => return stream::iter(vec![Err(e)]).boxed(),
        };

        let s = self.logs.files.current();
        let s = self.kv.left_join_observations(s, |(p, _, _)| p.clone());
        let s = TokioStreamExt::filter_map(s, move |e| match e {
            Ok(((p, _, blob_id), co)) => {
                let was_file_observed = co.is_some_and(|o| o.fs_last_seen_id == last_seen_id);
                if !was_file_observed {
                    Some(Ok((p, blob_id)))
                } else {
                    None
                }
            }
            Err(e) => Some(Err(e)),
        })
        .boxed();
        let s = self.logs.blobs.left_join_current(s, move |(_, b)| BlobRef {
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
        s: BoxStream<'static, FileCheck>,
    ) -> Result<u64, DBError> {
        let s = s.map(Ok).boxed();
        self.kv.apply_file_checks(s).await
    }

    pub async fn add_virtual_filesystem_file_seen_events(
        &self,
        s: BoxStream<'static, FileSeen>,
    ) -> Result<u64, DBError> {
        let s = s.map(Ok).boxed();
        self.kv.apply_file_seen(s).await
    }

    pub fn select_virtual_filesystem(
        &self,
        s: BoxStream<'static, FileSeen>,
        repo_id: RepoID,
    ) -> BoxStream<'static, Result<VirtualFile, DBError>> {
        let s = s.map(Ok).boxed();
        let s = self.logs.files.left_join_current(s, move |f| f.path);
        let s = self
            .logs
            .blobs
            .left_join_current(s, move |(_, cf)| BlobRef {
                blob_id: cf.map(|(_, b)| b).unwrap_or(BlobID("".into())), // yz - this is wrong - needs some missing value
                repo_id: repo_id.clone(),
            })
            .map_ok(|((f, cf), cb)| (f, cf, cb))
            .boxed();
        let s = self
            .kv
            .left_join_materialisations(s, move |(f, _, _)| f.path)
            .map_ok(|((f, cf, cb), cm)| (f, cf, cb, cm))
            .boxed();
        let s = self
            .kv
            .left_join_check(s, move |(f, _, _, _)| f.path)
            .map_ok(|((f, cf, cb, cm), cc)| VirtualFile {
                file_seen: f,
                current_file: cf.map(|(_, b)| CurrentFile { blob_id: b }),
                current_blob: cb.map(|(b, _)| BlobMeta {
                    size: b.size,
                    path: b.path,
                }),
                current_materialisation: cm,
                current_check: cc,
            })
            .boxed();

        s.track("Database::select_virtual_filesystem").boxed()
    }

    pub async fn add_connection(&self, connection: Connection) -> Result<(), DBError> {
        self.kv.store_connection(connection).await?;
        Ok(())
    }

    pub async fn connection_by_name(
        &self,
        name: ConnectionName,
    ) -> Result<Option<Connection>, DBError> {
        let s = stream::iter([Ok::<_, DBError>(name.clone())]).boxed();
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
    ) -> BoxStream<'static, Result<BlobTransferItem, DBError>> {
        use tokio_stream::StreamExt as TokioStreamExt;

        let repo_id = match self.get_or_create_current_repository().await {
            Ok(r) => r.repo_id,
            Err(e) => return stream::iter(vec![Err(e)]).boxed(),
        };

        let s = self.logs.files.current();
        let s = TokioStreamExt::filter_map(s, move |e| match e {
            Ok((p, (), blob_id)) => {
                if prefixes.is_empty() || prefixes.iter().any(|prefix| p.0.starts_with(prefix)) {
                    Some(Ok((p, blob_id)))
                } else {
                    None
                }
            }
            Err(e) => Some(Err(e)),
        })
        .boxed();

        let s = self
            .logs
            .blobs
            .left_join_current(s, move |(_, b)| BlobRef {
                blob_id: b.clone(),
                repo_id: repo_id.clone(),
            })
            .boxed();
        let s = TokioStreamExt::filter_map(s, |e| match e {
            Ok(((p, b), None)) => Some(Ok((p, b))),
            Ok((_, Some(_))) => None,
            Err(e) => Some(Err(e)),
        })
        .boxed();
        let s = self.logs.blobs.left_join_current(s, move |(_, b)| BlobRef {
            blob_id: b.clone(),
            repo_id: remote_repo_id.clone(),
        });
        TokioStreamExt::filter_map(s, move |e| match e {
            Ok(((_, b), Some((lb, _)))) => {
                let blob_path = Path(b.path().to_string_lossy().to_string());
                Some(Ok(BlobTransferItem {
                    transfer_id,
                    blob_id: b,
                    path: blob_path,
                    blob_size: lb.size,
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
    ) -> BoxStream<'static, Result<FileTransferItem, DBError>> {
        use tokio_stream::StreamExt as TokioStreamExt;

        let s = self.logs.files.current();
        let s = TokioStreamExt::filter_map(s, move |e| match e {
            Ok((p, (), blob_id)) => {
                if prefixes.is_empty() || prefixes.iter().any(|prefix| p.0.starts_with(prefix)) {
                    Some(Ok((p, blob_id)))
                } else {
                    None
                }
            }
            Err(e) => Some(Err(e)),
        })
        .boxed();

        let s = self.logs.blobs.left_join_current(s, move |(_, b)| BlobRef {
            blob_id: b.clone(),
            repo_id: local_repo_id.clone(),
        });
        let s = TokioStreamExt::filter_map(s, |e| match e {
            Ok(((p, b), None)) => Some(Ok((p, b))),
            Ok((_, Some(_))) => None,
            Err(e) => Some(Err(e)),
        })
        .boxed();
        let s = self.logs.blobs.left_join_current(s, move |(_, b)| BlobRef {
            blob_id: b.clone(),
            repo_id: remote_repo_id.clone(),
        });

        TokioStreamExt::filter_map(s, move |e| match e {
            Ok(((p, b), Some((lb, ())))) => Some(Ok(FileTransferItem {
                transfer_id,
                blob_id: b,
                blob_size: lb.size,
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
    ) -> BoxStream<'a, Result<(Path, BlobID), DBError>> {
        use tokio_stream::StreamExt as TokioStreamExt;

        let s = stream::iter([Ok(Path(file_or_dir.clone()))]).boxed();
        let s = self.logs.files.left_join_current(s, |p| p);
        let s: Vec<_> = TokioStreamExt::collect(s).await;
        let (p, cf) = match s.into_iter().next().unwrap() {
            Ok(e) => e,
            Err(e) => return stream::iter([Err(e)]).boxed(),
        };

        match cf {
            Some(((), b)) => stream::iter([Ok((p, b))]).boxed(),
            None => {
                let s = self.logs.files.current();
                let s = TokioStreamExt::filter_map(s, move |p| match p {
                    Ok((p, (), b)) => match p.0.starts_with(&file_or_dir) {
                        true => Some(Ok((p, b))),
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
        s: BoxStream<'static, Result<K, E>>,
        key_func: impl Fn(K) -> Path + Sync + Send + 'static,
    ) -> BoxStream<'a, Result<(K, Option<CurrentFile>), E>> {
        self.logs
            .files
            .left_join_current(s, key_func)
            .map_ok(|(k, f)| {
                let v = f.map(|(_, blob_id)| CurrentFile { blob_id });
                (k, v)
            })
            .boxed()
    }
}

struct FileBundleTransaction {
    blob_txn: Transaction<BlobRef, BlobMeta, HasBlob>,
    file_txn: Transaction<Path, (), FileBlobID>,
    mat_tx: TrackedSender<Result<InsertMaterialisation, DBError>, Adapter>,
}

impl
    TransactionLike<(
        (Uid, BlobRef, BlobMeta, HasBlob, ValidFrom),
        (Uid, Path, (), FileBlobID, ValidFrom),
        InsertMaterialisation,
    )> for FileBundleTransaction
{
    async fn put(
        &mut self,
        (b, f, m): &(
            (Uid, BlobRef, BlobMeta, HasBlob, ValidFrom),
            (Uid, Path, (), FileBlobID, ValidFrom),
            InsertMaterialisation,
        ),
    ) -> Result<(), DBError> {
        self.blob_txn.put(b).await?;
        self.file_txn.put(f).await?;
        self.mat_tx.send(Ok(m.clone())).await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), DBError> {
        self.blob_txn.flush().await?;
        self.file_txn.flush().await?;
        Ok(())
    }

    async fn close(self) -> Result<(), DBError> {
        self.blob_txn.close().await?;
        self.file_txn.close().await?;
        drop(self.mat_tx);
        Ok(())
    }
}
