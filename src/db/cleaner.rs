use crate::db::models::WalCheckpoint;
use sqlx::SqlitePool;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{RwLock, RwLockReadGuard};

#[derive(Clone)]
pub struct Cleaner {
    pool: SqlitePool,
    row_threshold: usize,
    deadtime: std::time::Duration,
    row_counter: Arc<AtomicUsize>,
    last_cleanup: Arc<RwLock<Option<tokio::time::Instant>>>,
    long_running_stream_lock: Arc<RwLock<()>>,
    cleanup_in_progress_lock: Arc<RwLock<()>>,
}

const DEFAULT_CLEANUP_ROW_THRESHOLD: usize = 10000;
const DEFAULT_CLEANUP_DEADTIME: std::time::Duration = std::time::Duration::from_secs(2);

impl Cleaner {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            row_threshold: DEFAULT_CLEANUP_ROW_THRESHOLD,
            deadtime: DEFAULT_CLEANUP_DEADTIME,
            row_counter: Arc::new(AtomicUsize::new(0)),
            last_cleanup: Arc::new(RwLock::new(None)),
            long_running_stream_lock: Arc::new(RwLock::new(())),
            cleanup_in_progress_lock: Arc::new(RwLock::new(())),
        }
    }

    pub(crate) async fn try_periodic_cleanup(&self) -> RwLockReadGuard<()> {
        match self.long_running_stream_lock.try_write() {
            Ok(_g) => {
                let _cleanup_write_guard = self.cleanup_in_progress_lock.write().await;
                self.do_periodic_cleanup().await;
            }
            Err(_) => {
                log::debug!("skipping periodic cleanup because a long running stream is active");
            }
        };

        self.long_running_stream_lock.read().await
    }

    pub(crate) async fn periodic_cleanup(&self, n: usize) -> Option<RwLockReadGuard<()>> {
        let _guard = match self.long_running_stream_lock.try_write() {
            Ok(g) => g,
            Err(_) => {
                log::debug!("skipping periodic cleanup because a long running stream is active");
                return None;
            }
        };

        {
            let _cleanup_write_guard = self.cleanup_in_progress_lock.write().await;

            let rows = self.row_counter.load(Ordering::Relaxed);
            let is_in_deadtime = self
                .last_cleanup
                .read()
                .await
                .is_some_and(|t| t.elapsed() <= self.deadtime);

            if rows > self.row_threshold && !is_in_deadtime {
                self.do_periodic_cleanup().await;
            }
        }

        self.row_counter.fetch_add(n, Ordering::Relaxed);

        Some(self.cleanup_in_progress_lock.read().await)
    }

    async fn do_periodic_cleanup(&self) {
        log::debug!("triggered periodic cleanup");
        let result = sqlx::query_as::<_, WalCheckpoint>("PRAGMA wal_checkpoint(TRUNCATE);")
            .fetch_one(&self.pool)
            .await;
        match result {
            Ok(r) => log::warn!("cleanup result: {r:?}"),
            Err(e) => log::error!("DB cleanup failed: {e:?}"),
        }

        self.row_counter.store(0, Ordering::SeqCst);
        let mut guard = self.last_cleanup.write().await;
        *guard = Some(tokio::time::Instant::now());
    }
}
