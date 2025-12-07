use crate::flightdeck::tracer::Tracer;
use rocksdb::DB;

pub(crate) struct DatabaseGuard {
    pub(crate) name: String,
    pub(crate) db: Option<DB>,
}

impl std::ops::Deref for DatabaseGuard {
    type Target = Option<DB>;
    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl std::ops::DerefMut for DatabaseGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.db
    }
}

impl DatabaseGuard {
    pub(crate) fn close_sync(&mut self) {
        let db = self.db.take();
        let name = self.name.clone();
        let tracer = Tracer::new_on(format!("KVStore({})::close", name));
        drop(db);
        tracer.measure();
    }
}

impl Drop for DatabaseGuard {
    fn drop(&mut self) {
        if let Some(_db) = self.db.take() {
            if cfg!(feature = "__kv-drop-dev-assert") {
                panic!("Use ::close instead of relying on the default Drop behaviour");
            } else {
                log::error!("Use ::close instead of relying on the default Drop behaviour");
                let tracer = Tracer::new_on(format!("KVStore({})::drop", self.name));
                drop(_db);
                tracer.measure();
            }
        }
    }
}
