-- Indices
CREATE INDEX IF NOT EXISTS blobs_blob_id_idx
    on blobs (blob_id);

CREATE INDEX IF NOT EXISTS blobs_repo_id_idx
    on blobs (repo_id);

CREATE INDEX IF NOT EXISTS files_path_idx
    on files (path);

CREATE INDEX IF NOT EXISTS files_path_blob_idx
    on files (path, blob_id);

CREATE INDEX IF NOT EXISTS materialisations_path_idx
    on materialisations (path);

CREATE INDEX IF NOT EXISTS materialisations_path_blob_idx
    on materialisations (path, blob_id);

CREATE INDEX IF NOT EXISTS virtual_filesystem_blob_id_idx
    on virtual_filesystem (target_blob_id);
