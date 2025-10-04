create index blobs_blob_id_idx
    on blobs (blob_id);

create index blobs_repo_id_idx
    on blobs (repo_id);

create index files_path_idx
    on files (path);

create index files_path_blob_idx
    on files (path, blob_id);

create index materialisations_path_idx
    on materialisations (path);

create index materialisations_path_blob_idx
    on materialisations (path, blob_id);
