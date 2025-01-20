create index blobs_blob_id_idx
    on blobs (blob_id);

create index blobs_repo_id_idx
    on blobs (repo_id);

create index files_path_idx
    on files (path);

create index virtual_filesystem_blob_id_idx
    on virtual_filesystem (blob_id);

