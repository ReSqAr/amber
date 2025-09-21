CREATE SEQUENCE IF NOT EXISTS current_repository_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS repositories_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS files_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS blobs_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS repository_names_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS materialisations_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS virtual_filesystem_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS connections_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS transfers_id_seq START 1;

CREATE TABLE IF NOT EXISTS current_repository(
    id BIGINT PRIMARY KEY DEFAULT nextval('current_repository_id_seq'),
    repo_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS repositories(
    id BIGINT PRIMARY KEY DEFAULT nextval('repositories_id_seq'),
    repo_id TEXT UNIQUE NOT NULL,
    last_file_index INTEGER NOT NULL DEFAULT -1,
    last_blob_index INTEGER NOT NULL DEFAULT -1,
    last_name_index INTEGER NOT NULL DEFAULT -1
);

CREATE TABLE IF NOT EXISTS files(
    id BIGINT PRIMARY KEY DEFAULT nextval('files_id_seq'),
    uuid TEXT UNIQUE NOT NULL,
    path TEXT NOT NULL,
    blob_id TEXT,
    valid_from TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS blobs(
    id BIGINT PRIMARY KEY DEFAULT nextval('blobs_id_seq'),
    uuid TEXT UNIQUE NOT NULL,
    repo_id TEXT NOT NULL,
    blob_id TEXT NOT NULL,
    blob_size BIGINT NOT NULL,
    has_blob BOOLEAN NOT NULL,
    path TEXT,
    valid_from TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS repository_names(
    id BIGINT PRIMARY KEY DEFAULT nextval('repository_names_id_seq'),
    uuid TEXT UNIQUE NOT NULL,
    repo_id TEXT NOT NULL,
    name TEXT NOT NULL,
    valid_from TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS materialisations(
    id BIGINT PRIMARY KEY DEFAULT nextval('materialisations_id_seq'),
    path TEXT NOT NULL,
    blob_id TEXT,
    valid_from TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS virtual_filesystem(
    id BIGINT PRIMARY KEY DEFAULT nextval('virtual_filesystem_id_seq'),
    path TEXT UNIQUE NOT NULL,
    fs_last_seen_id BIGINT,
    fs_last_seen_dttm BIGINT,
    fs_last_modified_dttm BIGINT,
    fs_last_size BIGINT,
    materialisation_last_blob_id TEXT,
    target_blob_id TEXT,
    target_blob_size BIGINT,
    local_has_target_blob BOOLEAN NOT NULL DEFAULT FALSE,
    check_last_dttm BIGINT,
    check_last_hash TEXT
    );

CREATE TABLE IF NOT EXISTS connections(
    id BIGINT PRIMARY KEY DEFAULT nextval('connections_id_seq'),
    name TEXT UNIQUE NOT NULL,
    connection_type TEXT NOT NULL,
    parameter TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transfers(
    id BIGINT PRIMARY KEY DEFAULT nextval('transfers_id_seq'),
    transfer_id BIGINT NOT NULL,
    blob_id TEXT NOT NULL,
    blob_size BIGINT NOT NULL,
    path TEXT NOT NULL
);