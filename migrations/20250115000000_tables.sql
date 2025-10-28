-- table: current_repository
CREATE TABLE current_repository
(
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_id TEXT NOT NULL
);

-- table: repositories
CREATE TABLE repositories
(
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_id         TEXT UNIQUE NOT NULL,
    last_file_index INTEGER     NOT NULL DEFAULT -1,
    last_blob_index INTEGER     NOT NULL DEFAULT -1,
    last_name_index INTEGER     NOT NULL DEFAULT -1
);


CREATE TABLE latest_available_blobs
(
    repo_id     TEXT    NOT NULL,
    blob_id     TEXT    NOT NULL,
    blob_size   INTEGER NOT NULL,
    has_blob    INTEGER NOT NULL,
    path        TEXT,
    valid_from  DATETIME NOT NULL,
    PRIMARY KEY (repo_id, blob_id)
);

CREATE TABLE latest_filesystem_files
(
    path        TEXT PRIMARY KEY,
    blob_id     TEXT,
    valid_from  DATETIME NOT NULL
);

CREATE TABLE latest_materialisations
(
    path        TEXT PRIMARY KEY,
    blob_id     TEXT,
    valid_from  DATETIME NOT NULL
);

CREATE TABLE latest_repository_names
(
    repo_id     TEXT PRIMARY KEY,
    name        TEXT     NOT NULL,
    valid_from  DATETIME NOT NULL
);

CREATE TABLE latest_reductions
(
    table_name TEXT PRIMARY KEY,
    offset     INTEGER NOT NULL
);

-- table: virtual_filesystem
CREATE TABLE virtual_filesystem (
    id                            INTEGER PRIMARY KEY AUTOINCREMENT,
    path                          TEXT UNIQUE NOT NULL,
    fs_last_seen_id               INTEGER,
    fs_last_seen_dttm             INTEGER,
    fs_last_modified_dttm         INTEGER,
    fs_last_size                  INTEGER,
    materialisation_last_blob_id  TEXT,
    target_blob_id                TEXT,
    target_blob_size              INTEGER,
    local_has_target_blob         BOOLEAN NOT NULL DEFAULT FALSE,
    check_last_dttm               INTEGER,
    check_last_hash               TEXT
);

-- table: connections
CREATE TABLE connections
(
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    name             TEXT UNIQUE NOT NULL,
    connection_type  TEXT        NOT NULL,
    parameter        TEXT        NOT NULL
);

-- table: transfers
CREATE TABLE transfers
(
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    transfer_id      INTEGER NOT NULL,
    blob_id          TEXT    NOT NULL,
    blob_size        INTEGER NOT NULL,
    path             TEXT    NOT NULL
);