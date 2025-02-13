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
    last_blob_index INTEGER     NOT NULL DEFAULT -1
);

-- table: files
CREATE TABLE files
(
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid       TEXT UNIQUE NOT NULL,
    path       TEXT        NOT NULL,
    blob_id    TEXT,
    valid_from DATETIME    NOT NULL
);

-- table: blobs
CREATE TABLE blobs
(
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid        TEXT UNIQUE NOT NULL,
    repo_id     TEXT        NOT NULL,
    blob_id     TEXT        NOT NULL,
    blob_size   INTEGER     NOT NULL,
    has_blob    INTEGER     NOT NULL,
    valid_from  DATETIME    NOT NULL
);

-- table: materialisations
CREATE TABLE materialisations (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    path        TEXT        NOT NULL,
    blob_id     TEXT        NOT NULL,
    valid_from  DATETIME    NOT NULL
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
    path             TEXT    NOT NULL
);