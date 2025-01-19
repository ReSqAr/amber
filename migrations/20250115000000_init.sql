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
    has_blob    INTEGER     NOT NULL,
    valid_from  DATETIME    NOT NULL
);
