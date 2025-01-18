-- view: latest_filesystem_files
CREATE VIEW latest_filesystem_files AS
WITH versioned_files AS (
    SELECT
        path,
        object_id,
        valid_from,
        ROW_NUMBER() OVER (
            PARTITION BY path
            ORDER BY valid_from DESC, object_id DESC
        ) AS rn
    FROM files
),
latest_file_version AS (
    SELECT
        path,
        object_id
    FROM versioned_files
    WHERE rn = 1
)
SELECT
    path,
    object_id
FROM latest_file_version
WHERE object_id IS NOT NULL;

-- view: latest_available_blobs
CREATE VIEW latest_available_blobs AS
WITH versioned_blobs AS (
    SELECT
        repo_id,
        object_id,
        valid_from,
        has_blob,
        ROW_NUMBER() OVER (
            PARTITION BY repo_id, object_id
            ORDER BY valid_from DESC, has_blob DESC
        ) AS rn
    FROM blobs
),
latest_blob_version AS (
    SELECT
        repo_id,
        object_id,
        has_blob
    FROM versioned_blobs
    WHERE rn = 1
)
SELECT
    repo_id,
    object_id
FROM latest_blob_version
WHERE has_blob = 1;

-- view: repository_filesystem_available_files
CREATE VIEW repository_filesystem_available_files AS
SELECT
    path,
    object_id,
    repo_id
FROM latest_filesystem_files
    INNER JOIN latest_available_blobs USING (object_id);