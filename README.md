TODO:
- refactoring
  - trait: see below
  - Configuration
  - find .inv folder also in levels above the current one
- enhance:
  - add should add all files
- tables:
  - connections
  - export has different structure - so how to capture in blobs table
- consistent naming


COMMANDS:
- add: adds all [Local + Metadata + Adder]
- status [Local + Metadata]
- missing [Metadata]

- sync [Syncer]
- pull [Local, BlobSender]
- push [Local, BlobReceiver]

- remote add [Local]
- fsck  [Local + Metadata]

TRAINTS:
- Metadata: repo id + look up of effective files(?) / blobs
- Local: has path, remotes
- Adder: add files + add blobs
- Syncer: sync DB files + sync DB blobs + sync DB repositories
- Reconciler: bundles: DB desired_filesystem_state + checkout new blobs files [overwrites existing and creates new]
- BlobReceiver: Reconciler [inlined] + receive (open and close transaction) [uses: DB missing_blobs]
- BlobSender: prep + send

TYPES
- LocalRepository: *
- SSHRepository: Metadata + Syncer + BlobReceiver + BlobSender 
- BlobRepository: Metadata + BlobReceiver + BlobSender
- InaccessibleRepository: Metadata


via Connection which can:
- directly <-> LocalRepository
- open grpc via ssh <-> SSHRepository
- rclone <-> BlobRepository

connections: returns enum of Repository + trait based dispatch

---

sync patterns: push: local -> remote

if remote = LocalRepository or SSHRepository:
1. local: create staging folder 
2. remote: create staging folder
3. remote: return stream of missing blobs - format is: <blob_id> -> .inv/staging/<transfer-id>/<blob_id>  
   1. local: hardlink files in staging according to stream (just the list as received)
4. local: run rclone to copy local staging -> remote staging (just the list as received)
5. remote: integrate blobs into blob structure + DB using [temp table in <staging>/<transfer-id>.sqlite: blobs & location]
6. remote: reconcile


if remote = BlobRepository:
1. local: create staging folder 
2. 'remote': create staging folder locally
3. 'remote': return stream of missing blobs - format is: blob_id -> <first relative path>
   1. local: hardlink files in staging according to stream (just the list as received)
4. local: run rclone to copy local staging -> remote staging
5. 'remote': update DB using [temp table in <staging>/<transfer-id>.sqlite: blobs & location]


-----

sync patterns: pull: remote -> local

if remote = LocalRepository or SSHRepository:
1. local: create staging folder
2. remote: create staging folder
3. local: send stream of missing blobs - format is: <blob_id>
   1. remote: hardlink files in staging according to stream
   2. remote: record <blob_id> -> .inv/staging/<transfer-id>/<blob_id>
4. local: run rclone to copy remote staging -> local staging (just the list as received)
5. local: integrate blobs into blob structure + DB [temp table in <staging>/<transfer-id>.sqlite: blobs & location]
6. local: reconcile


if remote = BlobRepository:
1. local: create staging folder
2. 'remote': create staging folder locally 
3. 'local': send stream of missing blobs - format is: blob_id -> <first relative path>
   1. remote: record <blob_id> -> $filename
4. local: run rclone to copy remote staging -> local staging (just the list as received)
5. local: integrate blobs into blob structure + DB [temp table in <staging>/<transfer-id>.sqlite: blobs & location]
6. local: reconcile


---
rclone:
- specify files via: --files-from=<staging>/<transfer-id>.rclone
- make sure that it doesn't loop up if the target file exists - just copy it

---

push and pull are not symmetric - the remote does choose the blob filenames in both cases 