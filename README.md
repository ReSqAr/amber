# PROJECT MANAGEMENT
- pull/push
  - BlobRepository: rclone
- multi sync & push
- fsck
- edit & deletion
- amazing user feedback
  - TODO
    - adding: terminal audit logs + file audit logs
  - good info/debug levels
  - dedicated system for user messages
    - mirrored into file? session.<>.txt
    - nice to look at: colour = log level
    - proper progress tracking as in rclone transfers
    - map out which messages are produced per command and their level
- make all files read-only <- disable via --immutable
- amber add breaks if blob exists but is not available locally [checker] 
- cleanup table transfers in the DB
- repo name table like blob/file + sync + set at init
- blob path: two level blob
- ssh which honours ssh config + key auth
- add documentation folder with knowledge base articles on:
  - large scale testing
  - ssh testing

- features:
- `--immutable` - no del to hard link multiple copies & not read only
- `--statistics` - print stats

# Pull & Push

transfer id = 32bit random uint

BlobSender:
- prepare_transfer(transfer_id, stream of (blob_id,path))
  - hard links blob_id to staging/<transfer_id>/path if exists

BlobReceiver:
- create_transfer_request(transfer_id)
    - creates <repo>/staging/tid_<transfer_id>/
    - creates temp table with: (blob_id,path)
    - streams that table
- finalise_transfer(transfer_id):
  - stream from temp table
  - stream like adder - blob_adder:
    - in parallel: hardlink files to blob
      - recompute hash -> fail if files do not match advertised hash (sender corruption)
    - add to blobs table

Connection:
- transfer(transfer_id, source repo, target repo, stream of (blob_id,path)) -> Transfer

Transfer:
- Transfer::new:(transfer_id, sender repo, receiver repo) figures out the direction of transfer: up or down
- Transfer::execute
  - calls: receiver::create_transfer_request:
    - stream of (blob_id,path) to: 
      - sender::prepare_transfer
      - locally: <repo>/staging/rclone_<transfer_id>.<conf|txt>/
  - execute rclone using rclone files
  - receiver::finalise_transfer




# TODO
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


# COMMANDS
- add: adds all [Local + Metadata + Adder]
- status [Local + Metadata]
- missing [Metadata]

- sync [Syncer]
- pull [Local, BlobSender]
- push [Local, BlobReceiver]

- remote add [Local]
- fsck  [Local + Metadata]


# TRAITS
- Metadata: repo id
- Local: has path, remotes, look up of effective files(?) / blobs
- Adder: add files + add blobs
- Syncer: sync DB files + sync DB blobs + sync DB repositories
- Reconciler: bundles: DB target_filesystem_state + checkout new blobs files [overwrites existing and creates new]
- BlobReceiver: Reconciler [inlined] + receive (open and close transaction) [uses: DB missing_blobs]
- BlobSender: prep + send

# TYPES
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

# SYNC PATTERNS: PUSH: LOCAL -> REMOTE

-----

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



-----

# ADD / STATUS

Filesystem state - business logic:
- input:
  - DB (one can lookup current repo_id via current_repository table)
  - path
- output:
  - stream of enum:
    - Ok: path, blob_id
    - Dirty: path, Option(blob_id)
    - New: path
    - Deleted: path, blob_id

1. Have table local with (virtual_filesystem): (use sqlite integer dttm representation for truncation; all are optional)
   - path (UNIQUE)
   - file last seen id 
   - file last seen
   - file last modified
   - file size
   - local_has_blob
   - blob id
   - blob size
   - last blob check
   - last blob check result is ok: TRUE, FALSE (= three cases: same/different file & hardlinked to some blob)
   - state:
     - new         = blob id = null
     - dirty       = last blob check result is ok = FALSE AND file last modified < last blob check
     - ok          = blob id = null OR (last blob check result is ok = TRUE AND file last modified < last blob check)
     - needs_check = everything else
2. Pipe target filesystem to update desired blob id & blob file size (SQL UPDATE)
   1. zero blob * if not blob has been deleted
   2. have:
      1. latest_filesystem_files view with columns: path & blob_id (this table doesn't take into account what is locally available) - this is the desired state 
      2. latest_available_blobs view with columns: repo_id & blob_id - filter on current repo_id to get locally available blobs
3. Pipe current filesystem using walkers to batch update (SLQ INSERT INTO) last seen & last modified ts & file size & last seen id
   1. Update state via ON CONFLICT handling (path); use coalesce to update only things that have changed
   2. Use RETURNING to return path & state and everything else
4. Handle state:
   1. new -> emit
   2. dirty -> emit
   3. ok -> emit
   4. needs_check -> handle internally:
      1. check file: check if inode of blob id is the same as the inode of the file
5. clean up current_fs: delete all rows: last seen in walk != current walk AND blob id != null
6. detect deleted files which should have been there: last seen in walk != current walk AND local_has_blob
   1. emit delete

DB provides:
- refresh_virtual_filesystem():
  - does step 2 to update the virtual_filesystem table: SQL UPDATE statement using views
  - no input/no output
- add_virtual_filesystem_observations():
  - does step 3
  - input stream - enum type with two cases:
    - FileSeen: path, last seen id, last seen (datetime), last modified, size
    - BlobCheck: path, last blob check (datetime), check result is ok (boolean)
  - output stream: just * one the row; convert state into an enum
  - simple version: adds this row by row; uses
    - INSERT INTO ... ON CONFLICT .. RETURNING
    - CASE statement to compute the state
    - just one INSERT INTO - do not distinguish between FileSeen and BlobCheck; use coalesce to merge existing and new data

Helper function 'Walker' - walks a directory and observes files:
- input: root, maybe some config, .gitignore like, not as file atm but as function parameter
- output: async tokio stream of: path (relative to root), size, last modified

Helper function checker:
- input:
  - needs_check stream
  - DB.add_virtual_filesystem_observations::input stream
- checks the state of a file
- check if the inode of blob id is the same as the inode of the file (ie: if they are hardlinked)
  - if blob and path are hardlinked: return check result is ok = true
  - else return: check result is ok = false

Main logic:
- use DB.refresh_virtual_filesystem()
- piping:
  - walker() -[transform]-> DB.add_virtual_filesystem_observations::input                                                transformer = walker_transformer_handle
  - DB.add_virtual_filesystem_observations::output -[filter: state = new/dirty/ok] -> output stream                      transformer = splitter_handle
  - DB.add_virtual_filesystem_observations::output -[filter: state = needs_check] -> needs_check stream                  transformer = splitter_handle
  - needs_check stream -[checker]-> DB.add_virtual_filesystem_observations::input                                        transformer = checker_handle

