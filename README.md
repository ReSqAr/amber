![img.png](docs/res/logo.png)

# Amber

----

[![CI](https://github.com/ReSqAr/amber/actions/workflows/ci.yaml/badge.svg)](https://github.com/ReSqAr/amber/actions)
[![License](https://img.shields.io/github/license/ReSqAr/amber.svg)](LICENSE)
[![codecov](https://codecov.io/gh/ReSqAr/amber/branch/main/graph/badge.svg)](https://codecov.io/gh/ReSqAr/amber)
[![GitHub issues](https://img.shields.io/github/issues/ReSqAr/amber.svg)](https://github.com/ReSqAr/amber/issues)
[![GitHub release](https://img.shields.io/github/release/ReSqAr/amber.svg)](https://github.com/ReSqAr/amber/releases)


<!-- TOC -->
* [Amber](#amber)
  * [Pitch](#pitch)
  * [Overview](#overview)
  * [Is amber for you?](#is-amber-for-you)
* [Quick Start](#quick-start)
  * [External Hard Disk](#external-hard-disk)
  * [SSH](#ssh)
  * [rclone Supported Storage: S3/Backblaze/...](#rclone-supported-storage-s3backblaze)
  * [Syncing Metadata](#syncing-metadata)
  * [Updating gRPC bindings](#updating-grpc-bindings)
  * [I don't have any data but still want to play around with amber](#i-dont-have-any-data-but-still-want-to-play-around-with-amber)
<!-- TOC -->

## Pitch

Amber is a streamlined tool for managing large file collections by tracking content-addressable blobs.
It works like a "Git for blobs" - similar in spirit to Git Annex but without branching - by preserving files in their original form.
Instead of versioning changes, amber treats every file as an immutable blob (think of how you rarely edit photos) and leverages hard links to deduplicate them.
File tracking information is stored in a database, while file transfers between locations are handled via rclone.
Built in Rust for speed and robustness, amber provides a clear and efficient way to manage vast amounts of data.

## Overview

- **Immutable Blobs:** Files are stored as unchangeable blobs - once added, they aren’t edited (much like photos, which remain in their original state). This read-only approach, combined with CoW (or hard links), ensures efficient tracking.
- **Massive Collections:** Designed for large-scale use (imagine managing 300k photos), amber provides great visibility into your data so nothing scrolls past unnoticed.
- **Accurate Tracking:** All file metadata and tracking details are maintained in a lightweight database.
- **Effortless Mobility:** Utilize rclone to move files between storage locations seamlessly.
- **Fast & Robust:** Developed in Rust, amber delivers high performance and reliability.
- **Material Preservation:** amber preserves your files exactly as they are.

## Is amber for you?

- Your documents look like `v5.final-3` and you don't expect the file to ever change again → _amber can lock down your files and ensure file integrity_
- You have 200k photos, some duplicated and you want to move them to a hard disk/B2/... while keeping track of what's already there and what isn't → _amber can track your files and copy them_
- You have a TODO folder which you constantly edit → amber is not for you - _amber assumes your blobs are immutable_
- You collaborate with others → _amber might be for you - conflict resolution is simply 'most recently added file wins'_

Requirements are:
- your filesystem supports hard links or ref links
- you have `rclone` installed
- you understand that amber sets your files read-only to protect the files' integrity. Do not interfere

# Quick Start

To initialize a new repository and add all files in the current folder,
run this command:

```bash
amber init tycho
amber status
amber add
```
![get_started.gif](docs/res/get_started.gif)


## External Hard Disk

When moving files from/to an external hard drive,
use these commands to set up the external drive:
```bash
amber init drive
```

Then in your primary amber repository run these commands:
```
amber remote add external local /path/to/external/drive
amber push external
```
![external_drive.gif](docs/res/external_drive.gif)

You'll find that after you run the push command,
the external disk doesn't show any files.
In this case just run `amber sync` to materialise all available files.

## SSH

When moving files via an SSH connection,
use these commands on the remote host to set up the repository:
```bash
amber init remote
```

Then in your primary amber repository run these commands:
```
amber remote add medina ssh holden@tycho.com:22/home/holden 
amber push medina
```

You'll find that after you run the push command,
the SSH remote doesn't show any files in the directory.
In this case just run `amber sync` to materialise all available files.

**Note:**

- SSH key auth is supported - amber leverages the ssh agent for key discovery.
So make sure that you `ssh-add` your keys before running amber
- you can explicitly specify a password for the connection using this syntax:
`holden:hunter2@tycho.com:22/` - where `hunter2` is the example password.
Password auth is not recommended though.


## rclone Supported Storage: S3/Backblaze/...

amber can also move files to any service supported by rclone.
The tracking information gets synced into an `.amb` folder on the rclone remote.

You'll first need to set up your remote in rclone via `rclone config`.
Assume the target is called `b2-ganymede` in `rclone`, then run this:

```
amber remote add ganymede rclone b2-ganymede 
amber push ganymede
```

**Note:**

- The connection is only locally set up. Other repositories do not automatically see this repository.

## Syncing Metadata

These commands sync metadata:
```bash
amber sync <connection name>
amber push <connection name>
amber pull <connection name>
```

Metadata here mainly means the availability of blobs in the repositories and the desired file structure.

## Updating gRPC bindings

If `proto/grpc.proto` changes, regenerate the committed gRPC stubs with the helper script:

```bash
scripts/update-grpc.sh
```

This uses `tonic-build` under the hood and writes the generated files into `src/grpc/generated/`.

## I don't have any data but still want to play around with amber

You are in luck.
`examples/generate_repository.rs` is the tool you are looking for.
Have a look at `--help` but the gist is:
it creates a deterministic repository of configurable size
with a configurable distribution of file sizes.

```
alias genrepo='cargo run --color=always --package amber --example generate_repository --profile dev --manifest-path /path/to/Cargo.toml --'
genrepo --total 1GB .
```
