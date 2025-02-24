![img.png](docs/res/logo.png)

[![CI](https://github.com/ReSqAr/amber/actions/workflows/ci.yaml/badge.svg)](https://github.com/ReSqAr/amber/actions)
[![License](https://img.shields.io/github/license/ReSqAr/amber.svg)](LICENSE)
[![codecov](https://codecov.io/gh/ReSqAr/amber/branch/main/graph/badge.svg)](https://codecov.io/gh/ReSqAr/amber)

# Amber

## Summary

Amber is a streamlined tool for managing large file collections by tracking content-addressable blobs. It works like a "Git for blobs"—similar in spirit to Git Annex but without branching—by preserving files in their original form. Instead of versioning changes, Amber treats every file as an immutable blob (think of how you rarely edit photos) and leverages hard links to deduplicate them. File tracking information is stored in a SQLite database, while file transfers between locations are handled via rclone. Built in Rust for speed and robustness, Amber provides a clear and efficient way to manage vast amounts of data.

## Overview

- **Immutable Blobs:** Files are stored as unchangeable blobs—once added, they aren’t edited (much like photos, which remain in their original state). This read-only approach, combined with hard links, ensures efficient deduplication.
- **Massive Collections:** Designed for large-scale use (imagine managing 150,000 photos), Amber provides great visibility into your data so nothing scrolls past unnoticed.
- **Accurate Tracking:** All file metadata and tracking details are maintained in a lightweight SQLite database.
- **Effortless Mobility:** Utilize rclone to move files between storage locations seamlessly.
- **Fast & Robust:** Developed in Rust, Amber delivers high performance and reliability.
- **Material Preservation:** Amber preserves your files exactly as they are.

## Quick Start

Initialize a new repository and perform basic operations:

```bash
amber init my_first_repo
amber add
amber status
```

Add remote storage locations using either SSH or rclone:

```bash
amber remote add my_first_ssh_remote ssh holden@tycho.belt/home/holden/rocinante
amber remote add my_first_rclone_remote rclone <rclone-target>:/amber
```
