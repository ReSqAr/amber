![img.png](docs/res/logo.png)

# Amber

## 1. Summary

Amber is a streamlined tool for managing large file collections by tracking content-addressable blobs. It works like a "Git for blobs"—similar in spirit to Git Annex but without branching—by preserving files in their original form. Instead of versioning changes, Amber treats every file as an immutable blob (think of how you rarely edit photos) and leverages hard links to deduplicate them. File tracking information is stored in a SQLite database, while file transfers between locations are handled via rclone. Built in Rust for speed and robustness, Amber provides a clear and efficient way to manage vast amounts of data.

## 2. Mental Model

- **Immutable Blobs:** Everything is stored as a blob; files are made read-only. This ensures that the original content is preserved, similar to how photos remain unchanged after being taken.
- **Massive Collections:** When dealing with tens or hundreds of thousands of files, Amber offers excellent visibility into your data, ensuring nothing scrolls past unnoticed.
- **SQLite Tracking:** All metadata and tracking information is stored in a lightweight yet powerful SQLite database.
- **Rclone for Mobility:** File transfers to remote locations are performed using rclone, enabling seamless movement across different storage backends.
- **Hard Links & Deduplication:** Hard links are used extensively to manage files without data duplication. Deduplication is achieved by recognizing blobs with identical content.
- **Rust-Powered:** Written in Rust, Amber is designed to be fast and robust, providing a solid foundation for preserving your files exactly as they are.

## 3. Quick Summary (Elevator Pitch)

- **Everything is a Blob:** Files are immutable—no editing, just preservation (like photos, which are typically read-only). Hard links ensure efficient deduplication.
- **Manage Massive Collections:** Designed to handle vast numbers (e.g., 150,000 photos) with great visibility so that nothing is missed.
- **Accurate Tracking:** Uses SQLite to keep track of all file metadata.
- **Effortless Mobility:** Relocate your files easily using rclone.
- **Fast & Robust:** Built in Rust for high performance and reliability.
- **Amber = Material Preservation:** It preserves files exactly as they are—just the material, unaltered.

## 4. Example Commands

### Initializing a Repository and Basic Operations

```bash
amber init my_first_repo
amber add
amber status
```

### Adding Remotes

```bash
amber remote add my_first_ssh_remote ssh holden@tycho.belt/home/holden/rocinante
amber remote add my_first_rclone_remote rclone <rclone-target>:/amber
```

These commands set up your repository, add files, and check status, as well as configure remote storage locations using both rclone and SSH.
