![img.png](docs/res/logo.png)

# Amber

## Summary
   Amber is a streamlined tool for managing large file collections by tracking content-addressable blobs. It is designed as a "Git for blobs" and operates similarly to Git Annex but without the overhead of branching. Amber leverages a SQLite database to track file metadata and employs hard links to efficiently manage file storage, ensuring deduplication based on blob content. Its simplicity and robust design make it ideal for environments where linear history and minimal complexity are preferred.

## Mental Model
   
Amber operates on a straightforward yet powerful principle:

- **File Tracking in SQLite**: File metadata and associations are stored in a SQLite database, allowing for quick and efficient queries.
- **Content Copying via rclone**: Files are transferred between local and remote storage using rclone, ensuring broad compatibility and high performance.
- **Hard Link Management**: By utilizing hard links, Amber avoids data duplication and maintains a consistent state across storage locations.
- **No Branching**: The tool intentionally avoids branching, simplifying the model to a linear workflow.
- **Deduplication**: Deduplication is performed per 'blob content'—files are uniquely identified by their content hash, allowing Amber to efficiently manage storage by reusing identical blobs wherever they occur.

### Example Commands
   Below are sample commands for adding remote storage locations:

```bash
amber init my_first_repo
amber add
amber status
```




