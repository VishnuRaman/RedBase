# RedBase Basic Usage Guide

This document provides a basic guide to using RedBase, a lightweight HBase-like database implemented in Rust.

## Table of Contents

- [Creating Tables and Column Families](#creating-tables-and-column-families)
- [Writing Data](#writing-data)
- [Reading Data](#reading-data)
- [Deleting Data](#deleting-data)
- [Scanning Data](#scanning-data)
- [Flushing and Compaction](#flushing-and-compaction)

## Creating Tables and Column Families

Tables in RedBase are directories on disk, and column families are subdirectories within a table directory.

```rust
use std::path::Path;
use RedBase::api::Table;

// Open a table (creates the directory if it doesn't exist)
let mut table = Table::open("./data/my_table")?;

// Create a column family
if table.cf("default").is_none() {
    table.create_cf("default")?;
}

// Get a reference to the column family
let cf = table.cf("default").unwrap();
```

You can create multiple column families in a table:

```rust
table.create_cf("users")?;
table.create_cf("posts")?;

let users_cf = table.cf("users").unwrap();
let posts_cf = table.cf("posts").unwrap();
```

## Writing Data

Data in RedBase is organized by row key, column name, and timestamp. Each write operation automatically assigns a timestamp based on the current time.

```rust
// Write data to a column family
cf.put(b"user1".to_vec(), b"name".to_vec(), b"John Doe".to_vec())?;
cf.put(b"user1".to_vec(), b"email".to_vec(), b"john@example.com".to_vec())?;
cf.put(b"user1".to_vec(), b"age".to_vec(), b"30".to_vec())?;
```

Writing to the same row and column creates a new version:

```rust
// This creates a new version of the name column
cf.put(b"user1".to_vec(), b"name".to_vec(), b"John Smith".to_vec())?;
```

## Reading Data

RedBase provides several ways to read data:

### Get the Latest Value

```rust
// Get the latest value for a specific row and column
let name = cf.get(b"user1", b"name")?;
if let Some(value) = name {
    println!("Name: {}", String::from_utf8_lossy(&value));
}
```

### Get Multiple Versions

```rust
// Get multiple versions of a value (up to 10 versions)
let name_versions = cf.get_versions(b"user1", b"name", 10)?;
for (timestamp, value) in name_versions {
    println!("At {}: {}", timestamp, String::from_utf8_lossy(&value));
}
```

## Deleting Data

Deleting data in RedBase creates a tombstone marker:

```rust
// Delete a value (creates a tombstone with no TTL)
cf.delete(b"user1".to_vec(), b"age".to_vec())?;
```

You can also delete with a Time-To-Live (TTL):

```rust
// Delete with TTL (tombstone expires after 1 hour)
cf.delete_with_ttl(b"user1".to_vec(), b"email".to_vec(), Some(3600 * 1000))?;
```

After the TTL expires, the tombstone can be removed during compaction. Until then, it will hide any older versions of the data.

## Scanning Data

RedBase allows you to scan all columns for a specific row:

```rust
// Scan all columns for a row, getting up to 10 versions per column
let row_data = cf.scan_row_versions(b"user1", 10)?;
for (column, versions) in row_data {
    println!("Column: {}", String::from_utf8_lossy(&column));
    for (ts, value) in versions {
        println!("  {} -> {}", ts, String::from_utf8_lossy(&value));
    }
}
```

## Flushing and Compaction

RedBase uses a MemStore for in-memory storage before flushing to disk. By default, the MemStore is flushed to disk when it reaches 10,000 entries. You can manually flush the MemStore:

```rust
// Flush the MemStore to disk
cf.flush()?;
```

Compaction is the process of merging multiple SSTables and optionally removing old versions or expired tombstones. RedBase supports several compaction strategies:

```rust
// Default compaction (minor compaction)
cf.compact()?;

// Major compaction (merges all SSTables)
cf.major_compact()?;

// Compaction with version limits (keep only the 2 newest versions)
cf.compact_with_max_versions(2)?;

// Compaction with age limits (keep only data from the last hour)
cf.compact_with_max_age(3600 * 1000)?;
```

You can also use custom compaction options:

```rust
use RedBase::api::{CompactionOptions, CompactionType};

// Custom compaction options
let options = CompactionOptions {
    compaction_type: CompactionType::Major,
    max_versions: Some(3),
    max_age_ms: Some(24 * 3600 * 1000), // 1 day
    cleanup_tombstones: true,
};
cf.compact_with_options(options)?;
```

RedBase runs a background compaction thread every 60 seconds, but you can also trigger compaction manually as shown above.