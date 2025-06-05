# RedBase Usage Guide

This document provides detailed instructions on how to use RedBase, a lightweight HBase-like database implemented in Rust.

## Table of Contents

- [Getting Started](#getting-started)
- [Basic Operations](#basic-operations)
  - [Creating Tables and Column Families](#creating-tables-and-column-families)
  - [Writing Data](#writing-data)
  - [Reading Data](#reading-data)
  - [Deleting Data](#deleting-data)
- [Advanced Features](#advanced-features)
  - [Multi-Version Concurrency Control](#multi-version-concurrency-control)
  - [Tombstones and TTL](#tombstones-and-ttl)
  - [Scanning](#scanning)
  - [Compaction](#compaction)
  - [Filtering](#filtering)
  - [Aggregation](#aggregation)
- [Performance Considerations](#performance-considerations)
- [Error Handling](#error-handling)
- [Examples](#examples)

## Getting Started

### Installation

Add RedBase to your Cargo.toml:

```toml
[dependencies]
RedBase = { git = "https://github.com/yourusername/RedBase.git" }
```

### Basic Setup

```rust
use std::path::Path;
use RedBase::api::Table;

fn main() -> std::io::Result<()> {
    // Open a table (creates the directory if it doesn't exist)
    let mut table = Table::open("./data/my_table")?;
    
    // Create a column family
    if table.cf("default").is_none() {
        table.create_cf("default")?;
    }
    
    // Get a reference to the column family
    let cf = table.cf("default").unwrap();
    
    // Now you can use the column family for operations
    
    Ok(())
}
```

## Basic Operations

### Creating Tables and Column Families

Tables in RedBase are directories on disk, and column families are subdirectories within a table directory.

```rust
// Open a table (creates the directory if it doesn't exist)
let mut table = Table::open("./data/my_table")?;

// Create a column family
table.create_cf("users")?;
table.create_cf("posts")?;

// Get a reference to a column family
let users_cf = table.cf("users").unwrap();
let posts_cf = table.cf("posts").unwrap();
```

### Writing Data

Data in RedBase is organized by row key, column name, and timestamp. Each write operation automatically assigns a timestamp based on the current time.

```rust
// Write data to a column family
users_cf.put(b"user1".to_vec(), b"name".to_vec(), b"John Doe".to_vec())?;
users_cf.put(b"user1".to_vec(), b"email".to_vec(), b"john@example.com".to_vec())?;
users_cf.put(b"user1".to_vec(), b"age".to_vec(), b"30".to_vec())?;

// Writing to the same row and column creates a new version
users_cf.put(b"user1".to_vec(), b"name".to_vec(), b"John Smith".to_vec())?;
```

### Reading Data

RedBase provides several ways to read data:

```rust
// Get the latest value for a specific row and column
let name = users_cf.get(b"user1", b"name")?;
if let Some(value) = name {
    println!("Name: {}", String::from_utf8_lossy(&value));
}

// Get multiple versions of a value
let name_versions = users_cf.get_versions(b"user1", b"name", 10)?;
for (timestamp, value) in name_versions {
    println!("At {}: {}", timestamp, String::from_utf8_lossy(&value));
}
```

### Deleting Data

Deleting data in RedBase creates a tombstone marker:

```rust
// Delete a value (creates a tombstone with no TTL)
users_cf.delete(b"user1".to_vec(), b"age".to_vec())?;

// Delete with TTL (tombstone expires after 1 hour)
users_cf.delete_with_ttl(b"user1".to_vec(), b"email".to_vec(), Some(3600 * 1000))?;
```

## Advanced Features

### Multi-Version Concurrency Control

RedBase implements MVCC (Multi-Version Concurrency Control) which allows it to maintain multiple versions of data. Each write operation creates a new version with a timestamp, and read operations can retrieve specific versions.

```rust
// Write multiple versions
cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec())?;
cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value2".to_vec())?;
cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value3".to_vec())?;

// Get all versions (up to 10)
let versions = cf.get_versions(b"row1", b"col1", 10)?;
for (ts, value) in versions {
    println!("Timestamp: {}, Value: {}", ts, String::from_utf8_lossy(&value));
}

// The latest version is returned by default
let latest = cf.get(b"row1", b"col1")?;
println!("Latest value: {}", String::from_utf8_lossy(&latest.unwrap()));
```

### Tombstones and TTL

When you delete data in RedBase, it creates a tombstone marker rather than immediately removing the data. Tombstones can have an optional Time-To-Live (TTL) after which they are eligible for removal during compaction.

```rust
// Delete with no TTL (tombstone never expires automatically)
cf.delete(b"row1".to_vec(), b"col1".to_vec())?;

// Delete with a TTL of 1 hour (3,600,000 milliseconds)
cf.delete_with_ttl(b"row1".to_vec(), b"col2".to_vec(), Some(3600 * 1000))?;

// After the TTL expires, the tombstone can be removed during compaction
// But until then, it will hide any older versions of the data
```

### Scanning

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

### Compaction

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

// Custom compaction options
let options = CompactionOptions {
    compaction_type: CompactionType::Major,
    max_versions: Some(3),
    max_age_ms: Some(24 * 3600 * 1000), // 1 day
    cleanup_tombstones: true,
};
cf.compact_with_options(options)?;
```

### Filtering

RedBase supports filtering data based on various predicates:

```rust
use RedBase::filter::{Filter, FilterSet};

// Simple filter: get a value that equals a specific value
let filter = Filter::Equal(b"John Doe".to_vec());
let result = cf.get_with_filter(b"user1", b"name", &filter)?;

// Contains filter: get a value that contains a substring
let filter = Filter::Contains(b"Smith".to_vec());
let result = cf.get_with_filter(b"user2", b"name", &filter)?;

// Create a filter set for scanning
let mut filter_set = FilterSet::new();
filter_set.add_column_filter(
    b"age".to_vec(),
    Filter::GreaterThan(b"25".to_vec())
);

// Scan with filter
let scan_result = cf.scan_with_filter(b"user1", b"user3", &filter_set)?;
for (row, columns) in scan_result {
    println!("Row: {}", String::from_utf8_lossy(&row));
    for (col, versions) in columns {
        for (ts, value) in versions {
            println!("  {} -> {} -> {}", 
                String::from_utf8_lossy(&col),
                ts,
                String::from_utf8_lossy(&value)
            );
        }
    }
}
```

Available filter types:
- `Equal`: Exact match
- `NotEqual`: Not an exact match
- `GreaterThan`: Greater than comparison
- `GreaterThanOrEqual`: Greater than or equal comparison
- `LessThan`: Less than comparison
- `LessThanOrEqual`: Less than or equal comparison
- `Contains`: Contains a substring
- `StartsWith`: Starts with a prefix
- `EndsWith`: Ends with a suffix
- `And`: Logical AND of multiple filters
- `Or`: Logical OR of multiple filters
- `Not`: Logical NOT of a filter

### Aggregation

RedBase supports aggregation operations on data:

```rust
use RedBase::aggregation::{AggregationType, AggregationSet};

// Create an aggregation set
let mut agg_set = AggregationSet::new();
agg_set.add_aggregation(b"value1".to_vec(), AggregationType::Count);
agg_set.add_aggregation(b"value2".to_vec(), AggregationType::Sum);
agg_set.add_aggregation(b"value3".to_vec(), AggregationType::Average);
agg_set.add_aggregation(b"value4".to_vec(), AggregationType::Min);
agg_set.add_aggregation(b"value5".to_vec(), AggregationType::Max);

// Perform aggregations
let agg_result = cf.aggregate(b"stats", None, &agg_set)?;
for (col, result) in agg_result {
    println!("  {} -> {}", String::from_utf8_lossy(&col), result.to_string());
}

// Combined filtering and aggregation
let mut filter_set = FilterSet::new();
filter_set.add_column_filter(
    b"cpu".to_vec(),
    Filter::GreaterThan(b"20".to_vec())
);

let mut agg_set = AggregationSet::new();
agg_set.add_aggregation(b"cpu".to_vec(), AggregationType::Average);

let agg_result = cf.aggregate(b"metrics", Some(&filter_set), &agg_set)?;
```

Available aggregation types:
- `Count`: Count the number of values
- `Sum`: Sum the values (must be numeric)
- `Average`: Calculate the average of the values (must be numeric)
- `Min`: Find the minimum value
- `Max`: Find the maximum value

## Performance Considerations

### Memory Management

RedBase uses a MemStore for in-memory storage before flushing to disk. By default, the MemStore is flushed to disk when it reaches 10,000 entries. You can manually flush the MemStore:

```rust
// Flush the MemStore to disk
cf.flush()?;
```

### Compaction

Regular compaction is important for maintaining good read performance. RedBase runs a background compaction thread every 60 seconds, but you can also trigger compaction manually:

```rust
// Run compaction
cf.compact()?;
```

## Error Handling

Most operations in RedBase return a `Result<T, std::io::Error>`, which allows you to handle errors using Rust's standard error handling mechanisms:

```rust
match cf.get(b"row1", b"col1") {
    Ok(Some(value)) => println!("Value: {}", String::from_utf8_lossy(&value)),
    Ok(None) => println!("Value not found"),
    Err(e) => eprintln!("Error: {}", e),
}
```

## Examples

### User Profile Management

```rust
// Create a users column family
let mut table = Table::open("./data/my_app")?;
if table.cf("users").is_none() {
    table.create_cf("users")?;
}
let users_cf = table.cf("users").unwrap();

// Create a user profile
let user_id = b"user123".to_vec();
users_cf.put(user_id.clone(), b"name".to_vec(), b"John Doe".to_vec())?;
users_cf.put(user_id.clone(), b"email".to_vec(), b"john@example.com".to_vec())?;
users_cf.put(user_id.clone(), b"age".to_vec(), b"30".to_vec())?;

// Update a user profile
users_cf.put(user_id.clone(), b"name".to_vec(), b"John Smith".to_vec())?;

// Get user profile
let name = users_cf.get(&user_id, b"name")?;
let email = users_cf.get(&user_id, b"email")?;
let age = users_cf.get(&user_id, b"age")?;

println!("User: {}", String::from_utf8_lossy(&name.unwrap()));
println!("Email: {}", String::from_utf8_lossy(&email.unwrap()));
println!("Age: {}", String::from_utf8_lossy(&age.unwrap()));

// Get profile history
let name_history = users_cf.get_versions(&user_id, b"name", 10)?;
println!("Name history:");
for (ts, name) in name_history {
    println!("  {} -> {}", ts, String::from_utf8_lossy(&name));
}
```

### Time Series Data

```rust
// Create a metrics column family
let mut table = Table::open("./data/metrics")?;
if table.cf("cpu").is_none() {
    table.create_cf("cpu")?;
}
let cpu_cf = table.cf("cpu").unwrap();

// Record CPU metrics for different servers
for i in 1..=5 {
    let server_id = format!("server{}", i);
    cpu_cf.put(server_id.into_bytes(), b"usage".to_vec(), format!("{}", i * 10).into_bytes())?;
}

// Query with filtering
let mut filter_set = FilterSet::new();
filter_set.add_column_filter(
    b"usage".to_vec(),
    Filter::GreaterThan(b"20".to_vec())
);

// Scan with filter
let high_cpu_servers = cpu_cf.scan_with_filter(b"server1", b"server5", &filter_set)?;
println!("Servers with high CPU usage:");
for (server, columns) in high_cpu_servers {
    println!("Server: {}", String::from_utf8_lossy(&server));
    for (col, versions) in columns {
        for (_, value) in versions {
            println!("  {} -> {}", 
                String::from_utf8_lossy(&col),
                String::from_utf8_lossy(&value)
            );
        }
    }
}

// Aggregate CPU usage
let mut agg_set = AggregationSet::new();
agg_set.add_aggregation(b"usage".to_vec(), AggregationType::Average);

let avg_cpu = cpu_cf.aggregate_range(b"server1", b"server5", None, &agg_set)?;
println!("Average CPU usage across all servers:");
for (_, columns) in avg_cpu {
    for (col, result) in columns {
        println!("  {} -> {}", String::from_utf8_lossy(&col), result.to_string());
    }
}
```