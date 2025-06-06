# RedBase: An HBase-like Database in Rust

RedBase is a lightweight implementation of an HBase-like distributed database system written in Rust. It provides a simplified but functional subset of Apache HBase's features, focusing on core functionality while maintaining a clean, memory-safe implementation.

## Overview

RedBase implements a columnar storage system with:
- Tables and Column Families
- Multi-Version Concurrency Control (MVCC)
- Tombstone markers for deleted data with TTL
- Background compaction with various strategies
- Version filtering and cleanup

## Architecture

RedBase follows a similar architecture to HBase with some simplifications:

- **MemStore**: In-memory storage with Write-Ahead Log (WAL) for durability
- **SSTable**: Immutable on-disk storage format for persisted data
- **Column Family**: Logical grouping of columns with versioning support
- **Table**: Container for multiple Column Families

## Features Consistent with HBase

RedBase implements the following features that are consistent with Apache HBase:

1. **Data Model**
   - Tables containing Column Families
   - Row-oriented storage with column qualifiers
   - Timestamps for versioning
   - Binary storage (keys and values are byte arrays)

2. **MVCC (Multi-Version Concurrency Control)**
   - Support for multiple versions of data
   - Timestamp-based versioning
   - Version retrieval with configurable limits

3. **Storage Architecture**
   - MemStore for in-memory storage
   - Write-Ahead Log (WAL) for durability
   - SSTables for on-disk storage
   - Automatic flushing of MemStore to disk

4. **Compaction**
   - Minor compaction (subset of SSTables)
   - Major compaction (all SSTables)
   - Version-based cleanup during compaction
   - Age-based cleanup during compaction
   - Tombstone cleanup

5. **Tombstones**
   - Delete markers with optional TTL
   - Proper handling in reads and compactions

6. **API**
   - Put operations
   - Get operations (single version and multi-version)
   - Delete operations
   - Scan operations
   - Flush operations

## Features Missing Compared to HBase

RedBase is a simplified implementation and lacks several features present in Apache HBase:

1. **Distributed Architecture**
   - No RegionServers or distributed storage
   - No ZooKeeper integration for coordination
   - No sharding/partitioning of data across nodes
   - No replication or high availability features

2. **Advanced Features**
   - No coprocessors or custom filters
   - No bloom filters for optimized lookups
   - No block cache for frequently accessed data
   - No compression of stored data
   - No encryption
   - No snapshots
   - No bulk loading

3. **Performance Optimizations**
   - Limited indexing capabilities
   - No bloom filters to skip SSTables
   - No block encoding or compression
   - No off-heap memory management

4. **Administration**
   - No web UI or administrative tools
   - No metrics collection
   - No dynamic configuration
   - No region splitting or merging
   - No balancing of data across nodes

5. **Query Capabilities**
   - Advanced filters and predicates
   - Aggregation functions (count, sum, avg, min, max)
   - No secondary indices

6. **Client Features**
   - Connection pooling for efficient resource management
   - Batch operations for efficient multi-operation transactions
   - Async API for non-blocking operations
   - REST interface for HTTP access

7. **Operational Features**
   - No rolling upgrades
   - No backup and restore utilities
   - No replication for disaster recovery

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/RedBase.git
cd RedBase

# Build the project
cargo build --release
```

## Usage

Here's a simple example of using RedBase:

```rust
use std::path::Path;
use RedBase::api::Table;

fn main() -> std::io::Result<()> {
    // Create a table with a column family
    let mut table = Table::open("./data/example_table")?;
    if table.cf("default").is_none() {
        table.create_cf("default")?;
    }

    let cf = table.cf("default").unwrap();

    // Write some data
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec())?;

    // Read data
    let value = cf.get(b"row1", b"col1")?;
    println!("Value: {:?}", value.map(|v| String::from_utf8_lossy(&v).to_string()));

    // Read multiple versions
    let versions = cf.get_versions(b"row1", b"col1", 10)?;
    for (ts, value) in versions {
        println!("  {} -> {}", ts, String::from_utf8_lossy(&value).to_string());
    }

    // Delete data
    cf.delete(b"row1".to_vec(), b"col1".to_vec())?;

    // Flush to disk
    cf.flush()?;

    // Run compaction
    cf.compact()?;

    // Use advanced filters
    use RedBase::filter::{Filter, FilterSet};

    // Simple filter
    let filter = Filter::Equal(b"value1".to_vec());
    let result = cf.get_with_filter(b"row1", b"col1", &filter)?;

    // Create a filter set for scanning
    let mut filter_set = FilterSet::new();
    filter_set.add_column_filter(
        b"col1".to_vec(),
        Filter::GreaterThan(b"value1".to_vec())
    );

    // Scan with filter
    let scan_result = cf.scan_row_with_filter(b"row1", &filter_set)?;

    // Use aggregations
    use RedBase::aggregation::{AggregationType, AggregationSet};

    // Create an aggregation set
    let mut agg_set = AggregationSet::new();
    agg_set.add_aggregation(b"col1".to_vec(), AggregationType::Count);
    agg_set.add_aggregation(b"col2".to_vec(), AggregationType::Sum);

    // Perform aggregations
    let agg_result = cf.aggregate(b"row1", None, &agg_set)?;

    // Combined filtering and aggregation
    let agg_result = cf.aggregate(b"row1", Some(&filter_set), &agg_set)?;

    Ok(())
}
```

## Detailed Usage Guide

For advanced client features like Async API, Batch Operations, Connection Pooling, and REST Interface, see [ADVANCED_USAGE.md](ADVANCED_USAGE.md).

### Creating Tables and Column Families

Tables in RedBase are directories on disk, and column families are subdirectories within a table directory.

```rust
// Open a table (creates the directory if it doesn't exist)
let mut table = Table::open("./data/my_table")?;

// Create a column family
if table.cf("default").is_none() {
    table.create_cf("default")?;
}

// Get a reference to the column family
let cf = table.cf("default").unwrap();
```

### Writing Data

Data in RedBase is organized by row key, column name, and timestamp. Each write operation automatically assigns a timestamp based on the current time.

```rust
// Write data to a column family
cf.put(b"user1".to_vec(), b"name".to_vec(), b"John Doe".to_vec())?;
cf.put(b"user1".to_vec(), b"email".to_vec(), b"john@example.com".to_vec())?;
cf.put(b"user1".to_vec(), b"age".to_vec(), b"30".to_vec())?;

// Writing to the same row and column creates a new version
cf.put(b"user1".to_vec(), b"name".to_vec(), b"John Smith".to_vec())?;
```

### Reading Data

RedBase provides several ways to read data:

```rust
// Get the latest value for a specific row and column
let name = cf.get(b"user1", b"name")?;
if let Some(value) = name {
    println!("Name: {}", String::from_utf8_lossy(&value));
}

// Get multiple versions of a value (up to 10 versions)
let name_versions = cf.get_versions(b"user1", b"name", 10)?;
for (timestamp, value) in name_versions {
    println!("At {}: {}", timestamp, String::from_utf8_lossy(&value));
}
```

### Multi-Version Concurrency Control (MVCC)

RedBase implements MVCC which allows it to maintain multiple versions of data. Each write operation creates a new version with a timestamp, and read operations can retrieve specific versions.

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
```

### Tombstones and TTL

When you delete data in RedBase, it creates a tombstone marker rather than immediately removing the data. Tombstones can have an optional Time-To-Live (TTL) after which they are eligible for removal during compaction.

```rust
// Delete with no TTL (tombstone never expires automatically)
cf.delete(b"row1".to_vec(), b"col1".to_vec())?;

// Delete with a TTL of 1 hour (3,600,000 milliseconds)
cf.delete_with_ttl(b"row1".to_vec(), b"col2".to_vec(), Some(3600 * 1000))?;
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
- `Regex`: Match using a regular expression pattern (requires UTF-8 values)
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
```

Available aggregation types:
- `Count`: Count the number of values
- `Sum`: Sum the values (must be numeric)
- `Average`: Calculate the average of the values (must be numeric)
- `Min`: Find the minimum value
- `Max`: Find the maximum value

### Example: User Profile Management

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

### Example: Time Series Data

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

## Testing

Run the test suite with:

```bash
cargo test
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
