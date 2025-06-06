# RedBase Advanced Usage Guide

This document provides detailed instructions on how to use the advanced client features of RedBase, including:

- Asynchronous API
- Batch Operations
- Connection Pooling
- REST Interface

## Asynchronous API

RedBase provides an asynchronous API that allows you to interact with the database without blocking the current thread. This is particularly useful for applications that need to handle multiple concurrent requests.

### Basic Usage

```rust
use std::path::Path;
use RedBase::async_api::Table;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Open a table asynchronously
    let table = Table::open("./data/my_table").await?;
    
    // Create a column family
    table.create_cf("default").await?;
    
    // Get a reference to the column family
    let cf = table.cf("default").await?.unwrap();
    
    // Write data asynchronously
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec()).await?;
    
    // Read data asynchronously
    let value = cf.get(b"row1", b"col1").await?;
    println!("Value: {:?}", value.map(|v| String::from_utf8_lossy(&v).to_string()));
    
    // Delete data asynchronously
    cf.delete(b"row1".to_vec(), b"col1".to_vec()).await?;
    
    // Flush to disk asynchronously
    cf.flush().await?;
    
    // Run compaction asynchronously
    cf.compact().await?;
    
    Ok(())
}
```

### Advanced Async Operations

The async API supports all the same operations as the synchronous API, including:

- Multi-version reads
- Scanning
- Filtering
- Aggregation

```rust
use RedBase::async_api::Table;
use RedBase::filter::{Filter, FilterSet};
use RedBase::aggregation::{AggregationType, AggregationSet};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let table = Table::open("./data/my_table").await?;
    let cf = table.cf("default").await?.unwrap();
    
    // Get multiple versions
    let versions = cf.get_versions(b"row1", b"col1", 10).await?;
    for (ts, value) in versions {
        println!("At {}: {}", ts, String::from_utf8_lossy(&value));
    }
    
    // Scan a row
    let row_data = cf.scan_row_versions(b"row1", 10).await?;
    for (column, versions) in row_data {
        println!("Column: {}", String::from_utf8_lossy(&column));
        for (ts, value) in versions {
            println!("  {} -> {}", ts, String::from_utf8_lossy(&value));
        }
    }
    
    // Use filters
    let filter = Filter::Equal(b"value1".to_vec());
    let result = cf.get_with_filter(b"row1", b"col1", &filter).await?;
    
    // Create a filter set
    let mut filter_set = FilterSet::new();
    filter_set.add_column_filter(
        b"col1".to_vec(),
        Filter::GreaterThan(b"value1".to_vec())
    );
    
    // Scan with filter
    let scan_result = cf.scan_row_with_filter(b"row1", &filter_set).await?;
    
    // Use aggregations
    let mut agg_set = AggregationSet::new();
    agg_set.add_aggregation(b"col1".to_vec(), AggregationType::Count);
    
    // Perform aggregations
    let agg_result = cf.aggregate(b"row1", None, &agg_set).await?;
    
    Ok(())
}
```

## Batch Operations

Batch operations allow you to perform multiple operations in a single transaction, which is more efficient than performing them one by one.

### Synchronous Batch Operations

```rust
use RedBase::api::Table;
use RedBase::batch::{Batch, SyncBatchExt};

fn main() -> std::io::Result<()> {
    let mut table = Table::open("./data/my_table")?;
    let cf = table.cf("default").unwrap();
    
    // Create a batch
    let mut batch = Batch::new();
    batch.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec())
         .put(b"row1".to_vec(), b"col2".to_vec(), b"value2".to_vec())
         .put(b"row2".to_vec(), b"col1".to_vec(), b"value3".to_vec());
    
    // Execute the batch
    cf.execute_batch(&batch)?;
    
    // Create a batch with delete operations
    let mut batch = Batch::new();
    batch.delete(b"row1".to_vec(), b"col1".to_vec())
         .delete_with_ttl(b"row1".to_vec(), b"col2".to_vec(), Some(3600 * 1000));
    
    // Execute the batch
    cf.execute_batch(&batch)?;
    
    Ok(())
}
```

### Asynchronous Batch Operations

```rust
use RedBase::async_api::Table;
use RedBase::batch::{Batch, AsyncBatchExt};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let table = Table::open("./data/my_table").await?;
    let cf = table.cf("default").await?.unwrap();
    
    // Create a batch
    let mut batch = Batch::new();
    batch.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec())
         .put(b"row1".to_vec(), b"col2".to_vec(), b"value2".to_vec())
         .put(b"row2".to_vec(), b"col1".to_vec(), b"value3".to_vec());
    
    // Execute the batch asynchronously
    cf.execute_batch(&batch).await?;
    
    Ok(())
}
```

## Connection Pooling

Connection pooling allows you to efficiently reuse connections to the database, which is important for performance in multi-user scenarios.

### Synchronous Connection Pool

```rust
use RedBase::pool::SyncConnectionPool;

fn main() -> std::io::Result<()> {
    // Create a connection pool with 10 connections
    let pool = SyncConnectionPool::new("./data/my_table", 10);
    
    // Get a connection from the pool
    let conn = pool.get()?;
    
    // Use the connection
    let cf = conn.table.cf("default").unwrap();
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec())?;
    
    // Return the connection to the pool
    pool.put(conn);
    
    // Get another connection from the pool
    let conn2 = pool.get()?;
    
    // The connection will have access to the same data
    let cf2 = conn2.table.cf("default").unwrap();
    let value = cf2.get(b"row1", b"col1")?;
    assert_eq!(value.unwrap(), b"value1");
    
    Ok(())
}
```

### Asynchronous Connection Pool

```rust
use RedBase::pool::ConnectionPool;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Create a connection pool with 10 connections
    let pool = ConnectionPool::new("./data/my_table", 10);
    
    // Get a connection from the pool
    let conn = pool.get().await?;
    
    // Use the connection
    let cf = conn.table.cf("default").await?.unwrap();
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec()).await?;
    
    // The connection will be returned to the pool when it's dropped
    drop(conn);
    
    // Get another connection from the pool
    let conn2 = pool.get().await?;
    
    // The connection will have access to the same data
    let cf2 = conn2.table.cf("default").await?.unwrap();
    let value = cf2.get(b"row1", b"col1").await?;
    assert_eq!(value.unwrap(), b"value1");
    
    Ok(())
}
```

## REST Interface

RedBase provides a REST API that allows you to interact with the database over HTTP. This is useful for web applications and microservices.

### Starting the REST Server

```rust
use RedBase::rest::{RestConfig, start_server};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Create a REST server configuration
    let config = RestConfig {
        base_dir: "./data".into(),
        host: "127.0.0.1".into(),
        port: 8080,
        pool_size: 10,
    };
    
    // Start the REST server
    start_server(config).await
}
```

### Using the REST API

Once the REST server is running, you can interact with it using HTTP requests:

#### Health Check

```
GET /health
```

Response:
```json
{
  "status": "ok"
}
```

#### Create a Column Family

```
POST /tables/{table}/cf
```

Request body:
```json
{
  "name": "default"
}
```

Response:
```json
{
  "status": "created",
  "table": "my_table",
  "column_family": "default"
}
```

#### Put a Value

```
POST /tables/{table}/cf/{cf}/put
```

Request body:
```json
{
  "row": "row1",
  "column": "col1",
  "value": "value1"
}
```

Response:
```json
{
  "status": "ok",
  "table": "my_table",
  "column_family": "default",
  "row": "row1",
  "column": "col1"
}
```

#### Get a Value

```
POST /tables/{table}/cf/{cf}/get
```

Request body:
```json
{
  "row": "row1",
  "column": "col1"
}
```

Response:
```json
{
  "value": "value1"
}
```

#### Delete a Value

```
POST /tables/{table}/cf/{cf}/delete
```

Request body:
```json
{
  "row": "row1",
  "column": "col1"
}
```

Response:
```json
{
  "status": "ok",
  "table": "my_table",
  "column_family": "default",
  "row": "row1",
  "column": "col1"
}
```

#### Batch Operations

```
POST /tables/{table}/cf/{cf}/batch
```

Request body:
```json
{
  "operations": [
    {
      "type": "Put",
      "data": {
        "row": "row1",
        "column": "col1",
        "value": "value1"
      }
    },
    {
      "type": "Put",
      "data": {
        "row": "row1",
        "column": "col2",
        "value": "value2"
      }
    },
    {
      "type": "Delete",
      "data": {
        "row": "row2",
        "column": "col1"
      }
    }
  ]
}
```

Response:
```json
{
  "status": "ok",
  "table": "my_table",
  "column_family": "default",
  "operations_count": 3
}
```

#### Scan a Row

```
POST /tables/{table}/cf/{cf}/scan
```

Request body:
```json
{
  "row": "row1",
  "max_versions_per_column": 10
}
```

Response:
```json
{
  "col1": [
    {
      "timestamp": 1609459200000,
      "value": "value1"
    }
  ],
  "col2": [
    {
      "timestamp": 1609459200000,
      "value": "value2"
    }
  ]
}
```

#### Filter a Row

```
POST /tables/{table}/cf/{cf}/filter
```

Request body:
```json
{
  "row": "row1",
  "filter_set": {
    "column_filters": [
      {
        "column": "col1",
        "filter": {
          "type": "Equal",
          "data": "value1"
        }
      }
    ],
    "timestamp_range": [null, null],
    "max_versions": 10
  }
}
```

Response:
```json
{
  "col1": [
    {
      "timestamp": 1609459200000,
      "value": "value1"
    }
  ]
}
```

#### Aggregate a Row

```
POST /tables/{table}/cf/{cf}/aggregate
```

Request body:
```json
{
  "row": "row1",
  "aggregation_set": {
    "aggregations": [
      {
        "column": "col1",
        "aggregation_type": "count"
      },
      {
        "column": "col2",
        "aggregation_type": "sum"
      }
    ]
  }
}
```

Response:
```json
{
  "col1": "1",
  "col2": "2"
}
```

#### Flush a Column Family

```
POST /tables/{table}/cf/{cf}/flush
```

Response:
```json
{
  "status": "ok",
  "table": "my_table",
  "column_family": "default"
}
```

#### Compact a Column Family

```
POST /tables/{table}/cf/{cf}/compact
```

Response:
```json
{
  "status": "ok",
  "table": "my_table",
  "column_family": "default"
}
```