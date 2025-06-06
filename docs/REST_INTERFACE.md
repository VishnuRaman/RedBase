# RedBase REST Interface

This document provides an overview of the REST interface for RedBase, which allows you to interact with the database over HTTP.

## Starting the REST Server

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

## API Endpoints

### Health Check

```
GET /health
```

Response:
```json
{
  "status": "ok"
}
```

### Create a Column Family

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

### Put a Value

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

### Get a Value

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

### Delete a Value

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

### Batch Operations

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

### Scan a Row

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

### Filter a Row

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

### Aggregate a Row

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

### Flush a Column Family

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

### Compact a Column Family

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