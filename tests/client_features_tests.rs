use std::path::Path;
use tempfile::tempdir;

use RedBase::api::Table as SyncTable;
use RedBase::async_api::Table as AsyncTable;
use RedBase::batch::{Batch, SyncBatchExt, AsyncBatchExt};
use RedBase::pool::{ConnectionPool, SyncConnectionPool};

#[tokio::test]
async fn test_async_api() {
    let dir = tempdir().unwrap();
    let table_path = dir.path();

    // Open a table asynchronously
    let table = AsyncTable::open(table_path).await.unwrap();

    // Create a column family
    table.create_cf("test_cf").await.unwrap();

    // Add a delay to ensure the column family is properly created
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Get the column family
    let cf = table.cf("test_cf").await.unwrap();

    // Put some data
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec()).await.unwrap();
    cf.put(b"row1".to_vec(), b"col2".to_vec(), b"value2".to_vec()).await.unwrap();
    cf.put(b"row2".to_vec(), b"col1".to_vec(), b"value3".to_vec()).await.unwrap();

    // Get the data
    let value1 = cf.get(b"row1", b"col1").await.unwrap();
    let value2 = cf.get(b"row1", b"col2").await.unwrap();
    let value3 = cf.get(b"row2", b"col1").await.unwrap();

    assert_eq!(value1.unwrap(), b"value1");
    assert_eq!(value2.unwrap(), b"value2");
    assert_eq!(value3.unwrap(), b"value3");

    // Delete some data
    cf.delete(b"row1".to_vec(), b"col1".to_vec()).await.unwrap();

    // Verify it's gone
    let value1 = cf.get(b"row1", b"col1").await.unwrap();
    assert!(value1.is_none());

    // Flush to disk
    cf.flush().await.unwrap();

    // Compact
    cf.compact().await.unwrap();
}

#[tokio::test]
async fn test_async_batch_operations() {
    let dir = tempdir().unwrap();
    let table_path = dir.path();

    // Open a table asynchronously
    let table = AsyncTable::open(table_path).await.unwrap();

    // Create a column family
    table.create_cf("test_cf").await.unwrap();

    // Add a delay to ensure the column family is properly created
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Get the column family
    let cf = table.cf("test_cf").await.unwrap();

    // Create a batch
    let mut batch = Batch::new();
    batch.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec())
         .put(b"row1".to_vec(), b"col2".to_vec(), b"value2".to_vec())
         .put(b"row2".to_vec(), b"col1".to_vec(), b"value3".to_vec());

    // Execute the batch
    cf.execute_batch(&batch).await.unwrap();

    // Verify the results
    let value1 = cf.get(b"row1", b"col1").await.unwrap();
    let value2 = cf.get(b"row1", b"col2").await.unwrap();
    let value3 = cf.get(b"row2", b"col1").await.unwrap();

    assert_eq!(value1.unwrap(), b"value1");
    assert_eq!(value2.unwrap(), b"value2");
    assert_eq!(value3.unwrap(), b"value3");

    // Create a batch with delete operations
    let mut batch = Batch::new();
    batch.delete(b"row1".to_vec(), b"col1".to_vec())
         .delete_with_ttl(b"row1".to_vec(), b"col2".to_vec(), Some(3600 * 1000));

    // Execute the batch
    cf.execute_batch(&batch).await.unwrap();

    // Verify the results
    let value1 = cf.get(b"row1", b"col1").await.unwrap();
    let value2 = cf.get(b"row1", b"col2").await.unwrap();

    assert!(value1.is_none());
    assert!(value2.is_none());
}

#[test]
fn test_sync_batch_operations() {
    let dir = tempdir().unwrap();
    let table_path = dir.path();

    // Open a table synchronously
    let mut table = SyncTable::open(table_path).unwrap();

    // Create a column family
    table.create_cf("test_cf").unwrap();

    // Get the column family
    let cf = table.cf("test_cf").unwrap();

    // Create a batch
    let mut batch = Batch::new();
    batch.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec())
         .put(b"row1".to_vec(), b"col2".to_vec(), b"value2".to_vec())
         .put(b"row2".to_vec(), b"col1".to_vec(), b"value3".to_vec());

    // Execute the batch
    cf.execute_batch(&batch).unwrap();

    // Verify the results
    let value1 = cf.get(b"row1", b"col1").unwrap();
    let value2 = cf.get(b"row1", b"col2").unwrap();
    let value3 = cf.get(b"row2", b"col1").unwrap();

    assert_eq!(value1.unwrap(), b"value1");
    assert_eq!(value2.unwrap(), b"value2");
    assert_eq!(value3.unwrap(), b"value3");
}

#[tokio::test]
async fn test_connection_pool() {
    let dir = tempdir().unwrap();
    let table_path = dir.path();

    // Create a connection pool
    let pool = ConnectionPool::new(table_path, 5);

    // Get a connection from the pool
    let conn = pool.get().await.unwrap();

    // Create a column family
    conn.table.create_cf("test_cf").await.unwrap();

    // Add a delay to ensure the column family is properly created
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Get the column family
    let cf = conn.table.cf("test_cf").await.unwrap();

    // Put some data
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec()).await.unwrap();

    // Get the data
    let value = cf.get(b"row1", b"col1").await.unwrap();
    assert_eq!(value.unwrap(), b"value1");

    // The connection will be returned to the pool when it's dropped
    drop(conn);

    // Get another connection from the pool
    let conn2 = pool.get().await.unwrap();

    // The column family should still exist
    let cf2 = conn2.table.cf("test_cf").await.unwrap();

    // The data should still be there
    let value2 = cf2.get(b"row1", b"col1").await.unwrap();
    assert_eq!(value2.unwrap(), b"value1");
}

#[test]
fn test_sync_connection_pool() {
    let dir = tempdir().unwrap();
    let table_path = dir.path();

    // Create a connection pool
    let pool = SyncConnectionPool::new(table_path, 5);

    // Get a connection from the pool
    let mut conn = pool.get().unwrap();

    // Create a column family
    conn.table.create_cf("test_cf").unwrap();

    // Get the column family
    let cf = conn.table.cf("test_cf").unwrap();

    // Put some data
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec()).unwrap();

    // Get the data
    let value = cf.get(b"row1", b"col1").unwrap();
    assert_eq!(value.unwrap(), b"value1");

    // Return the connection to the pool
    pool.put(conn);

    // Get another connection from the pool
    let conn2 = pool.get().unwrap();

    // The column family should still exist
    let cf2 = conn2.table.cf("test_cf").unwrap();

    // The data should still be there
    let value2 = cf2.get(b"row1", b"col1").unwrap();
    assert_eq!(value2.unwrap(), b"value1");
}
