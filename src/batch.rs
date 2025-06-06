use std::{
    collections::VecDeque,
    io::Result as IoResult,
    sync::Arc,
};

use crate::api::{ColumnFamily as SyncColumnFamily, RowKey, Column};
use crate::async_api::ColumnFamily as AsyncColumnFamily;

/// Represents a single operation in a batch
#[derive(Debug, Clone)]
pub enum BatchOperation {
    /// Put operation: (row, column, value)
    Put(RowKey, Column, Vec<u8>),
    /// Delete operation: (row, column)
    Delete(RowKey, Column),
    /// Delete with TTL operation: (row, column, ttl_ms)
    DeleteWithTTL(RowKey, Column, Option<u64>),
}

/// A batch of operations to be executed together
#[derive(Debug, Clone)]
pub struct Batch {
    operations: VecDeque<BatchOperation>,
}

impl Batch {
    /// Create a new empty batch
    pub fn new() -> Self {
        Self {
            operations: VecDeque::new(),
        }
    }

    /// Add a put operation to the batch
    pub fn put(&mut self, row: RowKey, column: Column, value: Vec<u8>) -> &mut Self {
        self.operations.push_back(BatchOperation::Put(row, column, value));
        self
    }

    /// Add a delete operation to the batch
    pub fn delete(&mut self, row: RowKey, column: Column) -> &mut Self {
        self.operations.push_back(BatchOperation::Delete(row, column));
        self
    }

    /// Add a delete with TTL operation to the batch
    pub fn delete_with_ttl(&mut self, row: RowKey, column: Column, ttl_ms: Option<u64>) -> &mut Self {
        self.operations.push_back(BatchOperation::DeleteWithTTL(row, column, ttl_ms));
        self
    }

    /// Get the number of operations in the batch
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    /// Clear all operations from the batch
    pub fn clear(&mut self) {
        self.operations.clear();
    }
}

impl Default for Batch {
    fn default() -> Self {
        Self::new()
    }
}

/// Extension trait for synchronous ColumnFamily to add batch operations
pub trait SyncBatchExt {
    /// Execute a batch of operations
    fn execute_batch(&self, batch: &Batch) -> IoResult<()>;
}

impl SyncBatchExt for SyncColumnFamily {
    fn execute_batch(&self, batch: &Batch) -> IoResult<()> {
        for op in &batch.operations {
            match op {
                BatchOperation::Put(row, column, value) => {
                    self.put(row.clone(), column.clone(), value.clone())?;
                }
                BatchOperation::Delete(row, column) => {
                    self.delete(row.clone(), column.clone())?;
                }
                BatchOperation::DeleteWithTTL(row, column, ttl_ms) => {
                    self.delete_with_ttl(row.clone(), column.clone(), *ttl_ms)?;
                }
            }
        }
        Ok(())
    }
}

/// Extension trait for asynchronous ColumnFamily to add batch operations
pub trait AsyncBatchExt {
    /// Execute a batch of operations asynchronously
    async fn execute_batch(&self, batch: &Batch) -> IoResult<()>;
}

impl AsyncBatchExt for AsyncColumnFamily {
    async fn execute_batch(&self, batch: &Batch) -> IoResult<()> {
        for op in &batch.operations {
            match op {
                BatchOperation::Put(row, column, value) => {
                    self.put(row.clone(), column.clone(), value.clone()).await?;
                }
                BatchOperation::Delete(row, column) => {
                    self.delete(row.clone(), column.clone()).await?;
                }
                BatchOperation::DeleteWithTTL(row, column, ttl_ms) => {
                    self.delete_with_ttl(row.clone(), column.clone(), *ttl_ms).await?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use crate::api::Table;

    #[test]
    fn test_sync_batch_operations() {
        let dir = tempdir().unwrap();
        let table_path = dir.path();

        // Create a table and column family
        let mut table = Table::open(table_path).unwrap();
        table.create_cf("test_cf").unwrap();
        let cf = table.cf("test_cf").unwrap();

        // Create a batch
        let mut batch = Batch::new();
        batch.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec())
             .put(b"row1".to_vec(), b"col2".to_vec(), b"value2".to_vec())
             .put(b"row2".to_vec(), b"col1".to_vec(), b"value3".to_vec());

        // Execute the batch
        cf.execute_batch(&batch).unwrap();

        // Verify the results
        assert_eq!(cf.get(b"row1", b"col1").unwrap().unwrap(), b"value1");
        assert_eq!(cf.get(b"row1", b"col2").unwrap().unwrap(), b"value2");
        assert_eq!(cf.get(b"row2", b"col1").unwrap().unwrap(), b"value3");

        // Create a batch with delete operations
        let mut batch = Batch::new();
        batch.delete(b"row1".to_vec(), b"col1".to_vec())
             .delete_with_ttl(b"row1".to_vec(), b"col2".to_vec(), Some(3600 * 1000));

        // Execute the batch
        cf.execute_batch(&batch).unwrap();

        // Verify the results
        assert!(cf.get(b"row1", b"col1").unwrap().is_none());
        assert!(cf.get(b"row1", b"col2").unwrap().is_none());
        assert_eq!(cf.get(b"row2", b"col1").unwrap().unwrap(), b"value3");
    }

    #[tokio::test]
    async fn test_async_batch_operations() {
        use crate::async_api::Table as AsyncTable;

        let dir = tempdir().unwrap();
        let table_path = dir.path();

        // Create a table and column family
        let table = AsyncTable::open(table_path).await.unwrap();
        table.create_cf("test_cf").await.unwrap();

        // Add a delay to ensure the column family is properly created
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        let cf = table.cf("test_cf").await.unwrap();

        // Create a batch
        let mut batch = Batch::new();
        batch.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec())
             .put(b"row1".to_vec(), b"col2".to_vec(), b"value2".to_vec())
             .put(b"row2".to_vec(), b"col1".to_vec(), b"value3".to_vec());

        // Execute the batch
        cf.execute_batch(&batch).await.unwrap();

        // Verify the results
        assert_eq!(cf.get(b"row1", b"col1").await.unwrap().unwrap(), b"value1");
        assert_eq!(cf.get(b"row1", b"col2").await.unwrap().unwrap(), b"value2");
        assert_eq!(cf.get(b"row2", b"col1").await.unwrap().unwrap(), b"value3");

        // Create a batch with delete operations
        let mut batch = Batch::new();
        batch.delete(b"row1".to_vec(), b"col1".to_vec())
             .delete_with_ttl(b"row1".to_vec(), b"col2".to_vec(), Some(3600 * 1000));

        // Execute the batch
        cf.execute_batch(&batch).await.unwrap();

        // Verify the results
        assert!(cf.get(b"row1", b"col1").await.unwrap().is_none());
        assert!(cf.get(b"row1", b"col2").await.unwrap().is_none());
        assert_eq!(cf.get(b"row2", b"col1").await.unwrap().unwrap(), b"value3");
    }
}
