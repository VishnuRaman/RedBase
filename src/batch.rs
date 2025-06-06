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
    Put(RowKey, Column, Vec<u8>),
    Delete(RowKey, Column),
    DeleteWithTTL(RowKey, Column, Option<u64>),
}

#[derive(Debug, Clone)]
pub struct Batch {
    operations: VecDeque<BatchOperation>,
}

impl Batch {
    pub fn new() -> Self {
        Self {
            operations: VecDeque::new(),
        }
    }

    pub fn put(&mut self, row: RowKey, column: Column, value: Vec<u8>) -> &mut Self {
        self.operations.push_back(BatchOperation::Put(row, column, value));
        self
    }

    pub fn delete(&mut self, row: RowKey, column: Column) -> &mut Self {
        self.operations.push_back(BatchOperation::Delete(row, column));
        self
    }

    pub fn delete_with_ttl(&mut self, row: RowKey, column: Column, ttl_ms: Option<u64>) -> &mut Self {
        self.operations.push_back(BatchOperation::DeleteWithTTL(row, column, ttl_ms));
        self
    }

    pub fn len(&self) -> usize {
        self.operations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    pub fn clear(&mut self) {
        self.operations.clear();
    }
}

impl Default for Batch {
    fn default() -> Self {
        Self::new()
    }
}

pub trait SyncBatchExt {
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

pub trait AsyncBatchExt {
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

        let mut table = Table::open(table_path).unwrap();
        table.create_cf("test_cf").unwrap();
        let cf = table.cf("test_cf").unwrap();

        let mut batch = Batch::new();
        batch.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec())
             .put(b"row1".to_vec(), b"col2".to_vec(), b"value2".to_vec())
             .put(b"row2".to_vec(), b"col1".to_vec(), b"value3".to_vec());

        cf.execute_batch(&batch).unwrap();

        assert_eq!(cf.get(b"row1", b"col1").unwrap().unwrap(), b"value1");
        assert_eq!(cf.get(b"row1", b"col2").unwrap().unwrap(), b"value2");
        assert_eq!(cf.get(b"row2", b"col1").unwrap().unwrap(), b"value3");

        let mut batch = Batch::new();
        batch.delete(b"row1".to_vec(), b"col1".to_vec())
             .delete_with_ttl(b"row1".to_vec(), b"col2".to_vec(), Some(3600 * 1000));

        cf.execute_batch(&batch).unwrap();

        assert!(cf.get(b"row1", b"col1").unwrap().is_none());
        assert!(cf.get(b"row1", b"col2").unwrap().is_none());
        assert_eq!(cf.get(b"row2", b"col1").unwrap().unwrap(), b"value3");
    }

    #[tokio::test]
    async fn test_async_batch_operations() {
        use crate::async_api::Table as AsyncTable;

        let dir = tempdir().unwrap();
        let table_path = dir.path();

        let table = AsyncTable::open(table_path).await.unwrap();
        table.create_cf("test_cf").await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        let cf = table.cf("test_cf").await.unwrap();

        let mut batch = Batch::new();
        batch.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec())
             .put(b"row1".to_vec(), b"col2".to_vec(), b"value2".to_vec())
             .put(b"row2".to_vec(), b"col1".to_vec(), b"value3".to_vec());

        cf.execute_batch(&batch).await.unwrap();

        assert_eq!(cf.get(b"row1", b"col1").await.unwrap().unwrap(), b"value1");
        assert_eq!(cf.get(b"row1", b"col2").await.unwrap().unwrap(), b"value2");
        assert_eq!(cf.get(b"row2", b"col1").await.unwrap().unwrap(), b"value3");

        let mut batch = Batch::new();
        batch.delete(b"row1".to_vec(), b"col1".to_vec())
             .delete_with_ttl(b"row1".to_vec(), b"col2".to_vec(), Some(3600 * 1000));

        cf.execute_batch(&batch).await.unwrap();

        assert!(cf.get(b"row1", b"col1").await.unwrap().is_none());
        assert!(cf.get(b"row1", b"col2").await.unwrap().is_none());
        assert_eq!(cf.get(b"row2", b"col1").await.unwrap().unwrap(), b"value3");
    }
}
