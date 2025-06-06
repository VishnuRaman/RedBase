use std::{
    io::Result as IoResult,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use deadpool::managed::{Manager, Object, Pool, PoolError};
use async_trait::async_trait;

use crate::api::Table as SyncTable;
use crate::async_api::Table as AsyncTable;

/// A connection to a RedBase table
#[derive(Clone)]
pub struct Connection {
    /// The path to the table
    pub path: PathBuf,
    /// The async table handle
    pub table: AsyncTable,
}

/// A manager for RedBase connections
pub struct ConnectionManager {
    /// The base directory for tables
    base_dir: PathBuf,
}

impl ConnectionManager {
    /// Create a new connection manager with the given base directory
    pub fn new<P: AsRef<Path>>(base_dir: P) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
        }
    }
}

#[async_trait]
impl Manager for ConnectionManager {
    type Type = Connection;
    type Error = std::io::Error;

    async fn create(&self) -> Result<Connection, Self::Error> {
        let table_path = self.base_dir.clone();
        let table = AsyncTable::open(&table_path).await?;

        Ok(Connection {
            path: table_path,
            table,
        })
    }

    async fn recycle(&self, _conn: &mut Connection) -> Result<(), deadpool::managed::RecycleError<Self::Error>> {
        // Check if the connection is still valid
        // For now, we'll just assume it's valid
        Ok(())
    }
}

/// A pool of RedBase connections
pub struct ConnectionPool {
    pool: Pool<ConnectionManager>,
}

impl ConnectionPool {
    /// Create a new connection pool with the given base directory and size
    pub fn new<P: AsRef<Path>>(base_dir: P, size: usize) -> Self {
        let manager = ConnectionManager::new(base_dir);
        let pool = Pool::builder(manager)
            .max_size(size)
            .build()
            .expect("Failed to create connection pool");

        Self { pool }
    }

    /// Get a connection from the pool
    pub async fn get(&self) -> Result<Object<ConnectionManager>, PoolError<std::io::Error>> {
        self.pool.get().await
    }
}

/// A synchronous connection to a RedBase table
pub struct SyncConnection {
    /// The path to the table
    pub path: PathBuf,
    /// The sync table handle
    pub table: SyncTable,
}

/// A synchronous manager for RedBase connections
pub struct SyncConnectionManager {
    /// The base directory for tables
    base_dir: PathBuf,
    /// Lock to ensure thread safety
    lock: Arc<Mutex<()>>,
}

impl SyncConnectionManager {
    /// Create a new synchronous connection manager with the given base directory
    pub fn new<P: AsRef<Path>>(base_dir: P) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            lock: Arc::new(Mutex::new(())),
        }
    }

    /// Create a new connection
    pub fn create(&self) -> IoResult<SyncConnection> {
        let _guard = self.lock.lock().unwrap();
        let table_path = self.base_dir.clone();
        let table = SyncTable::open(&table_path)?;

        Ok(SyncConnection {
            path: table_path,
            table,
        })
    }

    /// Check if a connection is still valid
    pub fn recycle(&self, _conn: &mut SyncConnection) -> IoResult<()> {
        // For now, we'll just assume it's valid
        Ok(())
    }
}

/// A simple synchronous connection pool
pub struct SyncConnectionPool {
    manager: SyncConnectionManager,
    connections: Arc<Mutex<Vec<SyncConnection>>>,
    max_size: usize,
}

impl SyncConnectionPool {
    /// Create a new synchronous connection pool with the given base directory and size
    pub fn new<P: AsRef<Path>>(base_dir: P, size: usize) -> Self {
        let manager = SyncConnectionManager::new(base_dir);

        Self {
            manager,
            connections: Arc::new(Mutex::new(Vec::with_capacity(size))),
            max_size: size,
        }
    }

    /// Get a connection from the pool
    pub fn get(&self) -> IoResult<SyncConnection> {
        let mut connections = self.connections.lock().unwrap();

        if let Some(conn) = connections.pop() {
            // Recycle the connection to ensure it's still valid
            if self.manager.recycle(&mut SyncConnection { 
                path: conn.path.clone(), 
                table: conn.table.clone() 
            }).is_ok() {
                return Ok(conn);
            }
        }

        // Create a new connection if the pool is empty or the recycled connection is invalid
        self.manager.create()
    }

    /// Return a connection to the pool
    pub fn put(&self, conn: SyncConnection) {
        let mut connections = self.connections.lock().unwrap();

        if connections.len() < self.max_size {
            connections.push(conn);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_async_connection_pool() {
        let dir = tempdir().unwrap();
        let table_path = dir.path();

        // Create a connection pool
        let pool = ConnectionPool::new(table_path, 5);

        // Get a connection from the pool
        let conn1 = pool.get().await.unwrap();

        // Drop the connection to return it to the pool
        drop(conn1);

        // Get another connection from the pool
        let _conn2 = pool.get().await.unwrap();

        // If we got here without errors, the test passes
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
}
