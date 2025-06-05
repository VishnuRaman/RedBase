use bincode;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufReader, Read, Result as IoResult, Seek, SeekFrom, Write},
    path::Path,
};
use crate::api::{CellValue, Entry, EntryKey, Timestamp};

/// A single WAL record: binary‐encoded Entry.
#[derive(Serialize, Deserialize, Debug)]
pub struct WalEntry(Entry);

/// MemStore holds an in‐memory BTreeMap<EntryKey, CellValue> plus an append‐only WAL file.
pub struct MemStore {
    map: BTreeMap<EntryKey, CellValue>,
    wal: File,
    wal_path: String,
}

impl MemStore {
    /// Open (or create) a WAL at wal_path and replay it to rebuild map.
    pub fn open(wal_path: impl AsRef<Path>) -> IoResult<Self> {
        let path_str = wal_path.as_ref().to_string_lossy().into_owned();
        let wal = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&wal_path)?;
        let mut store = MemStore {
            map: BTreeMap::new(),
            wal,
            wal_path: path_str.clone(),
        };

        // Replay existing WAL
        let mut reader = BufReader::new(store.wal.try_clone()?);
        loop {
            let mut len_buf = [0u8; 4];
            if reader.read_exact(&mut len_buf).is_err() {
                break;
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf)?;
            let WalEntry(entry) = bincode::deserialize(&buf).unwrap();
            store.map.insert(entry.key, entry.value);
        }
        // Seek to end so future appends go there
        store.wal.seek(SeekFrom::End(0))?;
        Ok(store)
    }

    /// Number of entries in the in-memory map
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Append one Entry to both the WAL file (on disk) and map (in memory).
    pub fn append(&mut self, entry: Entry) -> IoResult<()> {
        let buf = bincode::serialize(&WalEntry(entry.clone())).unwrap();
        let len = (buf.len() as u32).to_be_bytes();
        self.wal.write_all(&len)?;
        self.wal.write_all(&buf)?;
        self.wal.flush()?;

        self.map.insert(entry.key, entry.value);
        Ok(())
    }

    /// Get the *latest* CellValue for (row, column) from in‐memory map (if any).
    pub fn get_full(&self, row: &[u8], column: &[u8]) -> Option<&CellValue> {
        // Range from (row, column, 0) to (row, column, u64::MAX), then take .last()
        let range_start = EntryKey {
            row: row.to_vec(),
            column: column.to_vec(),
            timestamp: 0,
        };
        let range_end = EntryKey {
            row: row.to_vec(),
            column: column.to_vec(),
            timestamp: u64::MAX,
        };
        self.map
            .range(range_start..=range_end)
            .last()
            .map(|(_k, v)| v)
    }

    /// *MVCC helper*: return all versions (timestamp + CellValue) for (row, column), sorted descending by timestamp.
    pub fn get_versions_full(&self, row: &[u8], column: &[u8]) -> Vec<(Timestamp, CellValue)> {
        let mut versions = Vec::new();
        let range_start = EntryKey {
            row: row.to_vec(),
            column: column.to_vec(),
            timestamp: 0,
        };
        let range_end = EntryKey {
            row: row.to_vec(),
            column: column.to_vec(),
            timestamp: u64::MAX,
        };
        for (k, v) in self.map.range(range_start..=range_end) {
            versions.push((k.timestamp, v.clone()));
        }
        versions.sort_by(|a, b| b.0.cmp(&a.0));
        versions
    }

    pub fn drain_all(&mut self) -> IoResult<Vec<Entry>> {
        let mut all = Vec::with_capacity(self.map.len());
        for (k, v) in self.map.iter() {
            all.push(Entry {
                key: k.clone(),
                value: v.clone(),
            });
        }
        all.sort_by(|a, b| a.key.cmp(&b.key));
        self.map.clear();

        drop(&self.wal);
        std::fs::remove_file(&self.wal_path)?;
        self.wal = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&self.wal_path)?;
        Ok(all)
    }

    /// For scanning: return all (EntryKey, CellValue) for a given row (in-memory).  
    /// Useful to merge with SSTables when doing versioned scans.
    pub fn scan_row_full(&self, row: &[u8]) -> Vec<(EntryKey, CellValue)> {
        let mut result = Vec::new();
        let range_start = EntryKey {
            row: row.to_vec(),
            column: vec![],
            timestamp: 0,
        };
        let range_end = EntryKey {
            row: row.to_vec(),
            column: vec![0xFF],
            timestamp: u64::MAX,
        };
        for (k, v) in self.map.range(range_start..=range_end) {
            if k.row == row {
                result.push((k.clone(), v.clone()));
            }
        }
        result
    }

    /// Scan a range of rows and return all (EntryKey, CellValue) pairs.
    /// The range is inclusive of start_row and end_row.
    pub fn scan_range(&self, start_row: &[u8], end_row: &[u8]) -> Vec<(EntryKey, CellValue)> {
        let mut result = Vec::new();

        let range_start = EntryKey {
            row: start_row.to_vec(),
            column: vec![],
            timestamp: 0,
        };
        let range_end = EntryKey {
            row: end_row.to_vec(),
            column: vec![0xFF],
            timestamp: u64::MAX,
        };

        for (k, v) in self.map.range(range_start..=range_end) {
            if k.row.as_slice() >= start_row && k.row.as_slice() <= end_row {
                result.push((k.clone(), v.clone()));
            }
        }

        result
    }

    /// Get all unique row keys in a range.
    pub fn get_row_keys_in_range(&self, start_row: &[u8], end_row: &[u8]) -> Vec<Vec<u8>> {
        let mut row_keys = std::collections::BTreeSet::new();

        for (k, _) in self.scan_range(start_row, end_row) {
            row_keys.insert(k.row);
        }

        row_keys.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{CellValue, Entry, EntryKey};
    use std::fs;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn temp_wal_path() -> (tempfile::TempDir, PathBuf) {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        (dir, wal_path)
    }

    #[test]
    fn test_memstore_open_empty() {
        let (dir, wal_path) = temp_wal_path();
        let store = MemStore::open(&wal_path).unwrap();
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
        drop(store);
        drop(dir); // Cleanup
    }

    #[test]
    fn test_memstore_append_and_get() {
        let (dir, wal_path) = temp_wal_path();
        let mut store = MemStore::open(&wal_path).unwrap();

        let entry = Entry {
            key: EntryKey {
                row: b"row1".to_vec(),
                column: b"col1".to_vec(),
                timestamp: 100,
            },
            value: CellValue::Put(b"value1".to_vec()),
        };
        store.append(entry).unwrap();

        // Check length
        assert_eq!(store.len(), 1);
        assert!(!store.is_empty());

        // Get the value
        let value = store.get_full(b"row1", b"col1");
        assert!(value.is_some());
        match value.unwrap() {
            CellValue::Put(data) => assert_eq!(data, b"value1"),
            _ => panic!("Expected Put value"),
        }

        // Get a non-existent value
        let value = store.get_full(b"row2", b"col1");
        assert!(value.is_none());

        drop(store);
        drop(dir); // Cleanup
    }

    #[test]
    fn test_memstore_get_versions_full() {
        let (dir, wal_path) = temp_wal_path();
        let mut store = MemStore::open(&wal_path).unwrap();

        // Append multiple versions of the same cell
        for i in 1..=3 {
            let entry = Entry {
                key: EntryKey {
                    row: b"row1".to_vec(),
                    column: b"col1".to_vec(),
                    timestamp: i * 100, // Different timestamps
                },
                value: CellValue::Put(format!("value{}", i).into_bytes()),
            };
            store.append(entry).unwrap();
        }

        // Get all versions
        let versions = store.get_versions_full(b"row1", b"col1");
        assert_eq!(versions.len(), 3);

        // Check they're sorted by timestamp (descending)
        assert_eq!(versions[0].0, 300);
        assert_eq!(versions[1].0, 200);
        assert_eq!(versions[2].0, 100);

        // Check values
        match &versions[0].1 {
            CellValue::Put(data) => assert_eq!(data, b"value3"),
            _ => panic!("Expected Put value"),
        }

        drop(store);
        drop(dir); // Cleanup
    }

    #[test]
    fn test_memstore_drain_all() {
        let (dir, wal_path) = temp_wal_path();
        let mut store = MemStore::open(&wal_path).unwrap();

        // Append multiple entries
        for i in 1..=3 {
            let entry = Entry {
                key: EntryKey {
                    row: format!("row{}", i).into_bytes(),
                    column: b"col1".to_vec(),
                    timestamp: 100,
                },
                value: CellValue::Put(format!("value{}", i).into_bytes()),
            };
            store.append(entry).unwrap();
        }

        assert_eq!(store.len(), 3);

        // Drain all entries
        let entries = store.drain_all().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());

        // Check entries are sorted by key
        assert_eq!(String::from_utf8_lossy(&entries[0].key.row), "row1");
        assert_eq!(String::from_utf8_lossy(&entries[1].key.row), "row2");
        assert_eq!(String::from_utf8_lossy(&entries[2].key.row), "row3");

        drop(store);
        drop(dir); // Cleanup
    }

    #[test]
    fn test_memstore_scan_row_full() {
        let (dir, wal_path) = temp_wal_path();
        let mut store = MemStore::open(&wal_path).unwrap();

        // Append multiple entries for the same row but different columns
        for i in 1..=3 {
            let entry = Entry {
                key: EntryKey {
                    row: b"row1".to_vec(),
                    column: format!("col{}", i).into_bytes(),
                    timestamp: 100 + i as u64,
                },
                value: CellValue::Put(format!("value{}", i).into_bytes()),
            };
            store.append(entry).unwrap();
        }

        // Add an entry for a different row
        let entry = Entry {
            key: EntryKey {
                row: b"row2".to_vec(),
                column: b"col1".to_vec(),
                timestamp: 100,
            },
            value: CellValue::Put(b"other_value".to_vec()),
        };
        store.append(entry).unwrap();

        // Scan row1
        let results = store.scan_row_full(b"row1");
        assert_eq!(results.len(), 3);

        // Verify all results are for row1
        for (key, _) in &results {
            assert_eq!(key.row, b"row1");
        }

        // Scan row2
        let results = store.scan_row_full(b"row2");
        assert_eq!(results.len(), 1);

        // Scan non-existent row
        let results = store.scan_row_full(b"row3");
        assert_eq!(results.len(), 0);

        drop(store);
        drop(dir); // Cleanup
    }

    #[test]
    fn test_memstore_wal_persistence() {
        let (dir, wal_path) = temp_wal_path();

        // Create a store and add some entries
        {
            let mut store = MemStore::open(&wal_path).unwrap();
            for i in 1..=3 {
                let entry = Entry {
                    key: EntryKey {
                        row: b"row1".to_vec(),
                        column: format!("col{}", i).into_bytes(),
                        timestamp: 100 + i as u64,
                    },
                    value: CellValue::Put(format!("value{}", i).into_bytes()),
                };
                store.append(entry).unwrap();
            }
            assert_eq!(store.len(), 3);
        } // store is dropped here

        // Reopen the store and check if entries were persisted
        {
            let store = MemStore::open(&wal_path).unwrap();
            assert_eq!(store.len(), 3);

            // Check if we can retrieve the values
            for i in 1..=3 {
                let col = format!("col{}", i).into_bytes();
                let value = store.get_full(b"row1", &col);
                assert!(value.is_some());
                match value.unwrap() {
                    CellValue::Put(data) => assert_eq!(data, format!("value{}", i).as_bytes()),
                    _ => panic!("Expected Put value"),
                }
            }
        }

        drop(dir); // Cleanup
    }

    #[test]
    fn test_memstore_tombstone() {
        let (dir, wal_path) = temp_wal_path();
        let mut store = MemStore::open(&wal_path).unwrap();

        // Add a value
        let entry = Entry {
            key: EntryKey {
                row: b"row1".to_vec(),
                column: b"col1".to_vec(),
                timestamp: 100,
            },
            value: CellValue::Put(b"value1".to_vec()),
        };
        store.append(entry).unwrap();

        // Add a tombstone with a later timestamp
        let entry = Entry {
            key: EntryKey {
                row: b"row1".to_vec(),
                column: b"col1".to_vec(),
                timestamp: 200,
            },
            value: CellValue::Delete(None),
        };
        store.append(entry).unwrap();

        // Get the latest value (should be the tombstone)
        let value = store.get_full(b"row1", b"col1");
        assert!(value.is_some());
        match value.unwrap() {
            CellValue::Delete(_) => {}, // Expected
            _ => panic!("Expected Delete value"),
        }

        // Get all versions
        let versions = store.get_versions_full(b"row1", b"col1");
        assert_eq!(versions.len(), 2);

        // First version should be the tombstone
        match &versions[0].1 {
            CellValue::Delete(_) => assert_eq!(versions[0].0, 200),
            _ => panic!("Expected Delete value"),
        }

        // Second version should be the original value
        match &versions[1].1 {
            CellValue::Put(data) => {
                assert_eq!(versions[1].0, 100);
                assert_eq!(data, b"value1");
            },
            _ => panic!("Expected Put value"),
        }

        drop(store);
        drop(dir); // Cleanup
    }
}
