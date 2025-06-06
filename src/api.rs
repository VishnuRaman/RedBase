use std::{
    collections::BTreeMap,
    fs,
    io::Result as IoResult,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};
use serde::{Deserialize, Serialize};

use crate::memstore::{MemStore, WalEntry};
use crate::storage::{SSTable, SSTableReader};
use crate::filter::{Filter, FilterSet};
use crate::aggregation::{AggregationSet, AggregationResult};

pub type RowKey = Vec<u8>;
pub type Column = Vec<u8>;
pub type Timestamp = u64;

/// A cell can either be a Put (with actual bytes) or a Delete marker with optional TTL.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum CellValue {
    /// Put operation with data bytes
    Put(Vec<u8>),
    /// Delete marker with optional TTL (time-to-live in milliseconds)
    /// After TTL expires, the tombstone can be removed during compaction
    Delete(Option<u64>),
}

/// Compaction type: minor (merge some SSTables) or major (merge all SSTables)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionType {
    /// Minor compaction: merge a subset of SSTables
    Minor,
    /// Major compaction: merge all SSTables
    Major,
}

/// Compaction options for controlling the compaction process
#[derive(Debug, Clone)]
pub struct CompactionOptions {
    /// Type of compaction to perform
    pub compaction_type: CompactionType,
    /// Maximum number of versions to keep per cell
    pub max_versions: Option<usize>,
    /// Maximum age of versions to keep (in milliseconds)
    pub max_age_ms: Option<u64>,
    /// Whether to clean up expired tombstones
    pub cleanup_tombstones: bool,
}

impl Default for CompactionOptions {
    fn default() -> Self {
        CompactionOptions {
            compaction_type: CompactionType::Minor,
            max_versions: None,
            max_age_ms: None,
            cleanup_tombstones: true,
        }
    }
}

/// Lexicographically‐ordered key for each versioned cell: (row, column, timestamp).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntryKey {
    pub row: RowKey,
    pub column: Column,
    pub timestamp: Timestamp,
}

/// An Entry couples an EntryKey with a CellValue (Put or Delete).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Entry {
    pub key: EntryKey,
    pub value: CellValue,
}

/// A single ColumnFamily inside a Table, with MVCC support and version filtering.
///
/// - *MemStore*: in‐memory BTreeMap + WAL (append‐only).
/// - *SSTables*: on‐disk files (immutable, each is a sorted list of (EntryKey, CellValue)).
/// - *Compaction*: runs periodically to merge SSTables (we keep all versions in compaction).
/// - *MVCC reads*: get_versions(...) and scan_row_versions(...) let you fetch multiple versions.
#[derive(Clone)]
pub struct ColumnFamily {
    name: String,
    path: PathBuf,
    memstore: Arc<Mutex<MemStore>>,
    sst_files: Arc<Mutex<Vec<PathBuf>>>,
}

impl ColumnFamily {
    /// Open (or create) a column family at table_path/colfam_name.
    ///
    /// Spawns a background thread that runs compact() every 60 seconds.
    pub fn open(table_path: &Path, colfam_name: &str) -> IoResult<Self> {
        let cf_path = table_path.join(colfam_name);
        fs::create_dir_all(&cf_path)?;

        let mem = MemStore::open(&cf_path.join("wal.log"))?;

        let mut sst_files = Vec::new();
        for entry in fs::read_dir(&cf_path)? {
            let e = entry?;
            if let Some(ext) = e.path().extension() {
                if ext == "sst" {
                    sst_files.push(e.path());
                }
            }
        }
        sst_files.sort();

        let cf = ColumnFamily {
            name: colfam_name.to_string(),
            path: cf_path.clone(),
            memstore: Arc::new(Mutex::new(mem)),
            sst_files: Arc::new(Mutex::new(sst_files)),
        };

        {
            let cf_clone = cf.clone();
            thread::spawn(move || {
                loop {
                    thread::sleep(Duration::from_secs(60));
                    if let Err(err) = cf_clone.compact() {
                        eprintln!(
                            "[ColumnFamily::compact] error in CF '{}': {:?}",
                            cf_clone.name, err
                        );
                    }
                }
            });
        }

        Ok(cf)
    }

    /// Write a new versioned cell (row, column) = value with a fresh timestamp.
    pub fn put(&self, row: RowKey, column: Column, value: Vec<u8>) -> IoResult<()> {
        let ts = chrono::Utc::now().timestamp_millis() as u64;
        let entry = Entry {
            key: EntryKey { row, column, timestamp: ts },
            value: CellValue::Put(value),
        };
        let mut ms = self.memstore.lock().unwrap();
        ms.append(entry)?;
        if ms.len() > 10_000 {
            drop(ms);
            self.flush()?;
        }
        Ok(())
    }

    /// Mark (row, column) as deleted by writing a tombstone at the current timestamp.
    /// The tombstone will never expire (no TTL).
    pub fn delete(&self, row: RowKey, column: Column) -> IoResult<()> {
        self.delete_with_ttl(row, column, None)
    }

    /// Mark (row, column) as deleted by writing a tombstone with a specified TTL.
    /// After the TTL expires, the tombstone can be removed during compaction.
    /// 
    /// # Arguments
    /// * `row` - The row key
    /// * `column` - The column name
    /// * `ttl_ms` - Optional TTL in milliseconds. If None, the tombstone never expires.
    pub fn delete_with_ttl(&self, row: RowKey, column: Column, ttl_ms: Option<u64>) -> IoResult<()> {
        let ts = chrono::Utc::now().timestamp_millis() as u64;
        let entry = Entry {
            key: EntryKey { row, column, timestamp: ts },
            value: CellValue::Delete(ttl_ms),
        };
        let mut ms = self.memstore.lock().unwrap();
        ms.append(entry)?;
        if ms.len() > 10_000 {
            drop(ms);
            self.flush()?;
        }
        Ok(())
    }

    /// *Get* the single latest value for (row, column).
    /// If the latest version is a tombstone, returns Ok(None).
    /// Otherwise returns Ok(Some(value_bytes)).
    pub fn get(&self, row: &[u8], column: &[u8]) -> IoResult<Option<Vec<u8>>> {
        let ms = self.memstore.lock().unwrap();
        if let Some(cell) = ms.get_full(row, column) {
            return match cell {
                CellValue::Put(data) => Ok(Some(data.clone())),
                CellValue::Delete(_) => Ok(None),
            };
        }
        drop(ms);

        let sst_list = self.sst_files.lock().unwrap();
        for sst_path in sst_list.iter().rev() {
            let mut reader = SSTableReader::open(sst_path)?;
            if let Some(cell) = reader.get_full(row, column)? {
                return match cell {
                    CellValue::Put(data) => Ok(Some(data)),
                    CellValue::Delete(_) => Ok(None),
                };
            }
        }
        Ok(None)
    }

    /// *MVCC read*: return up to max_versions recent (timestamp, value) for (row, column).
    /// - Versions are sorted descending by timestamp.
    /// - Tombstone versions (CellValue::Delete) are skipped entirely.
    pub fn get_versions(
        &self,
        row: &[u8],
        column: &[u8],
        max_versions: usize,
    ) -> IoResult<Vec<(Timestamp, Vec<u8>)>> {
        let mut all_versions: Vec<(Timestamp, CellValue)> = Vec::new();

        // Collect versions from memstore
        {
            let ms = self.memstore.lock().unwrap();
            all_versions.extend(ms.get_versions_full(row, column));
        }

        // Collect versions from SSTable files
        let sst_list = self.sst_files.lock().unwrap();
        // Use map and collect to handle IoResult properly
        let readers: IoResult<Vec<_>> = sst_list.iter()
            .map(|sst_path| SSTableReader::open(sst_path))
            .collect();

        // Process each reader
        for mut reader in readers? {
            all_versions.extend(reader.get_versions_full(row, column)?);
        }

        // Sort by timestamp (descending)
        all_versions.sort_by(|a, b| b.0.cmp(&a.0));

        // Filter for Put values and limit to max_versions
        let result = all_versions.into_iter()
            .filter_map(|(ts, cell)| {
                if let CellValue::Put(v) = cell {
                    Some((ts, v))
                } else {
                    None
                }
            })
            .take(max_versions)
            .collect();

        Ok(result)
    }

    /// *MVCC scan*: for each column under row, return up to max_versions_per_column recent (timestamp, value).
    /// - Tombstone versions are skipped.
    /// - If a column has fewer than max_versions_per_column puts, you get as many as exist.
    pub fn scan_row_versions(
        &self,
        row: &[u8],
        max_versions_per_column: usize,
    ) -> IoResult<BTreeMap<Column, Vec<(Timestamp, Vec<u8>)>>> {
        let mut per_column: BTreeMap<Column, Vec<(Timestamp, CellValue)>> = BTreeMap::new();
        {
            let sst_list = self.sst_files.lock().unwrap();
            // Use map and collect to handle IoResult properly
            let readers: IoResult<Vec<_>> = sst_list.iter()
                .map(|sst_path| SSTableReader::open(sst_path))
                .collect();

            // Process each reader
            for mut reader in readers? {
                // Use iterator methods to process scan_row_full results
                reader.scan_row_full(row)?.into_iter().for_each(|(col, ts, cell)| {
                    per_column.entry(col.clone()).or_default().push((ts, cell.clone()));
                });
            }
        }

        {
            let ms = self.memstore.lock().unwrap();
            // Use iterator methods to process memstore entries
            ms.scan_row_full(row).into_iter().for_each(|(entry_key, cell)| {
                per_column
                    .entry(entry_key.column.clone())
                    .or_default()
                    .push((entry_key.timestamp, cell.clone()));
            });
        }

        // Process each column's versions using iterators
        let result: BTreeMap<Column, Vec<(Timestamp, Vec<u8>)>> = per_column
            .into_iter()
            .filter_map(|(col, mut versions)| {
                // Sort by timestamp (descending)
                versions.sort_by(|a, b| b.0.cmp(&a.0));

                // Filter for Put values and limit to max_versions_per_column
                let kept: Vec<(Timestamp, Vec<u8>)> = versions.into_iter()
                    .filter_map(|(ts, cell)| {
                        if let CellValue::Put(v) = cell {
                            Some((ts, v))
                        } else {
                            None
                        }
                    })
                    .take(max_versions_per_column)
                    .collect();

                // Only include non-empty columns
                if !kept.is_empty() {
                    Some((col.clone(), kept))
                } else {
                    None
                }
            })
            .collect();

        Ok(result)
    }

    /// Flush the MemStore into a new SSTable file, then clear the MemStore + WAL.
    pub fn flush(&self) -> IoResult<()> {
        let mut ms = self.memstore.lock().unwrap();
        if ms.is_empty() {
            return Ok(());
        }

        let sst_seq = {
            let existing = self.sst_files.lock().unwrap();
            existing.len() + 1
        };
        let sst_name = format!("{:010}.sst", sst_seq as u64);
        let sst_path = self.path.join(&sst_name);

        let entries = ms.drain_all()?;
        SSTable::create(&sst_path, &entries)?;

        self.sst_files.lock().unwrap().push(sst_path);
        Ok(())
    }


    /// *Compact* all on-disk SSTables into one, preserving all versions (no dropping).
    /// After merging, the old SSTables are deleted, and replaced by a single new .sst.
    /// 
    /// This is a convenience method that calls compact_with_options with default options.
    pub fn compact(&self) -> IoResult<()> {
        self.compact_with_options(CompactionOptions::default())
    }

    /// Run a major compaction that merges all SSTables into one.
    /// This is more aggressive than the default compact() method, which only does minor compaction.
    pub fn major_compact(&self) -> IoResult<()> {
        let mut options = CompactionOptions::default();
        options.compaction_type = CompactionType::Major;
        self.compact_with_options(options)
    }

    /// Run a compaction with version cleanup, keeping only the specified number of versions.
    /// 
    /// # Arguments
    /// * `max_versions` - Maximum number of versions to keep per cell
    pub fn compact_with_max_versions(&self, max_versions: usize) -> IoResult<()> {
        let mut options = CompactionOptions::default();
        options.max_versions = Some(max_versions);
        self.compact_with_options(options)
    }

    /// Run a compaction with age-based cleanup, removing versions older than the specified age.
    /// 
    /// # Arguments
    /// * `max_age_ms` - Maximum age of versions to keep (in milliseconds)
    pub fn compact_with_max_age(&self, max_age_ms: u64) -> IoResult<()> {
        let mut options = CompactionOptions::default();
        options.max_age_ms = Some(max_age_ms);
        self.compact_with_options(options)
    }

    /// Get a value with a filter applied
    /// 
    /// # Arguments
    /// * `row` - The row key
    /// * `column` - The column name
    /// * `filter` - The filter to apply to the value
    pub fn get_with_filter(&self, row: &[u8], column: &[u8], filter: &Filter) -> IoResult<Option<Vec<u8>>> {
        let value = self.get(row, column)?;

        if let Some(data) = value {
            if filter.matches(&data) {
                Ok(Some(data))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Scan a row with a filter set applied
    /// 
    /// # Arguments
    /// * `row` - The row key
    /// * `filter_set` - The filter set to apply
    pub fn scan_row_with_filter(
        &self,
        row: &[u8],
        filter_set: &FilterSet,
    ) -> IoResult<BTreeMap<Column, Vec<(Timestamp, Vec<u8>)>>> {
        let max_versions = filter_set.max_versions.unwrap_or(usize::MAX);
        let mut result = self.scan_row_versions(row, max_versions)?;

        if !filter_set.column_filters.is_empty() {
            let filter_columns: Vec<Vec<u8>> = filter_set.column_filters
                .iter()
                .map(|cf| cf.column.clone())
                .collect();

            result.retain(|column, _| filter_columns.contains(column));
        }

        for column_filter in &filter_set.column_filters {
            if let Some(versions) = result.get_mut(&column_filter.column) {
                let filtered_versions: Vec<(Timestamp, Vec<u8>)> = versions
                    .iter()
                    .filter(|(ts, value)| {
                        filter_set.timestamp_matches(*ts) && column_filter.filter.matches(value)
                    })
                    .cloned()
                    .collect();

                if filtered_versions.is_empty() {
                    result.remove(&column_filter.column);
                } else {
                    *versions = filtered_versions;
                }
            }
        }

        Ok(result)
    }

    /// Scan multiple rows with a filter set applied
    /// 
    /// # Arguments
    /// * `start_row` - The starting row key (inclusive)
    /// * `end_row` - The ending row key (inclusive)
    /// * `filter_set` - The filter set to apply
    pub fn scan_with_filter(
        &self,
        start_row: &[u8],
        end_row: &[u8],
        filter_set: &FilterSet,
    ) -> IoResult<BTreeMap<RowKey, BTreeMap<Column, Vec<(Timestamp, Vec<u8>)>>>> {
        let mut result = BTreeMap::new();

        let mut row_keys = self.get_row_keys_in_range(start_row, end_row)?;

        for row_key in row_keys {
            let row_result = self.scan_row_with_filter(&row_key, filter_set)?;
            if !row_result.is_empty() {
                result.insert(row_key, row_result);
            }
        }

        Ok(result)
    }

    /// Helper method to get all row keys in a range
    fn get_row_keys_in_range(&self, start_row: &[u8], end_row: &[u8]) -> IoResult<Vec<RowKey>> {
        let mut row_keys = BTreeMap::new();

        {
            let ms = self.memstore.lock().unwrap();
            let keys = ms.get_row_keys_in_range(start_row, end_row);
            for row_key in keys {
                row_keys.insert(row_key, ());
            }
        }

        let sst_list = self.sst_files.lock().unwrap();
        for sst_path in sst_list.iter() {
            let mut reader = SSTableReader::open(sst_path)?;
            for row_key in reader.get_row_keys_in_range(start_row, end_row)? {
                row_keys.insert(row_key, ());
            }
        }

        Ok(row_keys.into_keys().collect())
    }

    /// Perform aggregations on query results
    /// 
    /// # Arguments
    /// * `row` - The row key
    /// * `filter_set` - Optional filter set to apply before aggregation
    /// * `aggregation_set` - The aggregations to perform
    pub fn aggregate(
        &self,
        row: &[u8],
        filter_set: Option<&FilterSet>,
        aggregation_set: &AggregationSet,
    ) -> IoResult<BTreeMap<Column, AggregationResult>> {
        let data = if let Some(fs) = filter_set {
            self.scan_row_with_filter(row, fs)?
        } else {
            self.scan_row_versions(row, usize::MAX)?
        };

        Ok(aggregation_set.apply(&data))
    }

    /// Perform aggregations on multiple rows
    /// 
    /// # Arguments
    /// * `start_row` - The starting row key (inclusive)
    /// * `end_row` - The ending row key (inclusive)
    /// * `filter_set` - Optional filter set to apply before aggregation
    /// * `aggregation_set` - The aggregations to perform
    pub fn aggregate_range(
        &self,
        start_row: &[u8],
        end_row: &[u8],
        filter_set: Option<&FilterSet>,
        aggregation_set: &AggregationSet,
    ) -> IoResult<BTreeMap<RowKey, BTreeMap<Column, AggregationResult>>> {
        let mut result = BTreeMap::new();

        let row_keys = self.get_row_keys_in_range(start_row, end_row)?;

        for row_key in row_keys {
            let row_result = self.aggregate(&row_key, filter_set, aggregation_set)?;
            if !row_result.is_empty() {
                result.insert(row_key, row_result);
            }
        }

        Ok(result)
    }

    /// *Compact* SSTables with the specified options.
    /// 
    /// # Arguments
    /// * `options` - Options controlling the compaction process
    pub fn compact_with_options(&self, options: CompactionOptions) -> IoResult<()> {
        let current_paths = {
            let guard = self.sst_files.lock().unwrap();
            guard.clone()
        };

        if current_paths.len() <= 1 && options.compaction_type == CompactionType::Minor {
            return Ok(());
        }

        let mut max_seq: u64 = 0;
        for path in current_paths.iter() {
            if let Some(fname) = path.file_name().and_then(|os| os.to_str()) {
                if let Some(stripped) = fname.strip_suffix(".sst") {
                    if let Ok(seq) = stripped.parse::<u64>() {
                        max_seq = max_seq.max(seq);
                    }
                }
            }
        }
        let new_seq = max_seq + 1;
        let new_fname = format!("{:010}.sst", new_seq);
        let new_sst_path = self.path.join(&new_fname);

        let tables_to_compact = match options.compaction_type {
            CompactionType::Major => current_paths.clone(),
            CompactionType::Minor => {
                let mut tables = current_paths.clone();
                tables.sort();
                let count = (tables.len() / 2).max(2).min(tables.len());
                tables[0..count].to_vec()
            }
        };

        if tables_to_compact.is_empty() {
            return Ok(());
        }

        // Collect entries from all tables to compact
        let mut merged: Vec<Entry> = Vec::new();
        {
            // Use flat_map to process all tables
            let entries: IoResult<Vec<_>> = tables_to_compact.iter()
                .map(|path| {
                    let mut reader = SSTableReader::open(path)?;
                    // Map each (entry_key, cell) to an Entry
                    let table_entries: Vec<Entry> = reader.scan_all()?
                        .into_iter()
                        .map(|(entry_key, cell)| Entry {
                            key: entry_key.clone(),
                            value: cell.clone(),
                        })
                        .collect();
                    Ok(table_entries)
                })
                .collect();

            // Flatten the nested vectors and extend merged
            merged.extend(entries?.into_iter().flatten());
        }

        merged.sort_by(|a, b| a.key.cmp(&b.key));

        if options.max_versions.is_some() || options.max_age_ms.is_some() || options.cleanup_tombstones {
            let now = chrono::Utc::now().timestamp_millis() as u64;

            // Group entries by row and column using iterators
            let grouped: BTreeMap<(Vec<u8>, Vec<u8>), Vec<Entry>> = merged
                .into_iter()
                .fold(BTreeMap::new(), |mut acc, entry| {
                    let key = (entry.key.row.clone(), entry.key.column.clone());
                    acc.entry(key).or_default().push(entry);
                    acc
                });

            // Process each group of entries using iterators
            let filtered: Vec<Entry> = grouped.into_iter()
                .flat_map(|(_, mut entries)| {
                    // Sort by timestamp (descending)
                    entries.sort_by(|a, b| b.key.timestamp.cmp(&a.key.timestamp));

                    // Use fold to maintain state while filtering entries
                    entries.into_iter()
                        .fold((Vec::new(), false), |(mut kept, mut seen_non_tombstone), entry| {
                            let keep = match &entry.value {
                                CellValue::Put(_) => {
                                    let within_version_limit = options.max_versions
                                        .map(|max| kept.len() < max)
                                        .unwrap_or(true);

                                    let within_age_limit = options.max_age_ms
                                        .map(|max_age| now - entry.key.timestamp <= max_age)
                                        .unwrap_or(true);

                                    within_version_limit && within_age_limit
                                },
                                CellValue::Delete(ttl) => {
                                    if options.cleanup_tombstones {
                                        match ttl {
                                            Some(ttl_ms) => {
                                                entry.key.timestamp + ttl_ms > now
                                            },
                                            None => {
                                                !seen_non_tombstone
                                            }
                                        }
                                    } else {
                                        true
                                    }
                                }
                            };

                            if keep {
                                if let CellValue::Put(_) = entry.value {
                                    seen_non_tombstone = true;
                                }
                                kept.push(entry);
                            }

                            (kept, seen_non_tombstone)
                        })
                        .0  // Return just the kept entries
                })
                .collect();

            merged = filtered;
        }

        SSTable::create(&new_sst_path, &merged)?;

        let mut list_guard = self.sst_files.lock().unwrap();

        // Remove old SSTable files using iterators
        tables_to_compact.iter().for_each(|old_path| {
            let _ = std::fs::remove_file(old_path);
        });

        if options.compaction_type == CompactionType::Major {
            *list_guard = vec![new_sst_path];
        } else {
            list_guard.retain(|path| !tables_to_compact.contains(path));
            list_guard.push(new_sst_path);
            list_guard.sort(); 
        }

        Ok(())
    }
}

/// A Table is a directory containing one or more ColumnFamily subdirectories.
#[derive(Clone)]
pub struct Table {
    path: PathBuf,
    column_families: BTreeMap<String, ColumnFamily>,
}

impl Table {
    /// Open (or create) a table directory.
    pub fn open(table_dir: impl AsRef<Path>) -> IoResult<Self> {
        let tbl_path = table_dir.as_ref().to_path_buf();
        fs::create_dir_all(&tbl_path)?;
        // Process directory entries using iterators
        let mut cfs = BTreeMap::new();

        // Use try_fold to handle errors properly
        fs::read_dir(&tbl_path)?.try_for_each(|entry_result| -> IoResult<()> {
            let entry = entry_result?;
            if entry.file_type()?.is_dir() {
                let name = entry.file_name().into_string().unwrap();
                let cf = ColumnFamily::open(&tbl_path, &name)?;
                cfs.insert(name, cf);
            }
            Ok(())
        })?;

        Ok(Table {
            path: tbl_path,
            column_families: cfs,
        })
    }

    /// Create a new column family named cf_name. Fails if it already exists.
    pub fn create_cf(&mut self, cf_name: &str) -> IoResult<()> {
        if self.column_families.contains_key(cf_name) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("ColumnFamily {} already exists", cf_name),
            ));
        }
        let cf = ColumnFamily::open(&self.path, cf_name)?;
        self.column_families.insert(cf_name.to_string(), cf);
        Ok(())
    }

    /// Retrieve a handle to an existing ColumnFamily (or None if it doesn’t exist).
    pub fn cf(&self, cf_name: &str) -> Option<ColumnFamily> {
        self.column_families.get(cf_name).cloned()
    }
}
