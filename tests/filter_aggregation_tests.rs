use std::{
    collections::BTreeMap,
    path::PathBuf,
    thread,
    time::Duration,
};
use tempfile::tempdir;
use RedBase::api::{Table, ColumnFamily};
use RedBase::filter::{Filter, FilterSet, ColumnFilter};
use RedBase::aggregation::{AggregationType, AggregationSet, AggregationResult};

// Helper function to create a temporary directory for a table
fn temp_table_dir() -> (tempfile::TempDir, PathBuf) {
    let dir = tempdir().unwrap();
    let table_path = dir.path().to_path_buf();
    (dir, table_path)
}

#[test]
fn test_filter_equal() {
    let (dir, table_path) = temp_table_dir();

    // Open a new table and create a column family
    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    // Put some values
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col2".to_vec(), b"value2".to_vec()).unwrap();
    cf.put(b"row2".to_vec(), b"col1".to_vec(), b"value3".to_vec()).unwrap();

    // Test Equal filter
    let filter = Filter::Equal(b"value1".to_vec());
    let result = cf.get_with_filter(b"row1", b"col1", &filter).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap(), b"value1");

    // Test Equal filter (no match)
    let filter = Filter::Equal(b"value2".to_vec());
    let result = cf.get_with_filter(b"row1", b"col1", &filter).unwrap();
    assert!(result.is_none());

    drop(dir); // Cleanup
}

#[test]
fn test_filter_contains() {
    let (dir, table_path) = temp_table_dir();

    // Open a new table and create a column family
    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    // Put some values
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"hello world".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col2".to_vec(), b"goodbye world".to_vec()).unwrap();
    cf.put(b"row2".to_vec(), b"col1".to_vec(), b"hello rust".to_vec()).unwrap();

    // Test Contains filter
    let filter = Filter::Contains(b"world".to_vec());
    let result = cf.get_with_filter(b"row1", b"col1", &filter).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap(), b"hello world");

    // Test Contains filter (second match)
    let result = cf.get_with_filter(b"row1", b"col2", &filter).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap(), b"goodbye world");

    // Test Contains filter (no match)
    let result = cf.get_with_filter(b"row2", b"col1", &filter).unwrap();
    assert!(result.is_none());

    drop(dir); // Cleanup
}

#[test]
fn test_filter_set() {
    let (dir, table_path) = temp_table_dir();

    // Open a new table and create a column family
    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    // Put some values
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col2".to_vec(), b"value2".to_vec()).unwrap();
    cf.put(b"row2".to_vec(), b"col1".to_vec(), b"value3".to_vec()).unwrap();
    cf.put(b"row2".to_vec(), b"col2".to_vec(), b"value4".to_vec()).unwrap();

    // Create a filter set
    let mut filter_set = FilterSet::new();
    filter_set.add_column_filter(
        b"col1".to_vec(),
        Filter::Equal(b"value1".to_vec())
    );

    // Test scan_row_with_filter
    let result = cf.scan_row_with_filter(b"row1", &filter_set).unwrap();
    assert_eq!(result.len(), 1);
    assert!(result.contains_key(&b"col1".to_vec()));
    assert!(!result.contains_key(&b"col2".to_vec()));

    // Add another column filter
    filter_set.add_column_filter(
        b"col2".to_vec(),
        Filter::Equal(b"value2".to_vec())
    );

    // Test scan_row_with_filter with multiple column filters
    let result = cf.scan_row_with_filter(b"row1", &filter_set).unwrap();
    assert_eq!(result.len(), 2);
    assert!(result.contains_key(&b"col1".to_vec()));
    assert!(result.contains_key(&b"col2".to_vec()));

    drop(dir); // Cleanup
}

#[test]
fn test_aggregation_count() {
    let (dir, table_path) = temp_table_dir();

    // Open a new table and create a column family
    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    // Put multiple versions of the same cell
    for i in 1..=3 {
        cf.put(
            b"row1".to_vec(), 
            b"col1".to_vec(), 
            format!("value{}", i).into_bytes()
        ).unwrap();
        
        // Small sleep to ensure different timestamps
        thread::sleep(Duration::from_millis(10));
    }

    // Create an aggregation set for counting
    let mut agg_set = AggregationSet::new();
    agg_set.add_aggregation(b"col1".to_vec(), AggregationType::Count);

    // Test aggregation
    let result = cf.aggregate(b"row1", None, &agg_set).unwrap();
    assert_eq!(result.len(), 1);
    
    if let Some(AggregationResult::Count(count)) = result.get(&b"col1".to_vec()) {
        assert_eq!(*count, 3);
    } else {
        panic!("Expected Count aggregation result");
    }

    drop(dir); // Cleanup
}

#[test]
fn test_aggregation_sum() {
    let (dir, table_path) = temp_table_dir();

    // Open a new table and create a column family
    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    // Put numeric values
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"10".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col2".to_vec(), b"20".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col3".to_vec(), b"30".to_vec()).unwrap();

    // Create an aggregation set for summing
    let mut agg_set = AggregationSet::new();
    agg_set.add_aggregation(b"col1".to_vec(), AggregationType::Sum);
    agg_set.add_aggregation(b"col2".to_vec(), AggregationType::Sum);
    agg_set.add_aggregation(b"col3".to_vec(), AggregationType::Sum);

    // Test aggregation
    let result = cf.aggregate(b"row1", None, &agg_set).unwrap();
    assert_eq!(result.len(), 3);
    
    if let Some(AggregationResult::Sum(sum)) = result.get(&b"col1".to_vec()) {
        assert_eq!(*sum, 10);
    } else {
        panic!("Expected Sum aggregation result for col1");
    }
    
    if let Some(AggregationResult::Sum(sum)) = result.get(&b"col2".to_vec()) {
        assert_eq!(*sum, 20);
    } else {
        panic!("Expected Sum aggregation result for col2");
    }
    
    if let Some(AggregationResult::Sum(sum)) = result.get(&b"col3".to_vec()) {
        assert_eq!(*sum, 30);
    } else {
        panic!("Expected Sum aggregation result for col3");
    }

    drop(dir); // Cleanup
}

#[test]
fn test_aggregation_average() {
    let (dir, table_path) = temp_table_dir();

    // Open a new table and create a column family
    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    // Put numeric values
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"10".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"20".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"30".to_vec()).unwrap();

    // Create an aggregation set for averaging
    let mut agg_set = AggregationSet::new();
    agg_set.add_aggregation(b"col1".to_vec(), AggregationType::Average);

    // Test aggregation
    let result = cf.aggregate(b"row1", None, &agg_set).unwrap();
    assert_eq!(result.len(), 1);
    
    if let Some(AggregationResult::Average(avg)) = result.get(&b"col1".to_vec()) {
        assert_eq!(*avg, 20.0);
    } else {
        panic!("Expected Average aggregation result");
    }

    drop(dir); // Cleanup
}

#[test]
fn test_aggregation_min_max() {
    let (dir, table_path) = temp_table_dir();

    // Open a new table and create a column family
    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    // Put values
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"apple".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"banana".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"cherry".to_vec()).unwrap();

    // Create an aggregation set for min and max
    let mut agg_set = AggregationSet::new();
    agg_set.add_aggregation(b"col1".to_vec(), AggregationType::Min);
    
    // Test min aggregation
    let result = cf.aggregate(b"row1", None, &agg_set).unwrap();
    assert_eq!(result.len(), 1);
    
    if let Some(AggregationResult::Min(min)) = result.get(&b"col1".to_vec()) {
        assert_eq!(min, &b"apple".to_vec());
    } else {
        panic!("Expected Min aggregation result");
    }
    
    // Create an aggregation set for max
    let mut agg_set = AggregationSet::new();
    agg_set.add_aggregation(b"col1".to_vec(), AggregationType::Max);
    
    // Test max aggregation
    let result = cf.aggregate(b"row1", None, &agg_set).unwrap();
    assert_eq!(result.len(), 1);
    
    if let Some(AggregationResult::Max(max)) = result.get(&b"col1".to_vec()) {
        assert_eq!(max, &b"cherry".to_vec());
    } else {
        panic!("Expected Max aggregation result");
    }

    drop(dir); // Cleanup
}

#[test]
fn test_filter_and_aggregation() {
    let (dir, table_path) = temp_table_dir();

    // Open a new table and create a column family
    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    // Put numeric values
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"10".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"20".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"30".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"40".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"50".to_vec()).unwrap();

    // Create a filter set to get values > 20
    let mut filter_set = FilterSet::new();
    filter_set.add_column_filter(
        b"col1".to_vec(),
        Filter::GreaterThan(b"20".to_vec())
    );

    // Create an aggregation set for averaging
    let mut agg_set = AggregationSet::new();
    agg_set.add_aggregation(b"col1".to_vec(), AggregationType::Average);

    // Test filtered aggregation
    let result = cf.aggregate(b"row1", Some(&filter_set), &agg_set).unwrap();
    assert_eq!(result.len(), 1);
    
    if let Some(AggregationResult::Average(avg)) = result.get(&b"col1".to_vec()) {
        assert_eq!(*avg, 40.0); // Average of 30, 40, 50
    } else {
        panic!("Expected Average aggregation result");
    }

    drop(dir); // Cleanup
}