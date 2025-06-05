use std::cmp::Ordering;
use serde::{Deserialize, Serialize};

/// Filter represents a predicate that can be applied to cell values
/// to determine if they should be included in query results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Filter {
    /// Match values equal to the given value
    Equal(Vec<u8>),
    /// Match values not equal to the given value
    NotEqual(Vec<u8>),
    /// Match values greater than the given value
    GreaterThan(Vec<u8>),
    /// Match values greater than or equal to the given value
    GreaterThanOrEqual(Vec<u8>),
    /// Match values less than the given value
    LessThan(Vec<u8>),
    /// Match values less than or equal to the given value
    LessThanOrEqual(Vec<u8>),
    /// Match values that contain the given substring
    Contains(Vec<u8>),
    /// Match values that start with the given prefix
    StartsWith(Vec<u8>),
    /// Match values that end with the given suffix
    EndsWith(Vec<u8>),
    /// Match values that match the given regex pattern
    /// Note: This is a placeholder for future implementation
    Regex(String),
    /// Combine multiple filters with AND logic (all must match)
    And(Vec<Filter>),
    /// Combine multiple filters with OR logic (any must match)
    Or(Vec<Filter>),
    /// Negate the result of the contained filter
    Not(Box<Filter>),
}

impl Filter {
    /// Apply the filter to a value and return true if it matches
    pub fn matches(&self, value: &[u8]) -> bool {
        match self {
            Filter::Equal(target) => value == target.as_slice(),
            Filter::NotEqual(target) => value != target.as_slice(),
            Filter::GreaterThan(target) => value > target.as_slice(),
            Filter::GreaterThanOrEqual(target) => value >= target.as_slice(),
            Filter::LessThan(target) => value < target.as_slice(),
            Filter::LessThanOrEqual(target) => value <= target.as_slice(),
            Filter::Contains(target) => contains_subsequence(value, target),
            Filter::StartsWith(target) => value.starts_with(target),
            Filter::EndsWith(target) => value.ends_with(target),
            Filter::Regex(_pattern) => {
                // Placeholder for future implementation
                // Would require a regex crate dependency
                false
            },
            Filter::And(filters) => filters.iter().all(|f| f.matches(value)),
            Filter::Or(filters) => filters.iter().any(|f| f.matches(value)),
            Filter::Not(filter) => !filter.matches(value),
        }
    }
}

/// Helper function to check if a value contains a subsequence
fn contains_subsequence(value: &[u8], subsequence: &[u8]) -> bool {
    if subsequence.is_empty() {
        return true;
    }
    if subsequence.len() > value.len() {
        return false;
    }
    
    for i in 0..=(value.len() - subsequence.len()) {
        if value[i..(i + subsequence.len())] == subsequence[..] {
            return true;
        }
    }
    false
}

/// Represents a column filter that can be applied to scan operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnFilter {
    /// The column to filter on
    pub column: Vec<u8>,
    /// The filter to apply to the column's values
    pub filter: Filter,
}

/// Represents a set of filters that can be applied to scan operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterSet {
    /// Column filters to apply (AND logic between different columns)
    pub column_filters: Vec<ColumnFilter>,
    /// Optional timestamp range filter
    pub timestamp_range: Option<(Option<u64>, Option<u64>)>,
    /// Maximum number of versions to return per cell
    pub max_versions: Option<usize>,
}

impl FilterSet {
    /// Create a new empty filter set
    pub fn new() -> Self {
        FilterSet {
            column_filters: Vec::new(),
            timestamp_range: None,
            max_versions: None,
        }
    }

    /// Add a column filter
    pub fn add_column_filter(&mut self, column: Vec<u8>, filter: Filter) -> &mut Self {
        self.column_filters.push(ColumnFilter { column, filter });
        self
    }

    /// Set the timestamp range filter
    pub fn with_timestamp_range(&mut self, min: Option<u64>, max: Option<u64>) -> &mut Self {
        self.timestamp_range = Some((min, max));
        self
    }

    /// Set the maximum number of versions to return per cell
    pub fn with_max_versions(&mut self, max_versions: usize) -> &mut Self {
        self.max_versions = Some(max_versions);
        self
    }

    /// Check if a timestamp is within the specified range
    pub fn timestamp_matches(&self, timestamp: u64) -> bool {
        if let Some((min, max)) = self.timestamp_range {
            let min_match = min.map_or(true, |min_ts| timestamp >= min_ts);
            let max_match = max.map_or(true, |max_ts| timestamp <= max_ts);
            min_match && max_match
        } else {
            true
        }
    }
}

impl Default for FilterSet {
    fn default() -> Self {
        Self::new()
    }
}