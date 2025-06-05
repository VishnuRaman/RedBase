use std::collections::BTreeMap;
use serde::{Deserialize, Serialize};

/// Represents the type of aggregation to perform on a column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregationType {
    /// Count the number of values
    Count,
    /// Sum the values (must be numeric)
    Sum,
    /// Calculate the average of the values (must be numeric)
    Average,
    /// Find the minimum value
    Min,
    /// Find the maximum value
    Max,
}

/// Represents an aggregation to be performed on a specific column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Aggregation {
    /// The column to aggregate
    pub column: Vec<u8>,
    /// The type of aggregation to perform
    pub aggregation_type: AggregationType,
}

/// Result of an aggregation operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregationResult {
    /// Count result
    Count(u64),
    /// Sum result (as i64)
    Sum(i64),
    /// Sum result (as f64)
    SumFloat(f64),
    /// Average result
    Average(f64),
    /// Minimum value
    Min(Vec<u8>),
    /// Maximum value
    Max(Vec<u8>),
    /// Error during aggregation
    Error(String),
}

impl AggregationResult {
    /// Convert the aggregation result to a string representation
    pub fn to_string(&self) -> String {
        match self {
            AggregationResult::Count(count) => format!("{}", count),
            AggregationResult::Sum(sum) => format!("{}", sum),
            AggregationResult::SumFloat(sum) => format!("{}", sum),
            AggregationResult::Average(avg) => format!("{}", avg),
            AggregationResult::Min(min) => format!("{:?}", min),
            AggregationResult::Max(max) => format!("{:?}", max),
            AggregationResult::Error(err) => format!("Error: {}", err),
        }
    }
}

/// Represents a set of aggregations to be performed on query results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationSet {
    /// The aggregations to perform
    pub aggregations: Vec<Aggregation>,
}

impl AggregationSet {
    /// Create a new empty aggregation set
    pub fn new() -> Self {
        AggregationSet {
            aggregations: Vec::new(),
        }
    }

    /// Add an aggregation to the set
    pub fn add_aggregation(&mut self, column: Vec<u8>, aggregation_type: AggregationType) -> &mut Self {
        self.aggregations.push(Aggregation {
            column,
            aggregation_type,
        });
        self
    }

    /// Apply the aggregations to a set of values
    pub fn apply(&self, values: &BTreeMap<Vec<u8>, Vec<(u64, Vec<u8>)>>) -> BTreeMap<Vec<u8>, AggregationResult> {
        let mut results = BTreeMap::new();

        for aggregation in &self.aggregations {
            let result = match values.get(&aggregation.column) {
                Some(column_values) => {
                    match aggregation.aggregation_type {
                        AggregationType::Count => {
                            AggregationResult::Count(column_values.len() as u64)
                        },
                        AggregationType::Sum => {
                            // Try to parse values as integers
                            let mut sum_i64 = 0i64;
                            let mut sum_f64 = 0.0f64;
                            let mut is_float = false;

                            for (_, value) in column_values {
                                if let Ok(value_str) = std::str::from_utf8(value) {
                                    if let Ok(num) = value_str.parse::<i64>() {
                                        sum_i64 += num;
                                    } else if let Ok(num) = value_str.parse::<f64>() {
                                        sum_f64 += num;
                                        is_float = true;
                                    } else {
                                        return BTreeMap::from([(
                                            aggregation.column.clone(),
                                            AggregationResult::Error("Non-numeric value found".to_string())
                                        )]);
                                    }
                                } else {
                                    return BTreeMap::from([(
                                        aggregation.column.clone(),
                                        AggregationResult::Error("Invalid UTF-8 in value".to_string())
                                    )]);
                                }
                            }

                            if is_float {
                                AggregationResult::SumFloat(sum_f64)
                            } else {
                                AggregationResult::Sum(sum_i64)
                            }
                        },
                        AggregationType::Average => {
                            if column_values.is_empty() {
                                AggregationResult::Error("No values to average".to_string())
                            } else {
                                // Try to parse values as numbers
                                let mut sum = 0.0;
                                let count = column_values.len() as f64;

                                for (_, value) in column_values {
                                    if let Ok(value_str) = std::str::from_utf8(value) {
                                        if let Ok(num) = value_str.parse::<f64>() {
                                            sum += num;
                                        } else {
                                            return BTreeMap::from([(
                                                aggregation.column.clone(),
                                                AggregationResult::Error("Non-numeric value found".to_string())
                                            )]);
                                        }
                                    } else {
                                        return BTreeMap::from([(
                                            aggregation.column.clone(),
                                            AggregationResult::Error("Invalid UTF-8 in value".to_string())
                                        )]);
                                    }
                                }

                                AggregationResult::Average(sum / count)
                            }
                        },
                        AggregationType::Min => {
                            if column_values.is_empty() {
                                AggregationResult::Error("No values to find minimum".to_string())
                            } else {
                                let min_value = column_values.iter()
                                    .min_by(|(_, a), (_, b)| a.cmp(b))
                                    .map(|(_, v)| v.clone())
                                    .unwrap();
                                AggregationResult::Min(min_value)
                            }
                        },
                        AggregationType::Max => {
                            if column_values.is_empty() {
                                AggregationResult::Error("No values to find maximum".to_string())
                            } else {
                                let max_value = column_values.iter()
                                    .max_by(|(_, a), (_, b)| a.cmp(b))
                                    .map(|(_, v)| v.clone())
                                    .unwrap();
                                AggregationResult::Max(max_value)
                            }
                        },
                    }
                },
                None => AggregationResult::Error(format!("Column not found: {:?}", aggregation.column)),
            };

            results.insert(aggregation.column.clone(), result);
        }

        results
    }
}

impl Default for AggregationSet {
    fn default() -> Self {
        Self::new()
    }
}