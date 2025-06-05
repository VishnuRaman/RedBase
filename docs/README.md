# RedBase Documentation

This directory contains comprehensive documentation for the RedBase project, a lightweight HBase-like database implemented in Rust.

## Documentation Structure

- [README.md](../README.md) - Project overview, features, and basic usage examples
- [CONTRIBUTING.md](../CONTRIBUTING.md) - Guidelines for contributing to the project
- [usage_guide.txt](usage_guide.txt) - Detailed usage guide with examples for all features

## Quick Start

If you're new to RedBase, start with the main [README.md](../README.md) to get an overview of the project and its features.

For detailed usage instructions, refer to the [usage_guide.txt](usage_guide.txt) file, which covers:

1. Getting Started
   - Installation
   - Basic Setup

2. Basic Operations
   - Creating Tables and Column Families
   - Writing Data
   - Reading Data
   - Deleting Data

3. Advanced Features
   - Multi-Version Concurrency Control (MVCC)
   - Tombstones and TTL
   - Scanning
   - Compaction
   - Filtering
   - Aggregation

4. Performance Considerations
   - Memory Management
   - Compaction

5. Error Handling

6. Examples
   - User Profile Management
   - Time Series Data

## API Documentation

The RedBase API is organized into several modules:

- `api.rs` - Main API for tables and column families
- `memstore.rs` - In-memory storage with Write-Ahead Log (WAL)
- `storage.rs` - On-disk storage (SSTables)
- `filter.rs` - Filtering capabilities
- `aggregation.rs` - Aggregation functions

Each module has its own set of types and functions. The most commonly used types are:

- `Table` - Container for multiple Column Families
- `ColumnFamily` - Logical grouping of columns with versioning support
- `Filter` and `FilterSet` - For filtering data
- `AggregationType` and `AggregationSet` - For aggregating data

## Examples

The [main.rs](../src/main.rs) file contains a comprehensive example that demonstrates all the features of RedBase.

Additional examples can be found in the [usage_guide.txt](usage_guide.txt) file.

## Contributing

If you'd like to contribute to RedBase, please read the [CONTRIBUTING.md](../CONTRIBUTING.md) file for guidelines on how to get started.