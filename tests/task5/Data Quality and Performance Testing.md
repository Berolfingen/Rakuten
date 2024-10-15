# Data Quality Checks for ETL

## 1. Pre-Migration Analysis

### Standard:
- **Data Profiling**: Use tools like Talend or Informatica to perform standard data profiling, checking for missing values, data types, unique constraints, and identifying potential duplicates.
- **Dependency Mapping**: Identify primary key/foreign key dependencies to ensure consistent relationships throughout migration.

### Advanced:
- **Column-Level Profiling with Detailed Metrics**: Use Apache Griffin or Talend to drill down into metrics like min, max, average, and distribution for every column. Analyze outliers, cardinality, and correlation with related columns to catch any abnormality pre-migration.
- **Schema Drift Analysis**: Assess source schema changes over time, and implement automated checks to flag potential schema drifts before migration begins. This helps in adjusting transformation rules on the fly.

## 2. Extraction Validations

### Standard:
- **Row Count Verification**: Compare row counts between source and target systems to ensure completeness.
- **Data Type Verification**: Validate extracted data against the expected schema to catch any data type discrepancies.

### Advanced:
- **Hash-Based Data Integrity Check**: Compute a hash (e.g., MD5, SHA256) for each row during extraction and compare it post-load to ensure no data was corrupted during extraction or transit.
- **Incremental Data Freshness**: Validate that only new or changed records are extracted in incremental extraction processes. Use timestamp fields or CDC (Change Data Capture) techniques to ensure data freshness.

## 3. Transformation Validation

### Standard:
- **Transformation Rule Testing**: Verify basic transformation rules like column mapping, data format changes, and derived calculations using sample datasets.
- **Lookup and Join Validation**: Ensure lookup tables and join operations return the expected results, especially for key relationships.

### Advanced:
- **Rule-Based Transformation Testing**: Automate the validation of transformation logic with detailed test cases for every transformation step—splits, merges, and derived calculations. Use tools like dbt (Data Build Tool) or custom SQL scripts for precision.
- **Complex Aggregation and Business Logic Validation**: Pre-calculate aggregate metrics (sums, averages, etc.) using independent means and compare with ETL output. Test corner cases like empty groups, NULL values, and aggregation across multiple levels.

## 4. Data Load Quality Assurance

### Standard:
- **Constraint Verification**: Post-load, validate primary keys, foreign keys, and unique constraints to ensure relational integrity is upheld.
- **Duplicate Data Check**: Verify target data for duplicate records to ensure uniqueness is maintained after loading.

### Advanced:
- **Automated Data Consistency Framework**: Develop custom Python scripts or use data quality frameworks like Great Expectations to automate data consistency checks. Validate relationships between entities, especially where business rules are complex.
- **Load Distribution Verification**: For partitioned tables, verify that data is loaded evenly across partitions to ensure performance consistency during querying.

## 5. End-to-End Automation for Data Quality

### Standard:
- **Manual Spot Checks**: Perform sample-based manual verification, comparing data across source and target to detect discrepancies.

### Advanced:
