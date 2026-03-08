# Schema Validation vs. Schema Enforcement in Lakehouse Architectures

This document summarizes the core concepts, differences, and control strategies for Schema Validation and Schema Enforcement, customized to the patterns used in our Lakehouse Data Quality and Observability Framework.

---

## 1. Core Concepts & Comparison

In modern Lakehouse environments (like Delta Lake or Apache Iceberg), managing how table schemas change over time is critical. While both concepts address data structure consistency, they operate with different behaviors and goals:

| Feature | Schema Validation | Schema Enforcement |
| :--- | :--- | :--- |
| **Primary Goal** | Monitor, audit, and report structural compliance (observability). | Prevent data corruption in downstream tables (data integrity). |
| **Action on Mismatch** | Non-blocking. Generates metrics, logs metadata, and alerts. | Blocking. Raises database exceptions, failing write operations. |
| **Where it occurs** | Application layer (e.g., rules engine, pre-write check). | Storage engine/write layer (e.g., Delta Lake). |
| **Use Cases** | Bronze/Raw ingestion checks, reporting schema drift. | Silver/Gold structural consistency, strict schema compliance. |

---

## 2. Schema Validation (Observability & Drift Detection)

Schema Validation compares incoming raw datasets against a target baseline schema but does not interrupt the pipeline execution. Instead of crashing, validation isolates metadata about the changes.

In our framework:
* The raw schemas are defined with flexible constraints to permit dirty data ingestion (see [bronze_schemas.py](file:///f:/pyspark_study/Project%20Lakehouse%20Data%20Quality%20and%20Observability%20Framework/Pro%20Lakehouse%20Data%20Quality%20and%20Observability%20Framework/schemas/bronze_schemas.py)).
* The [schema_drift_detection](file:///f:/pyspark_study/Project%20Lakehouse%20Data%20Quality%20and%20Observability%20Framework/Pro%20Lakehouse%20Data%20Quality%20and%20Observability%20Framework/rules/schema_drift_detection.py) rule is used to compare a DataFrame's actual structure against the contract schema, identifying:
  * **Missing Columns**
  * **Unexpected Columns**
  * **Data Type Mismatches**
* A [ValidationResult](file:///f:/pyspark_study/Project%20Lakehouse%20Data%20Quality%20and%20Observability%20Framework/Pro%20Lakehouse%20Data%20Quality%20and%20Observability%20Framework/rules/validation_result.py) containing this diagnostic data is logged to the observability tables.

---

## 3. Schema Enforcement & Controlling Schema Evolution

Schema Enforcement actively blocks write actions that do not conform to the destination table's schema. However, when we enable schema evolution using `.option("mergeSchema", "true")`, the storage engine allows the target schema to expand.

To prevent **uncontrolled schema evolution** (schema pollution due to misspellings, temp fields, or corrupt schema columns) when `mergeSchema` is enabled, the following defensive strategies should be followed:

### Strategy 1: Conditional Evolution
Never enable auto-merge globally (e.g., setting `spark.databricks.delta.schema.autoMerge.enabled = true`). Instead, apply the option conditionally on writes by referencing the [DataContract](file:///f:/pyspark_study/Project%20Lakehouse%20Data%20Quality%20and%20Observability%20Framework/Pro%20Lakehouse%20Data%20Quality%20and%20Observability%20Framework/config/layer_schemas.py#L27) config:

```python
writer = df.write.format("delta").mode("append")

if contract.allow_schema_evolution:
    writer = writer.option("mergeSchema", "true")

writer.save(target_path)
```

### Strategy 2: Pre-Write Contract Validation
Integrate a programmatic check before triggering write commands. If a column is added but `allow_schema_evolution` is set to `False` for that table contract, block the write at the application layer.

In our framework, the [ContractEnforcer](file:///f:/pyspark_study/Project%20Lakehouse%20Data%20Quality%20and%20Observability%20Framework/Pro%20Lakehouse%20Data%20Quality%20and%20Observability%20Framework/engine/contract_enforcer.py) does exactly this:

```python
enforcer = ContractEnforcer(spark)
report = enforcer.check_schema_contract(df, contract)

if not report["passed"]:
    if report["extra_columns"] and not contract.allow_schema_evolution:
        raise ValueError(f"Write blocked: Unapproved columns detected: {report['extra_columns']}")
```

### Strategy 3: Column Whitelisting (Sanitization)
Project only the approved columns from the contract before appending data to the table:

```python
# Extract the expected schema field names from the contract
approved_cols = [field.name for field in contract.schema.fields]

# Select only allowed columns, dropping any unexpected ones
df_sanitized = df.select(*[col for col in df.columns if col in approved_cols])
```

### Strategy 4: Dead-Letter Queue (DLQ)
Quarantine records that deviate from the expected schema:
1. Separate conforming records from non-conforming records.
2. Append conforming records to the target table.
3. Write non-conforming batches to a dedicated DLQ/Quarantine Delta table for engineering analysis, logging the schema differences to the metadata columns.

### Strategy 5: Observability Alerts
Whenever schema drift is successfully processed and merged, emit a slack, email, or webhook notification identifying:
* The schema drift event details (which columns were added/removed/changed).
* The dataset name and execution time.
