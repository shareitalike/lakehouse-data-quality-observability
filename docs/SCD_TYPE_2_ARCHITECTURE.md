# SCD Type 2 Architecture Implementation Details

This document outlines the exact technical changes that will be introduced to our PySpark pipeline to support Slowly Changing Dimensions (SCD) Type 2 for the `silver_customers` table.

## 1. Modifications to `pipelines/silver_pipeline.py`

### Adding the Delta Merge Functionality
We will add a new dedicated method `merge_scd2_customers` to the `SilverPipeline` class. This method contains the advanced PySpark logic that production enterprise systems require.

**The process inside `merge_scd2_customers`:**
1. **Initial Load Support:** The script will first check if the `silver_customers` Delta table exists. If it doesn't, it provisions it automatically with the three vital SCD columns:
   * `is_current` = `True`
   * `valid_from` = `current_timestamp()`
   * `valid_to` = `null`
2. **Identifying Changes:** We will read the *active* records (`is_current = True`) from the existing Delta table and join them with the *incoming* customer data. We will flag a record as an "Update" if their `preferred_currency` has changed.
3. **The Staging Dataframe Construction (The "Trick"):** Delta `MERGE` handles inserts and updates cleanly, but for SCD Type 2, a single source change requires *two* table operations:
   * **Update** the existing record (`is_current` -> `False`, `valid_to` -> `now`)
   * **Insert** the new record (`is_current` -> `True`, `valid_from` -> `now`)
   * To achieve this in a single atomic Delta operation, we use PySpark to create a "staging dataframe" that outputs two rows for every updated customer using a union or explode function.
4. **The Atomic Delta Merge:** We execute the native `DeltaTable.forPath().alias("target").merge(staging.alias("source"), "target.customer_id = source.merge_key")` method.

### Updating `write_silver_tables`
Currently, the pipeline executes:
```python
customers_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(customers_path)
```
This will be removed and replaced with a call to:
```python
self.merge_scd2_customers(customers_df, customers_path)
```

## 2. Testing & Data Generation Adjustments

To validate this functionality, we generate changes!
We will either:
1. Provide a small custom Python notebook/script (`generate_currency_change.py`) that forcefully updates a single customer's currency in the source Bronze data.
2. OR instruct the user during the Walkthrough phase on how to quickly modify the Silver input manually to trigger the SCD 2 update flag in real-time.

## 3. Gold Layer Adjustments (Optional but Recommended)
Because the `silver_customers` table now has multiple rows per customer (history), any downstream Gold aggregations that join on `silver_customers` will need a minor tweak:
```python
# Downstream Gold logic must now filter for active records
df.join(customers_df.filter(F.col("is_current") == True), on="customer_id")
```
We will append this filter to the appropriate logic in `gold_pipeline.py` to ensure revenue and LTV logic isn't inadvertently double-counting closed customer records.

## Summary of Core Capabilities:
* **Complex Data Modeling:** Understanding how and why SCD Type 1 vs Type 2 is used.
* **Advanced PySpark:** Executing a multi-row transform required for Delta `MERGE`.
* **Atomic Transactions:** Leveraging Delta Lake to ensure historical records are closed and opened safely without leaving the table in a corrupted state if the pipeline fails midway.
