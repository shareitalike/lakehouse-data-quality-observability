# Databricks Community Edition (CE) Compatible Project Extensions

Since Databricks Community Edition restricts API access (which prevents external orchestration like Airflow, dbt Cloud integrations, or CI/CD deployments directly against the cluster), we must focus our "Senior-Level" enhancements entirely on **advanced Spark logic, pipeline architecture, and Delta Lake mechanics**.

These enhancements are 100% possible within the CE environment and provide massive value for resume points and interview discussions.

## 1. Advanced Incremental Processing: Change Data Feed (CDF)
Instead of overwriting the Gold layer or doing heavy merges based on full table scans from Silver, enable Change Data Feed.
*   **What it does:** Delta Lake records row-level changes (inserts, updates, deletes) in a separate hidden log. You can query just the *changes* from Silver and apply them to Gold.
*   **CE Implementation:** 
    *   Enable: `ALTER TABLE silver_table SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`
    *   Read changes: `spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", X)...`
*   **Interview Value:** "I reduced our Gold layer processing time by transitioning from daily full-table merges to incremental CDC (Change Data Capture) using Delta CDF."

## 2. Slowly Changing Dimensions (SCD Type 2)
Implement SCD Type 2 tracking in your Gold dimensional tables.
*   **What it does:** Instead of updating a record in place, you close out the old record (setting `is_current = False`, `end_date = today`) and insert the new record. 
*   **CE Implementation:** Write a complex `MERGE` statement in PySpark that matches on primary keys but inserts new rows when attributes change.
*   **Interview Value:** SCD Type 2 is a classic, mandatory concept in Data Warehousing. Being able to code it natively in PySpark + Delta `MERGE` proves you understand dimensional modeling.

## 3. Emulate Structured Streaming (Micro-Batching)
You don't need Kafka. You can use Spark Structured Streaming directly on local folders.
*   **What it does:** Replaces batch ingestion (reading a static JSON/CSV) with a continuous streaming stream.
*   **CE Implementation:** 
    *   Have a Python script continuously drop new JSON files into a `/FileStore/bronze_drop/` directory.
    *   Use `spark.readStream.schema(...).json(...)` to read them as they arrive.
    *   Use `.writeStream.trigger(availableNow=True)` to create an automated micro-batch architecture.
*   **Interview Value:** Demonstrates knowledge of streaming concepts: checkpointing, schemas in streams, micro-batching triggers, and state management.

## 4. "Chaos Engineering": Intentional Skew & Performance Tuning
Interviews heavily index on your ability to fix slow Spark jobs. You can simulate this.
*   **What it does:** Generate purposely skewed data (e.g., 90% of transactions come from 'store_id_1'). 
*   **CE Implementation:** 
    *   Write a query that joins this skewed data. It will lag or crash.
    *   Implement **Salting** (adding random numbers to the join key) or rely on **Adaptive Query Execution (AQE) Skew Join optimization** to fix it.
    *   Log exactly how much faster it ran after tuning.
*   **Interview Value:** "I identified a data skew issue causing a massive spill to disk during a shuffle. I implemented key salting which reduced job runtime by 60%."

## 5. Schema Evolution and Drift Handling
Show how your pipeline reacts when the upstream source suddenly adds or removes columns.
*   **What it does:** Prevents pipeline failure when APIs change over time.
*   **CE Implementation:** 
    *   Generate a new batch of data with a completely new column.
    *   Show how `mergeSchema` handles the addition gracefully.
    *   Alternatively, write strict Python validation that rejects unexpected columns using `StructType` comparisons, routing the bad files to a dead-letter queue.
*   **Interview Value:** Schema drift is one of the most common causes of production data incidents. Having a concrete strategy for it is very impressive.

## Summary for the Resume
Even without Airflow or AWS, implementing just **CDF** and **SCD Type 2** allows you to put the following bullet points on your resume:

> * "Architected an end-to-end Medallion data lakehouse using PySpark and Delta Lake, implementing Change Data Feed (CDF) for optimized incremental data propagation."
> * "Designed robust dimensional models implementing SCD Type 2 logic via complex Delta Lake `MERGE` operations, enabling point-in-time historical reporting."
> * "Optimized distributed join strategies by resolving data skew through key salting and leveraging Adaptive Query Execution (AQE)."
