# System Architecture: Lakehouse Data Quality & Observability

## Overview

This framework implements a robust, multi-layered data validation and observability system for Medallion architecture pipelines. It ensures data integrity from raw ingestion (Bronze) through trusted transformation (Silver) to business-ready aggregation (Gold).

## Core Design Patterns

### 1. The Quarantine Pattern
Instead of failing pipelines on data quality violations, the framework utilizes a non-blocking quarantine pattern:
- **Rule Evaluation**: Records are evaluated against configured DQ rules.
- **Routing**: Valid records continue to the next Medallion layer, while invalid records are routed to a dedicated quarantine table.
- **Traceability**: Quarantined records preserve the original data along with metadata describing which rules failed and why.

### 2. Configuration-Driven Validation
All validation rules are defined as serializable Python dataclasses. This enables:
- **Portability**: Rules can be easily ported across environments.
- **Extensibility**: New rule types can be added by implementing a configuration and its corresponding execution logic.
- **Centralization**: All data quality expectations (contracts) are defined in a central location.

### 3. dbt-Inspired SQL Observability
While the core engine uses the Spark DataFrame API for performance, the observability layer supports declarative SQL-based checks. This allows for:
- **Accessibility**: Analysts can define custom checks using standard SQL.
- **Pushdown Optimization**: Spark optimizes these queries to run directly on the storage layer.

## Medallion Layer Responsibilities

| Layer | Responsibility | Primary DQ Focus |
|---|---|---|
| **Bronze** | Raw ingestion & landing | Schema enforcement, critical null checks, capture raw fidelity. |
| **Silver** | Cleaned & normalized data | Deduplication, referential integrity, business logic validation. |
| **Gold** | Business-ready aggregates | Row count anomalies, distribution drift, SLA monitoring. |

## Observability & Monitoring

The framework maintains an append-only metrics store in Delta format, tracking:
- **Rule Results**: Pass/fail counts and rates per rule.
- **Pipeline Health**: Execution time, row counts, and layer-level status.
- **Data Distribution**: Statistical snapshots (mean, stddev, percentiles) to detect silent drift.
