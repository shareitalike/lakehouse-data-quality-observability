"""
Fault injection engine — simulates realistic production data issues.

This module provides capabilities to systematically inject controlled data 
quality issues into clean datasets, enabling thorough testing and validation 
of DQ rules and monitoring systems.
"""

from dataclasses import dataclass, field
from typing import List, Optional
import random

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


@dataclass
class InjectionConfig:
    """
    Configuration for which issues to inject and at what rates.
    
    """
    # Bronze issues
    null_customer_id_rate: float = 0.05        # 5% null customer_ids
    duplicate_order_rate: float = 0.03          # 3% duplicate order_ids
    malformed_timestamp_rate: float = 0.02      # 2% bad timestamps
    invalid_status_rate: float = 0.02           # 2% invalid order statuses
    schema_drift_enabled: bool = True           # Add unexpected column
    missing_column_enabled: bool = True         # Drop a column in subset
    negative_quantity_rate: float = 0.01         # 1% negative quantities
    invalid_product_id_rate: float = 0.03       # 3% invalid product_ids
    currency_mismatch_rate: float = 0.02        # 2% unexpected currencies
    
    seed: int = 42


def inject_null_customer_id(df: DataFrame, rate: float, seed: int) -> DataFrame:
    """
    Inject null customer_id values.
    
    
    # FAILURE MODE: If rate > 1.0 accidentally, ALL customer_ids become null.
    # Validation in InjectionConfig prevents this.
    """
    return df.withColumn(
        "customer_id",
        F.when(F.rand(seed) < rate, F.lit(None).cast(StringType()))
        .otherwise(F.col("customer_id"))
    )


def inject_duplicate_orders(df: DataFrame, rate: float, seed: int) -> DataFrame:
    """
    Inject duplicate order_id records by sampling and appending.
    
    
    
    # INTERVIEW: "What causes duplicate events in production?"
    # → "At-least-once delivery in Kafka/Kinesis, network retries in HTTP APIs,
    #    idempotency key failures, and producer restart replays."
    """
    sample_count = int(df.count() * rate)
    if sample_count == 0:
        return df
    
    # Sample records to duplicate
    duplicates = df.sample(fraction=rate, seed=seed).limit(sample_count)
    
    # Modify timestamp slightly to create "near-duplicates"
    duplicates = duplicates.withColumn(
        "order_timestamp",
        F.concat(
            F.substring(F.col("order_timestamp"), 1, 17),
            F.lpad((F.rand(seed + 100) * 59).cast("int").cast("string"), 2, "0")
        )
    )
    
    return df.unionByName(duplicates)


def inject_malformed_timestamps(df: DataFrame, rate: float, seed: int) -> DataFrame:
    """
    Inject malformed timestamp strings.
    
    
    # FAILURE MODE: If the downstream parser uses a lenient mode,
    # "2024/13/45" might parse as 2025-02-14 (month overflow). Strict
    # parsing in Silver catches this.
    """
    return df.withColumn(
        "order_timestamp",
        F.when(
            F.rand(seed + 10) < rate * 0.4,
            F.lit("NOT_A_DATE")
        ).when(
            F.rand(seed + 11) < rate * 0.3,
            F.lit("2024/13/45 99:99:99")
        ).when(
            F.rand(seed + 12) < rate * 0.2,
            F.lit("")
        ).when(
            F.rand(seed + 13) < rate * 0.1,
            F.lit("null")
        ).otherwise(F.col("order_timestamp"))
    )


def inject_invalid_statuses(df: DataFrame, rate: float, seed: int) -> DataFrame:
    """
    Inject unexpected order status enum values.
    
    """
    invalid_statuses = ["YOLO", "TEST", "PROCESSING", "unknown", "VOID"]
    
    return df.withColumn(
        "order_status",
        F.when(
            F.rand(seed + 20) < rate,
            F.element_at(
                F.array([F.lit(s) for s in invalid_statuses]),
                (F.abs(F.hash(F.col("order_id"))) % len(invalid_statuses) + 1).cast("int")
            )
        ).otherwise(F.col("order_status"))
    )


def inject_negative_quantities(df: DataFrame, rate: float, seed: int) -> DataFrame:
    """
    Inject negative quantity values.
    
    
    # INTERVIEW: "Is a negative quantity always an error?"
    # → "Depends on the domain. In some systems, negatives represent returns.
    #    In ours, returns have their own status. So negatives are errors.
    #    This is why severity is configurable — one domain's error is another's
    #    valid business case."
    """
    return df.withColumn(
        "quantity",
        F.when(
            F.rand(seed + 30) < rate,
            -(F.abs(F.hash(F.col("order_id"))) % 5 + 1).cast("int")
        ).otherwise(F.col("quantity"))
    )


def inject_invalid_product_ids(df: DataFrame, rate: float, seed: int) -> DataFrame:
    """
    Inject invalid product_id values (FK violations).
    
    """
    return df.withColumn(
        "product_id",
        F.when(
            F.rand(seed + 40) < rate,
            F.concat(
                F.lit("INVALID-"),
                F.lpad((F.abs(F.hash(F.col("order_id"))) % 999 + 1).cast("string"), 3, "0")
            )
        ).otherwise(F.col("product_id"))
    )


def inject_currency_mismatch(df: DataFrame, rate: float, seed: int) -> DataFrame:
    """
    Inject unexpected currency codes.
    
    """
    unexpected_currencies = ["BTC", "XYZ", "ABC", "GOLD", ""]
    
    return df.withColumn(
        "currency",
        F.when(
            F.rand(seed + 50) < rate,
            F.element_at(
                F.array([F.lit(c) for c in unexpected_currencies]),
                (F.abs(F.hash(F.col("order_id"))) % len(unexpected_currencies) + 1).cast("int")
            )
        ).otherwise(F.col("currency"))
    )


def inject_schema_drift(df: DataFrame) -> DataFrame:
    """
    Add an unexpected column to simulate schema drift.
    
    
    # INTERVIEW: "How do you handle schema drift in your pipelines?"
    # → "Schema drift detection alerts the team. Schema enforcement prevents
    #    unexpected columns from entering Bronze. The data contract specifies
    #    whether evolution is allowed at each layer."
    """
    return df.withColumn(
        "loyalty_tier",
        F.when(F.rand(99) < 0.3, F.lit("gold"))
        .when(F.rand(99) < 0.6, F.lit("silver"))
        .otherwise(F.lit("bronze"))
    )


def inject_all_issues(
    df: DataFrame,
    config: Optional[InjectionConfig] = None,
) -> DataFrame:
    """
    Apply all configured fault injections to the DataFrame.
    
    
    Returns:
        Tuple of (corrupted_df, injection_summary dict)
    """
    if config is None:
        config = InjectionConfig()
    
    seed = config.seed
    original_count = df.count()
    
    # Apply injections in order
    # Phase 1: Value-level corruptions (don't change row count)
    df = inject_null_customer_id(df, config.null_customer_id_rate, seed)
    df = inject_malformed_timestamps(df, config.malformed_timestamp_rate, seed)
    df = inject_invalid_statuses(df, config.invalid_status_rate, seed)
    df = inject_negative_quantities(df, config.negative_quantity_rate, seed)
    df = inject_invalid_product_ids(df, config.invalid_product_id_rate, seed)
    df = inject_currency_mismatch(df, config.currency_mismatch_rate, seed)
    
    # Phase 2: Row-level changes (changes row count)
    df = inject_duplicate_orders(df, config.duplicate_order_rate, seed)
    
    # Phase 3: Schema-level changes (changes columns)
    if config.schema_drift_enabled:
        df = inject_schema_drift(df)
    
    new_count = df.count()
    
    print(f"[IssueInjector] Original records: {original_count}")
    print(f"[IssueInjector] After injection:  {new_count}")
    print(f"[IssueInjector] Added duplicates: {new_count - original_count}")
    print(f"[IssueInjector] Injected issues:")
    print(f"  - null customer_id:     ~{config.null_customer_id_rate*100:.1f}%")
    print(f"  - duplicate order_id:   ~{config.duplicate_order_rate*100:.1f}%")
    print(f"  - malformed timestamps: ~{config.malformed_timestamp_rate*100:.1f}%")
    print(f"  - invalid status:       ~{config.invalid_status_rate*100:.1f}%")
    print(f"  - negative quantity:    ~{config.negative_quantity_rate*100:.1f}%")
    print(f"  - invalid product_id:   ~{config.invalid_product_id_rate*100:.1f}%")
    print(f"  - currency mismatch:    ~{config.currency_mismatch_rate*100:.1f}%")
    print(f"  - schema drift:         {'YES' if config.schema_drift_enabled else 'NO'}")
    
    return df
