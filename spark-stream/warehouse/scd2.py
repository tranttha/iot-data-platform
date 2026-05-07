"""
Common SCD Type 2 merge helper.

Two-step ACID-safe pattern on Iceberg:
  1. MERGE  — close rows whose tracked attributes changed
             (set effective_end = now, is_current = false).
  2. INSERT — open a new version for:
               (a) brand-new natural keys not yet in the table, and
               (b) keys whose active row was just closed in step 1.

Usage
-----
    from warehouse.scd2 import merge as scd2_merge

    scd2_merge(
        spark       = spark,
        stage_df    = latest_df,           # one row per natural key, already has surrogate_key + effective_start
        table       = "lakehouse.warehouse.dim_foo",
        join_cond   = "t.foo_id = s.foo_id",
        is_current  = "is_current",
        change_cond = "t.col_a != s.col_a OR t.col_b != s.col_b",
        effective_start = "effective_start",
        effective_end   = "effective_end",
        insert_cols = "sk, foo_id, col_a, col_b, effective_start, effective_end, is_current",
        select_expr = "s.sk, s.foo_id, s.col_a, s.col_b, s.effective_start, NULL, true",
        no_current_filter = "t.foo_id = s.foo_id AND t.is_current = true",
        view_name   = "_stage_foo",        # must be unique across concurrent dims
    )
"""

from pyspark.sql import DataFrame, SparkSession


def merge(
    *,
    spark: SparkSession,
    stage_df: DataFrame,
    table: str,
    join_cond: str,
    is_current: str,
    change_cond: str,
    effective_start: str,
    effective_end: str,
    insert_cols: str,
    select_expr: str,
    no_current_filter: str,
    view_name: str,
) -> None:
    """
    Execute a two-step SCD Type 2 merge against an Iceberg table.

    Parameters
    ----------
    spark             : active SparkSession
    stage_df          : DataFrame with one row per natural key (already enriched with
                        surrogate_key and effective_start columns)
    table             : fully-qualified Iceberg table name
    join_cond         : SQL ON clause matching target (t) to source (s) active row
    is_current        : name of the boolean is-current column in the table
    change_cond       : SQL expression that is TRUE when any tracked attribute differs
    effective_start   : name of the effective-start timestamp column
    effective_end     : name of the effective-end timestamp column
    insert_cols       : comma-separated column list for the INSERT statement
    select_expr       : comma-separated SELECT expressions aligned with insert_cols
                        (use NULL for effective_end, true for is_current)
    no_current_filter : SQL EXISTS sub-filter that identifies rows already active;
                        used to guard the INSERT against double-inserting
    view_name         : global temp view name (unique per dim to prevent collisions)
    """
    stage_df.createOrReplaceGlobalTempView(view_name)
    src = f"global_temp.{view_name}"

    # ── Step 1: close rows whose tracked attributes changed ──────────────────
    spark.sql(f"""
        MERGE INTO {table} t
        USING {src} s
        ON  {join_cond}
        AND t.{is_current} = true
        WHEN MATCHED AND ({change_cond})
        THEN UPDATE SET
            t.{effective_end}  = s.{effective_start},
            t.{is_current}     = false
    """)

    # ── Step 2: open a new version for new + just-closed keys ───────────────
    spark.sql(f"""
        INSERT INTO {table} ({insert_cols})
        SELECT {select_expr}
        FROM {src} s
        WHERE NOT EXISTS (
            SELECT 1 FROM {table} t
            WHERE {no_current_filter}
        )
    """)
