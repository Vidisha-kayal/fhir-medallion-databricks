"""scd2.py: generic SCD Type 2 merge for Silver tables (one function serves all resources)."""
from pyspark.sql import Window
from pyspark.sql import functions as F

from src import config_loader as cl


def _version_int():
    """FHIR version id as a number (tie-breaker when two versions share a timestamp)."""
    return F.expr("try_cast(meta_version_id AS int)")


def target_shape(df, key):
    """Final Silver column order: key + SCD columns first, audit columns last."""
    first = [key, "effective_from", "effective_to", "is_current", "version_number"]
    last = ["silver_processed_at", "silver_updated_at"]
    middle = [c for c in df.columns if c not in first + last and not c.startswith("_")]
    return df.select(*first, *middle, *last)


def _materialize(spark, df):
    """Pull a small DataFrame into memory and rebuild it as a fixed local DataFrame."""
    return spark.createDataFrame(df.collect(), df.schema)


def scd2_merge(spark, cfg, resource, incoming):
    """Merge an incoming (flattened, deduplicated) Silver DataFrame into the SCD2 table.
    Returns a dict of counters describing what happened."""
    key = cl.get_resource(cfg, resource)["silver_key"]
    table = cl.resource_table(cfg, "silver", resource)
    now = cl.utc_now()
    vint = _version_int()

    # Fix the incoming rows in memory so every later step sees identical data
    inc = _materialize(spark, incoming.withColumn(
        "effective_from", F.coalesce("meta_last_updated", "extraction_timestamp")))
    n_incoming = inc.count()

    # Create the empty Silver table on first run
    template = (inc.withColumn("effective_to", F.lit(None).cast("timestamp"))
                   .withColumn("is_current", F.lit(True))
                   .withColumn("version_number", F.lit(1).cast("int"))
                   .withColumn("silver_updated_at", F.lit(now)))
    shaped = target_shape(template, key)
    shaped.limit(0).write.format("delta").mode("ignore").saveAsTable(table)
    cols = shaped.columns
    target = spark.table(table)

    # 1) Skip versions that are already stored (idempotent reruns)
    seen = target.select(F.col(key).alias("_k"), F.col("meta_version_id").alias("_v"))
    fresh = inc.join(
        seen, (inc[key] == seen["_k"]) & inc["meta_version_id"].eqNullSafe(seen["_v"]), "left_anti")

    # 2) Attach the current stored version of each id, if any
    cur = (target.where("is_current = true")
                 .select(F.col(key).alias("_k"),
                         F.col("row_hash").alias("_cur_hash"),
                         F.col("effective_from").alias("_cur_from"),
                         F.col("version_number").alias("_cur_ver")))
    joined = _materialize(spark, fresh.join(cur, fresh[key] == cur["_k"], "left").drop("_k"))
    n_fresh = joined.count()

    # 3) Late-arriving versions (not newer than the stored current one): skip and report
    ok = joined.where(F.col("_cur_from").isNull() | (F.col("effective_from") > F.col("_cur_from")))
    n_late = n_fresh - ok.count()

    # 4) Keep only real changes: compare each version with the one before it
    w = Window.partitionBy(key).orderBy("effective_from", vint)
    prev_hash = F.coalesce(F.lag("row_hash").over(w), F.col("_cur_hash"))
    changed = (ok.withColumn("_prev_hash", prev_hash)
                 .where(F.col("_prev_hash").isNull() | (F.col("row_hash") != F.col("_prev_hash"))))
    n_unchanged = ok.count() - changed.count()

    # 5) New versions: end dates, version numbers, current flag
    new_rows = (changed
        .withColumn("effective_to", F.lead("effective_from").over(w))
        .withColumn("version_number",
                    (F.coalesce(F.col("_cur_ver"), F.lit(0)) + F.row_number().over(w)).cast("int"))
        .withColumn("is_current", F.col("effective_to").isNull())
        .withColumn("silver_updated_at", F.lit(now)))

    inserts = (target_shape(new_rows, key)
               .withColumn("_merge_key", F.lit(None).cast("string"))
               .withColumn("_action", F.lit("INSERT")))
    closers = (new_rows.where(F.col("_cur_from").isNotNull())
               .groupBy(key).agg(F.min("effective_from").alias("_close_at"))
               .select(F.col(key).alias("_merge_key"), "_close_at")
               .withColumn("_action", F.lit("CLOSE")))
    staged = _materialize(spark, inserts.unionByName(closers, allowMissingColumns=True))

    n_inserted = staged.where("_action = 'INSERT'").count()
    n_closed = staged.where("_action = 'CLOSE'").count()

    # 6) One atomic MERGE: close the old current rows, insert the new versions
    if n_inserted > 0:
        staged.createOrReplaceTempView("_scd2_staged")
        insert_cols = ", ".join(f"`{c}`" for c in cols)
        insert_vals = ", ".join(f"s.`{c}`" for c in cols)
        spark.sql(f"""
            MERGE INTO {table} AS t
            USING _scd2_staged AS s
            ON t.`{key}` = s.`_merge_key` AND t.is_current = true AND s._action = 'CLOSE'
            WHEN MATCHED THEN UPDATE SET
                is_current = false,
                effective_to = s._close_at,
                silver_updated_at = current_timestamp()
            WHEN NOT MATCHED AND s._action = 'INSERT' THEN
                INSERT ({insert_cols}) VALUES ({insert_vals})
        """)

    return {"resource": resource, "table": table,
            "incoming_versions": n_incoming,
            "already_in_silver": n_incoming - n_fresh,
            "late_arriving_skipped": n_late,
            "unchanged_collapsed": n_unchanged,
            "new_versions_inserted": n_inserted,
            "previous_versions_closed": n_closed}