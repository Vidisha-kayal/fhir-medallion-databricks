"""bronze.py: raw FHIR JSON pages -> Bronze Delta table (one row per resource)."""
import json
import os

import pandas as pd
from pyspark.sql import functions as F

from src import config_loader as cl

# Only the envelope is typed. 'resource' as string keeps the resource JSON untouched.
ENTRY_SCHEMA = "struct<entry:array<struct<fullUrl:string,resource:string>>>"


def read_manifest(spark, raw_dir):
    """Load the per-folder manifest written during ingestion (tiny file)."""
    path = os.path.join(raw_dir, "_manifest.jsonl")
    if not os.path.exists(path):
        return None
    with open(path, "r") as f:
        rows = [json.loads(line) for line in f if line.strip()]
    if not rows:
        return None
    return spark.createDataFrame(pd.DataFrame(rows)).select(
        "file_name",
        "batch_id",
        "page_number",
        F.col("api_url").alias("api_url_or_params"),
        F.col("extracted_at").cast("timestamp").alias("extraction_timestamp"),
        F.col("saved_at").cast("timestamp").alias("raw_saved_at"),
        "entry_count",
    )


def expected_record_count(manifest_df):
    return manifest_df.agg(F.sum("entry_count")).collect()[0][0] or 0


def build_bronze_df(spark, cfg, resource_name, load_date, fmt="json"):
    """Return (bronze_df, manifest_df) or (None, None) if no raw data exists."""
    if fmt != "json":
        raise NotImplementedError("XML bronze is part of the optional extension (later).")

    raw_dir = cl.raw_dir(cfg, resource_name, load_date, fmt)
    manifest = read_manifest(spark, raw_dir)
    if manifest is None:
        return None, None

    pages = (
        spark.read.text(f"{raw_dir}/page_*.json", wholetext=True)
        .select(
            F.col("value").alias("bundle_json"),
            F.col("_metadata.file_name").alias("file_name"),
            F.col("_metadata.file_path").alias("source_file"),
        )
    )

    exploded = (
        pages.withColumn("b", F.from_json("bundle_json", ENTRY_SCHEMA))
        .select("file_name", "source_file", F.explode_outer("b.entry").alias("e"))
        .where(F.col("e").isNotNull())
        .select("file_name", "source_file", F.col("e.resource").alias("resource_json"))
    )

    df = (
        exploded.join(manifest.drop("entry_count"), "file_name", "left")
        .select(
            F.get_json_object("resource_json", "$.id").alias("resource_id"),
            F.get_json_object("resource_json", "$.resourceType").alias("resource_type"),
            F.get_json_object("resource_json", "$.meta.versionId").alias("meta_version_id"),
            F.get_json_object("resource_json", "$.meta.lastUpdated")
             .cast("timestamp").alias("meta_last_updated"),
            "resource_json",
            "extraction_timestamp",
            "api_url_or_params",
            "raw_saved_at",
            F.current_timestamp().alias("bronze_loaded_at"),
            "source_file",
            "page_number",
            "batch_id",
            F.lit(load_date).alias("load_date"),
            F.lit(fmt).alias("source_format"),
        )
        .where(F.col("resource_type") == resource_name)   # drop OperationOutcome entries, if any
    )
    return df, manifest


def write_bronze(spark, df, table, load_date):
    """Create the table on first run; afterwards replace only this load_date."""
    if spark.catalog.tableExists(table):
        (df.write.format("delta").mode("overwrite")
           .option("replaceWhere", f"load_date = '{load_date}'")
           .saveAsTable(table))
    else:
        df.write.format("delta").saveAsTable(table)


def bronze_count(spark, table, load_date):
    return spark.table(table).where(F.col("load_date") == load_date).count()