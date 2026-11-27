"""meta.py: run-log and watermark tables + logging helpers."""
import uuid
from datetime import datetime

from pyspark.sql import types as T

from src import config_loader as cl

S, TS = T.StringType(), T.TimestampType()

RUN_LOG_SCHEMA = T.StructType([
    T.StructField("log_id", S, False),
    T.StructField("run_id", S),
    T.StructField("batch_id", S),
    T.StructField("pipeline_name", S),
    T.StructField("stage", S),                  # RAW | BRONZE | SILVER | GOLD
    T.StructField("resource_type", S),
    T.StructField("load_date", T.DateType()),
    T.StructField("source_format", S),
    T.StructField("window_since", TS),
    T.StructField("window_until", TS),
    T.StructField("started_at", TS),            # RAW: API call start
    T.StructField("ended_at", TS),              # RAW: API call end
    T.StructField("data_saved_at", TS),
    T.StructField("pages_fetched", T.IntegerType()),
    T.StructField("records_read", T.LongType()),
    T.StructField("records_written", T.LongType()),
    T.StructField("truncated", T.BooleanType()),
    T.StructField("status", S),                 # SUCCESS | FAILED | NO_RAW_DATA
    T.StructField("error_message", S),
    T.StructField("logged_at", TS),
])

WATERMARK_SCHEMA = T.StructType([
    T.StructField("resource_type", S, False),
    T.StructField("watermark_ts", TS),          # end of the last fully loaded window
    T.StructField("last_load_date", T.DateType()),
    T.StructField("last_batch_id", S),
    T.StructField("last_status", S),
    T.StructField("updated_at", TS),
])


def create_meta_tables(spark, cfg):
    """Create run_log and watermark if they don't exist (safe to call every run)."""
    for key, schema in (("run_log", RUN_LOG_SCHEMA), ("watermark", WATERMARK_SCHEMA)):
        (spark.createDataFrame([], schema)
              .write.format("delta").mode("ignore")
              .saveAsTable(cl.meta_table(cfg, key)))


def _dt(v):
    """ISO string / datetime / None -> timezone-aware datetime / None."""
    if v is None or isinstance(v, datetime):
        return v
    return datetime.fromisoformat(v.replace("Z", "+00:00"))


def _i(v):
    return int(v) if v is not None else None


def log_run(spark, cfg, *, run_id, pipeline_name, stage, resource_type, load_date, status,
            batch_id=None, source_format=None, window_since=None, window_until=None,
            started_at=None, ended_at=None, data_saved_at=None, pages_fetched=None,
            records_read=None, records_written=None, truncated=None, error_message=None):
    """Append one row to the run log. A logging problem warns but never breaks the pipeline."""
    row = (
        uuid.uuid4().hex, run_id, batch_id, pipeline_name, stage, resource_type,
        datetime.strptime(load_date, "%Y-%m-%d").date() if load_date else None,
        source_format, _dt(window_since), _dt(window_until),
        _dt(started_at), _dt(ended_at), _dt(data_saved_at),
        _i(pages_fetched), _i(records_read), _i(records_written),
        bool(truncated) if truncated is not None else None,
        status, error_message[:4000] if error_message else None, cl.utc_now(),
    )
    try:
        (spark.createDataFrame([row], RUN_LOG_SCHEMA)
              .write.format("delta").mode("append")
              .saveAsTable(cl.meta_table(cfg, "run_log")))
    except Exception as e:
        print(f"WARNING: could not write run log ({stage}/{resource_type}/{load_date}): {e}")


def log_raw_summary(spark, cfg, s, run_id, pipeline_name):
    """Turn the dict returned by fhir_client.ingest_resource_to_raw into a RAW log row."""
    log_run(spark, cfg, run_id=run_id, pipeline_name=pipeline_name, stage="RAW",
            resource_type=s["resource"], load_date=s["load_date"], status=s["status"],
            batch_id=s["batch_id"], source_format=s["format"],
            window_since=s["window_since"], window_until=s["window_until"],
            started_at=s["started_at"], ended_at=s["ended_at"], data_saved_at=s["ended_at"],
            pages_fetched=s["pages_fetched"], records_read=s["records_fetched"],
            records_written=s["records_fetched"], truncated=s["truncated"],
            error_message=s["error"])