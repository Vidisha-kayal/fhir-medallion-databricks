"""steps.py: one-day building blocks (Raw + Bronze), each with run-log entries."""
from src import config_loader as cl
from src import fhir_client as fc
from src import bronze as br
from src import meta


def run_raw_day(spark, cfg, resource, load_date, max_pages, fmt, run_id, pipeline_name):
    s = fc.ingest_resource_to_raw(cfg, resource, load_date, max_pages=max_pages, fmt=fmt)
    meta.log_raw_summary(spark, cfg, s, run_id, pipeline_name)
    return s


def run_bronze_day(spark, cfg, resource, load_date, run_id, pipeline_name, fmt="json"):
    table = cl.resource_table(cfg, "bronze", resource)
    t0 = cl.utc_now()
    batch_id, expected, actual, status, error = None, 0, 0, "SUCCESS", None
    try:
        df, manifest = br.build_bronze_df(spark, cfg, resource, load_date, fmt)
        if df is None:
            status = "NO_RAW_DATA"
        else:
            batch_id = manifest.select("batch_id").first()[0]
            expected = int(br.expected_record_count(manifest))
            br.write_bronze(spark, df, table, load_date)
            actual = br.bronze_count(spark, table, load_date)
            if actual != expected:
                status, error = "FAILED", f"count mismatch: expected {expected}, got {actual}"
    except Exception as e:
        status, error = "FAILED", f"{type(e).__name__}: {e}"
    t1 = cl.utc_now()

    meta.log_run(spark, cfg, run_id=run_id, pipeline_name=pipeline_name, stage="BRONZE",
                 resource_type=resource, load_date=load_date, status=status, batch_id=batch_id,
                 source_format=fmt, started_at=t0, ended_at=t1,
                 data_saved_at=t1 if status == "SUCCESS" else None,
                 records_read=expected, records_written=actual, error_message=error)
    return {"status": status, "batch_id": batch_id, "expected": expected,
            "bronze_rows": actual, "error": error}