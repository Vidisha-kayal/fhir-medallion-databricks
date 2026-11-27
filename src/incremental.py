"""incremental.py: watermark read/update and load-date planning."""
from datetime import date, datetime, time, timedelta, timezone

from pyspark.sql import functions as F

from src import config_loader as cl
from src import meta


def _to_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def get_last_load_date(spark, cfg, resource):
    """Last fully loaded day for a resource, or None if it was never loaded."""
    rows = (spark.table(cl.meta_table(cfg, "watermark"))
                 .where(F.col("resource_type") == resource)
                 .select("last_load_date").collect())
    return rows[0][0] if rows and rows[0][0] else None


def latest_complete_day(cfg, now=None) -> date:
    """Newest UTC day that is over (plus the safety lag)."""
    now = now or cl.utc_now()
    lag = timedelta(minutes=cfg["incremental"]["safety_lag_minutes"])
    return (now - lag).date() - timedelta(days=1)


def plan_load_dates(cfg, last_load_date, start_override=None, end_override=None,
                    max_days=None, now=None) -> dict:
    """Decide which days to load. Never plans a day that is not complete yet."""
    inc = cfg["incremental"]
    latest = latest_complete_day(cfg, now)

    if start_override:
        start, source = _to_date(start_override), "start_date parameter"
    elif last_load_date:
        start, source = last_load_date + timedelta(days=1), "watermark"
    else:
        start, source = _to_date(str(inc["initial_start_date"])), "config initial_start_date"

    end = min(_to_date(end_override), latest) if end_override else latest
    cap = max_days or inc["max_days_per_run"]

    dates, d = [], start
    while d <= end and len(dates) < cap:
        dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)

    return {"start": start, "end": end, "latest_complete": latest,
            "start_source": source, "dates": dates}


def update_watermark(spark, cfg, resource, load_date: str, batch_id, status="SUCCESS"):
    """Move the watermark forward to load_date. Never moves it backwards."""
    d = _to_date(load_date)
    next_midnight = datetime.combine(d + timedelta(days=1), time.min, tzinfo=timezone.utc)
    row = (resource, next_midnight, d, batch_id, status, cl.utc_now())
    spark.createDataFrame([row], meta.WATERMARK_SCHEMA).createOrReplaceTempView("_wm_update")
    spark.sql(f"""
        MERGE INTO {cl.meta_table(cfg, 'watermark')} t
        USING _wm_update s
        ON t.resource_type = s.resource_type
        WHEN MATCHED AND (t.last_load_date IS NULL OR s.last_load_date > t.last_load_date)
             THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)