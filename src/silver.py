"""silver.py: flatten + clean + dedupe Bronze FHIR JSON into typed Silver-ready DataFrames."""
from pyspark.sql import Window
from pyspark.sql import functions as F

from src import config_loader as cl

META_COLS = ["meta_version_id", "meta_last_updated", "extraction_timestamp",
             "api_url_or_params", "source_batch_id", "load_date", "bronze_loaded_at"]
SYSTEM_COLS = META_COLS + ["row_hash", "silver_processed_at"]

# ---------- schemas (only the fields we use; everything else is ignored) ----------
def _s(txt):
    return " ".join(txt.split())

CODING = "struct<system:string,code:string,display:string>"
CC = f"struct<coding:array<{CODING}>,text:string>"          # CodeableConcept
REF = "struct<reference:string>"
PERIOD = "struct<`start`:string,`end`:string>"

SCHEMAS = {
    "Patient": _s(f"""struct<active:boolean, gender:string, birthDate:string,
        name:array<struct<use:string,family:string,given:array<string>>>,
        address:array<struct<city:string,state:string,postalCode:string,country:string>>,
        maritalStatus:{CC}>"""),
    "Encounter": _s(f"""struct<status:string, `class`:{CODING}, `type`:array<{CC}>,
        subject:{REF}, period:{PERIOD}, serviceProvider:{REF}, reasonCode:array<{CC}>>"""),
    "Observation": _s(f"""struct<status:string, category:array<{CC}>, code:{CC},
        subject:{REF}, encounter:{REF}, effectiveDateTime:string, effectivePeriod:{PERIOD},
        issued:string,
        valueQuantity:struct<value:double,unit:string,system:string,code:string>,
        valueString:string, valueBoolean:boolean, valueInteger:long,
        valueCodeableConcept:{CC}, interpretation:array<{CC}>,
        component:array<struct<code:{CC}>>>"""),
    "Condition": _s(f"""struct<clinicalStatus:{CC}, verificationStatus:{CC}, category:array<{CC}>,
        code:{CC}, subject:{REF}, encounter:{REF}, onsetDateTime:string,
        abatementDateTime:string, recordedDate:string>"""),
}

# ---------- small helpers ----------
def clean(c):
    """Trim text; blank -> NULL."""
    t = F.trim(c)
    return F.when(t == "", F.lit(None)).otherwise(t)

def _path(*parts):
    return ".".join(f"`{p}`" for p in parts)

def ts(*path):
    """Safe timestamp parse of a struct path; malformed -> NULL."""
    return F.expr(f"try_cast({_path(*path)} AS timestamp)")

def dt(*path):
    return F.expr(f"try_cast({_path(*path)} AS date)")

def ref_id(c, rtype):
    """'Patient/123' | 'https://host/Patient/123/_history/2' -> '123'; anything else -> NULL."""
    return clean(F.regexp_extract(c, rf"{rtype}/([^/]+)(?:/_history/[^/]+)?$", 1))

def cc_field(cc, field):
    """First coding's field of one CodeableConcept."""
    return clean(F.get(cc["coding"], 0)[field])

def first_cc_field(arr, field):
    """First coding of the first CodeableConcept in an array."""
    return cc_field(F.get(arr, 0), field)


# ---------- flatten functions ----------
def flatten_patient(df, key):
    r = F.col("r")
    name0, addr0 = F.get(r["name"], 0), F.get(r["address"], 0)
    gender = F.lower(clean(r["gender"]))
    bdate = dt("r", "birthDate")
    return df.select(
        F.col("resource_id").alias(key),
        *[F.col(c) for c in META_COLS],
        r["active"].alias("active"),
        F.when(gender.isin("male", "female", "other", "unknown"), gender)
         .otherwise("unknown").alias("gender"),
        F.when(bdate.between(F.to_date(F.lit("1900-01-01")), F.current_date()), bdate)
         .alias("birth_date"),
        F.initcap(clean(name0["family"])).alias("family_name"),
        F.initcap(clean(F.concat_ws(" ", name0["given"]))).alias("given_name"),
        F.initcap(clean(F.concat_ws(" ", name0["given"], name0["family"]))).alias("full_name"),
        F.initcap(clean(addr0["city"])).alias("city"),
        clean(addr0["state"]).alias("state"),
        clean(addr0["postalCode"]).alias("postal_code"),
        clean(addr0["country"]).alias("country"),
        cc_field(r["maritalStatus"], "code").alias("marital_status_code"),
        cc_field(r["maritalStatus"], "display").alias("marital_status_display"),
    )


def flatten_encounter(df, key):
    r = F.col("r")
    start, end = ts("r", "period", "start"), ts("r", "period", "end")
    return df.select(
        F.col("resource_id").alias(key),
        *[F.col(c) for c in META_COLS],
        F.lower(clean(r["status"])).alias("status"),
        clean(r["class"]["code"]).alias("class_code"),
        clean(r["class"]["display"]).alias("class_display"),
        first_cc_field(r["type"], "code").alias("type_code"),
        first_cc_field(r["type"], "display").alias("type_display"),
        ref_id(r["subject"]["reference"], "Patient").alias("patient_id"),
        start.alias("period_start"),
        F.when(start.isNull() | (end >= start), end).alias("period_end"),
        ref_id(r["serviceProvider"]["reference"], "Organization").alias("service_provider_id"),
        first_cc_field(r["reasonCode"], "code").alias("reason_code"),
        first_cc_field(r["reasonCode"], "display").alias("reason_display"),
    )


def flatten_observation(df, key):
    r = F.col("r")
    vq, vcc = r["valueQuantity"], r["valueCodeableConcept"]
    return df.select(
        F.col("resource_id").alias(key),
        *[F.col(c) for c in META_COLS],
        F.lower(clean(r["status"])).alias("status"),
        first_cc_field(r["category"], "code").alias("category_code"),
        cc_field(r["code"], "system").alias("code_system"),
        cc_field(r["code"], "code").alias("code"),
        F.coalesce(cc_field(r["code"], "display"), clean(r["code"]["text"])).alias("code_display"),
        ref_id(r["subject"]["reference"], "Patient").alias("patient_id"),
        ref_id(r["encounter"]["reference"], "Encounter").alias("encounter_id"),
        F.coalesce(ts("r", "effectiveDateTime"), ts("r", "effectivePeriod", "start"))
         .alias("effective_at"),
        ts("r", "issued").alias("issued_at"),
        F.when(vq["value"].isNotNull(), "quantity")
         .when(r["valueInteger"].isNotNull(), "integer")
         .when(r["valueString"].isNotNull(), "string")
         .when(r["valueBoolean"].isNotNull(), "boolean")
         .when(vcc.isNotNull(), "codeable_concept")
         .otherwise("none").alias("value_type"),
        F.coalesce(vq["value"], r["valueInteger"].cast("double")).alias("value_number"),
        clean(vq["unit"]).alias("value_unit"),
        clean(F.coalesce(r["valueString"], vcc["text"], F.get(vcc["coding"], 0)["display"],
                         r["valueBoolean"].cast("string"))).alias("value_text"),
        first_cc_field(r["interpretation"], "code").alias("interpretation_code"),
        F.when(r["component"].isNull(), 0).otherwise(F.size(r["component"])).alias("component_count"),
    )


def flatten_condition(df, key):
    r = F.col("r")
    return df.select(
        F.col("resource_id").alias(key),
        *[F.col(c) for c in META_COLS],
        cc_field(r["clinicalStatus"], "code").alias("clinical_status"),
        cc_field(r["verificationStatus"], "code").alias("verification_status"),
        first_cc_field(r["category"], "code").alias("category_code"),
        cc_field(r["code"], "system").alias("code_system"),
        cc_field(r["code"], "code").alias("code"),
        F.coalesce(cc_field(r["code"], "display"), clean(r["code"]["text"])).alias("code_display"),
        ref_id(r["subject"]["reference"], "Patient").alias("patient_id"),
        ref_id(r["encounter"]["reference"], "Encounter").alias("encounter_id"),
        ts("r", "onsetDateTime").alias("onset_at"),
        ts("r", "abatementDateTime").alias("abatement_at"),
        ts("r", "recordedDate").alias("recorded_at"),
    )


SPECS = {
    "Patient": {"schema": SCHEMAS["Patient"], "flatten": flatten_patient},
    "Encounter": {"schema": SCHEMAS["Encounter"], "flatten": flatten_encounter},
    "Observation": {"schema": SCHEMAS["Observation"], "flatten": flatten_observation},
    "Condition": {"schema": SCHEMAS["Condition"], "flatten": flatten_condition},
}


# ---------- dedupe + hash ----------
def dedupe_versions(df, key):
    """One row per (resource id, version id). Keeps the most recently extracted copy."""
    vkey = F.coalesce(F.col("meta_version_id"), F.col("meta_last_updated").cast("string"),
                      F.lit("unknown"))
    w = (Window.partitionBy(F.col(key), vkey)
               .orderBy(F.col("extraction_timestamp").desc(), F.col("load_date").desc()))
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def add_row_hash(df, key):
    """SHA-256 of all business columns (not the key, not metadata)."""
    biz = [c for c in df.columns if c not in META_COLS and c != key]
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in biz]
    return df.withColumn("row_hash", F.sha2(F.concat_ws("||", *parts), 256))


# ---------- main entry points ----------
def _bronze(spark, cfg, resource, load_dates=None):
    df = spark.table(cl.resource_table(cfg, "bronze", resource))
    if load_dates:
        df = df.where(F.col("load_date").isin(list(load_dates)))
    return df


def build_silver_df(spark, cfg, resource, load_dates=None):
    """Bronze -> flattened, cleaned, deduplicated DataFrame (not written anywhere yet)."""
    spec = SPECS[resource]
    key = cl.get_resource(cfg, resource)["silver_key"]
    parsed = (_bronze(spark, cfg, resource, load_dates)
              .where(F.col("resource_id").isNotNull())
              .select("resource_id", "meta_version_id", "meta_last_updated",
                      "extraction_timestamp", "api_url_or_params",
                      F.col("batch_id").alias("source_batch_id"), "load_date", "bronze_loaded_at",
                      F.from_json("resource_json", spec["schema"]).alias("r")))
    out = add_row_hash(dedupe_versions(spec["flatten"](parsed, key), key), key)
    return out.withColumn("silver_processed_at", F.current_timestamp())


def count_parse_failures(spark, cfg, resource, load_dates=None):
    """Bronze rows whose JSON could not be parsed with the schema at all."""
    schema = SPECS[resource]["schema"]
    return (_bronze(spark, cfg, resource, load_dates)
            .where(F.from_json("resource_json", schema).isNull()).count())


def null_profile(df):
    """Per-column null counts: reveals fields that are always empty or mis-parsed."""
    cols = [c for c in df.columns if c not in SYSTEM_COLS]
    row = df.agg(F.count(F.lit(1)).alias("_rows"),
                 *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).collect()[0].asDict()
    total = row.pop("_rows")
    return [{"column": c, "null_count": int(n or 0),
             "pct_null": round(100.0 * (n or 0) / total, 1) if total else None}
            for c, n in row.items()]