-- Gold reporting layer built on the current rows of the Silver SCD2 tables

CREATE OR REPLACE VIEW {gold}.dim_date AS
SELECT d AS date_key,
       year(d) AS calendar_year,
       quarter(d) AS calendar_quarter,
       month(d) AS calendar_month,
       date_format(d, 'MMMM') AS month_name,
       date_format(d, 'yyyy-MM') AS year_month,
       date_format(d, 'EEEE') AS day_name,
       dayofweek(d) IN (1, 7) AS is_weekend,
       current_timestamp() AS gold_refreshed_at
FROM (SELECT explode(sequence(DATE'{date_start}', DATE'{date_end}', INTERVAL 1 DAY)) AS d);

CREATE OR REPLACE VIEW {gold}.dim_patient AS
SELECT patient_id,
       full_name, given_name, family_name, gender, birth_date,
       CAST(floor(months_between(current_date(), birth_date) / 12) AS INT) AS age,
       CASE WHEN birth_date IS NULL THEN 'Unknown'
            WHEN months_between(current_date(), birth_date) / 12 < 18 THEN '0-17'
            WHEN months_between(current_date(), birth_date) / 12 < 40 THEN '18-39'
            WHEN months_between(current_date(), birth_date) / 12 < 65 THEN '40-64'
            ELSE '65+' END AS age_group,
       city, state, country,
       marital_status_display AS marital_status,
       active AS is_active,
       version_number AS silver_version_number,
       effective_from AS valid_from,
       extraction_timestamp, load_date, silver_processed_at,
       current_timestamp() AS gold_refreshed_at
FROM {silver}.patient
WHERE is_current = true;

CREATE OR REPLACE VIEW {gold}.dim_patient_history AS
SELECT patient_id, version_number, is_current, effective_from, effective_to,
       full_name, gender, birth_date, city, state, country,
       marital_status_display AS marital_status, active AS is_active,
       meta_version_id, extraction_timestamp, load_date, silver_processed_at,
       current_timestamp() AS gold_refreshed_at
FROM {silver}.patient;

CREATE OR REPLACE VIEW {gold}.dim_condition AS
SELECT code_system, code, min(code_display) AS code_display,
       count(*) AS condition_count,
       current_timestamp() AS gold_refreshed_at
FROM {silver}.condition
WHERE is_current = true AND code IS NOT NULL
GROUP BY code_system, code;

CREATE OR REPLACE VIEW {gold}.dim_observation_code AS
SELECT code_system, code, min(code_display) AS code_display, min(category_code) AS category_code,
       count(*) AS observation_count,
       current_timestamp() AS gold_refreshed_at
FROM {silver}.observation
WHERE is_current = true AND code IS NOT NULL
GROUP BY code_system, code;

CREATE OR REPLACE VIEW {gold}.fact_encounter AS
SELECT e.encounter_id, e.patient_id,
       (p.patient_id IS NOT NULL) AS patient_in_dim,
       e.status, e.class_code, e.class_display, e.type_code, e.type_display,
       e.reason_code, e.reason_display, e.service_provider_id,
       e.period_start, e.period_end,
       CAST(e.period_start AS DATE) AS encounter_date,
       CASE WHEN e.period_start IS NOT NULL AND e.period_end IS NOT NULL
            THEN round((unix_timestamp(e.period_end) - unix_timestamp(e.period_start)) / 60.0, 1) END AS duration_minutes,
       e.version_number, e.effective_from AS valid_from,
       e.extraction_timestamp, e.load_date, e.silver_processed_at,
       current_timestamp() AS gold_refreshed_at
FROM {silver}.encounter e
LEFT JOIN {silver}.patient p ON e.patient_id = p.patient_id AND p.is_current = true
WHERE e.is_current = true;

CREATE OR REPLACE VIEW {gold}.fact_observation AS
SELECT o.observation_id, o.patient_id, o.encounter_id,
       (p.patient_id IS NOT NULL) AS patient_in_dim,
       o.status, o.category_code, o.code_system, o.code, o.code_display,
       o.effective_at, CAST(o.effective_at AS DATE) AS observation_date, o.issued_at,
       o.value_type, o.value_number, o.value_unit, o.value_text, o.interpretation_code,
       o.version_number, o.effective_from AS valid_from,
       o.extraction_timestamp, o.load_date, o.silver_processed_at,
       current_timestamp() AS gold_refreshed_at
FROM {silver}.observation o
LEFT JOIN {silver}.patient p ON o.patient_id = p.patient_id AND p.is_current = true
WHERE o.is_current = true;

CREATE OR REPLACE VIEW {gold}.fact_condition AS
SELECT c.condition_id, c.patient_id, c.encounter_id,
       (p.patient_id IS NOT NULL) AS patient_in_dim,
       c.clinical_status, c.verification_status, c.category_code,
       c.code_system, c.code, c.code_display,
       c.onset_at, c.abatement_at, c.recorded_at,
       CAST(c.recorded_at AS DATE) AS recorded_date,
       c.version_number, c.effective_from AS valid_from,
       c.extraction_timestamp, c.load_date, c.silver_processed_at,
       current_timestamp() AS gold_refreshed_at
FROM {silver}.condition c
LEFT JOIN {silver}.patient p ON c.patient_id = p.patient_id AND p.is_current = true
WHERE c.is_current = true;

CREATE OR REPLACE VIEW {gold}.vw_encounters_by_month AS
SELECT date_format(period_start, 'yyyy-MM') AS year_month,
       count(*) AS encounters,
       count(DISTINCT patient_id) AS patients
FROM {gold}.fact_encounter
WHERE period_start IS NOT NULL
GROUP BY date_format(period_start, 'yyyy-MM');

CREATE OR REPLACE VIEW {gold}.vw_top_conditions AS
SELECT code, code_display,
       count(*) AS condition_count,
       count(DISTINCT patient_id) AS patients
FROM {gold}.fact_condition
WHERE code IS NOT NULL
GROUP BY code, code_display;

CREATE OR REPLACE VIEW {gold}.vw_patient_summary AS
SELECT p.patient_id, p.full_name, p.gender, p.age, p.age_group, p.city,
       coalesce(e.encounters, 0) AS encounters,
       coalesce(o.observations, 0) AS observations,
       coalesce(c.conditions, 0) AS conditions
FROM {gold}.dim_patient p
LEFT JOIN (SELECT patient_id, count(*) AS encounters FROM {gold}.fact_encounter GROUP BY patient_id) e
       ON p.patient_id = e.patient_id
LEFT JOIN (SELECT patient_id, count(*) AS observations FROM {gold}.fact_observation GROUP BY patient_id) o
       ON p.patient_id = o.patient_id
LEFT JOIN (SELECT patient_id, count(*) AS conditions FROM {gold}.fact_condition GROUP BY patient_id) c
       ON p.patient_id = c.patient_id;