# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW TABLES IN default;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Gold Department Load Daily Aggregation
# MAGIC %sql
# MAGIC DESCRIBE TABLE default.silver_er_wait_time;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM default.silver_er_wait_time;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM default.silver_er_forecast;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   SUM(CASE WHEN admission_date IS NULL THEN 1 ELSE 0 END) AS null_admission_dates
# MAGIC FROM default.silver_hospital_admissions;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Gold Department Load Daily Aggregation
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE default.gold_department_daily_load AS
# MAGIC SELECT
# MAGIC     CAST(w.visit_date AS DATE)           AS date,
# MAGIC     'Emergency'                           AS department,
# MAGIC
# MAGIC     COUNT(DISTINCT w.patient_id)          AS total_admissions,
# MAGIC
# MAGIC     -- operational metrics
# MAGIC     AVG(w.time_to_triage_min)             AS avg_triage_time_min,
# MAGIC     AVG(w.time_to_registration_min)       AS avg_registration_time_min,
# MAGIC     AVG(w.nurse_patient_ratio)            AS avg_nurse_patient_ratio,
# MAGIC     AVG(w.specialist_availability)        AS avg_specialist_availability,
# MAGIC
# MAGIC     -- forecasted load
# MAGIC     COALESCE(SUM(f.patient_count),0)      AS forecasted_er_load,
# MAGIC
# MAGIC     CURRENT_DATE()                        AS processing_date
# MAGIC
# MAGIC FROM default.silver_er_wait_time w
# MAGIC
# MAGIC LEFT JOIN default.silver_er_forecast f
# MAGIC     ON CAST(w.visit_date AS DATE) = CAST(f.date AS DATE)
# MAGIC
# MAGIC GROUP BY CAST(w.visit_date AS DATE);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM default.gold_department_daily_load
# MAGIC ORDER BY date
# MAGIC LIMIT 20;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM default.gold_department_daily_load;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM default.gold_department_load_risk
# MAGIC ORDER BY date;