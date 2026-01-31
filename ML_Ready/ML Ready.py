# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE default.gold_department_load_risk AS
# MAGIC SELECT
# MAGIC     *,
# MAGIC     CASE
# MAGIC         WHEN total_admissions >= 18 
# MAGIC              OR avg_triage_time_min >= 35
# MAGIC             THEN 'HIGH'
# MAGIC
# MAGIC         WHEN total_admissions BETWEEN 12 AND 17
# MAGIC              OR avg_triage_time_min BETWEEN 25 AND 35
# MAGIC             THEN 'MEDIUM'
# MAGIC
# MAGIC         ELSE 'LOW'
# MAGIC     END AS overload_risk
# MAGIC FROM default.gold_department_daily_load;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     overload_risk,
# MAGIC     COUNT(*) AS days_count
# MAGIC FROM default.gold_department_load_risk
# MAGIC GROUP BY overload_risk;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     date,
# MAGIC     total_admissions,
# MAGIC     avg_triage_time_min,
# MAGIC     overload_risk
# MAGIC FROM default.gold_department_load_risk
# MAGIC WHERE overload_risk = 'HIGH'
# MAGIC ORDER BY date;
# MAGIC