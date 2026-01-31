# Databricks notebook source
# MAGIC %md
# MAGIC ER Wait Time Dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.er_wait_time LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE default.er_wait_time_dataset RENAME TO default.bronze_er_wait_time;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Admission dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.admission_data LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE default.bronze_admission_data
# MAGIC AS
# MAGIC SELECT
# MAGIC   `MRD No.`                         AS patient_id,
# MAGIC   `D.O.A`                           AS admission_date,
# MAGIC   `D.O.D`                           AS discharge_date,
# MAGIC   `TYPE OF ADMISSION-EMERGENCY/OPD` AS admission_type,
# MAGIC   `month year`                      AS month_year,
# MAGIC   `DURATION OF STAY`                AS length_of_stay,
# MAGIC   `OUTCOME`                         AS outcome,
# MAGIC   current_date()                    AS ingestion_date,
# MAGIC   'HDHI Admission data.csv'         AS source_file
# MAGIC FROM default.admission_data;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.bronze_admission_data LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS default.admission_data;

# COMMAND ----------

# MAGIC %md
# MAGIC Emergency room patient data
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.emergency_room_patient_forecast LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE default.emergency_room_patient_forecast RENAME TO default.bronze_emergency_room_data;
# MAGIC