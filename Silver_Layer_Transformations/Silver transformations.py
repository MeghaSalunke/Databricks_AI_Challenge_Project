# Databricks notebook source
# MAGIC %md
# MAGIC ER Wait Time Dataset

# COMMAND ----------

df_er_wait = spark.table("default.bronze_er_wait_time")
display(df_er_wait)


# COMMAND ----------

df_er_wait.printSchema()


# COMMAND ----------

# DBTITLE 1,Cell 4
df_silver_er = (
    df_bronze_er
    .withColumnRenamed("Visit ID", "visit_id")
    .withColumnRenamed("Patient ID", "patient_id")
    .withColumnRenamed("Hospital ID", "hospital_id")
    .withColumnRenamed("Hospital Name", "hospital_name")
    .withColumnRenamed("Visit Date", "visit_date")
    .withColumnRenamed("Day of Week", "day_of_week")
    .withColumnRenamed("Time of Day", "time_of_day")
    .withColumnRenamed("Urgency Level", "urgency_level")
    .withColumnRenamed("Nurse-to-Patient Ratio", "nurse_patient_ratio")
    .withColumnRenamed("Specialist Availability", "specialist_availability")
    .withColumnRenamed("Facility Size (Beds)", "facility_size_beds")
    .withColumnRenamed("Time to Registration (min)", "time_to_registration_min")
    .withColumnRenamed("Time to Triage (min)", "time_to_triage_min")
    .withColumnRenamed("Time to Medical Professional (min)", "time_to_medical_professional_min")
    .withColumnRenamed("Total Wait Time (min)", "total_wait_time_min")
    .withColumnRenamed("Patient Outcome", "patient_outcome")
    .withColumnRenamed("Patient Satisfaction", "patient_satisfaction")
)


# COMMAND ----------

df_silver_er = (
    df_silver_er
    .withColumn("urgency_level", lower(col("urgency_level")))
    .withColumn("patient_outcome", lower(col("patient_outcome")))
    .withColumn("season", lower(col("Season")))
)


# COMMAND ----------

df_silver_er = df_silver_er.filter(
    col("visit_id").isNotNull() & col("visit_date").isNotNull()
)


# COMMAND ----------

df_silver_er.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.silver_er_wait_time")


# COMMAND ----------

spark.table("default.silver_er_wait_time").printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC Admission Data

# COMMAND ----------

df_adm = spark.table("default.bronze_admission_data")
display(df_adm)


# COMMAND ----------

df_adm.printSchema()

# COMMAND ----------

# DBTITLE 1,Cell 7
from pyspark.sql.functions import to_date, col

df_silver = spark.table("default.bronze_admission_data") \
    .withColumn("admission_date", to_date(col("admission_date"), "dd-MM-yyyy")) \
    .withColumn("discharge_date", to_date(col("discharge_date"), "dd-MM-yyyy"))\
    .withColumn("patient_id", col("patient_id").cast("int"))

# COMMAND ----------

df_silver.printSchema()


# COMMAND ----------

# DBTITLE 1,Fix length_of_stay type mismatch
from pyspark.sql.functions import col, to_date, when, datediff

# Load Bronze admissions table
df_bronze_adm = spark.table("default.bronze_admission_data")

# Step 1: Fix data types
df_silver_adm = (
    df_bronze_adm
    .withColumn("patient_id", col("patient_id").cast("int"))
    .withColumn("admission_date", to_date(col("admission_date")))
    .withColumn("discharge_date", to_date(col("discharge_date")))
)

# Step 2: Fix length_of_stay using datediff (KEY FIX)
df_silver_adm = df_silver_adm.withColumn(
    "length_of_stay",
    when(
        col("length_of_stay").isNull() & col("discharge_date").isNotNull(),
        datediff(col("discharge_date"), col("admission_date"))
    ).otherwise(col("length_of_stay"))
)

# Step 3: Remove invalid records
df_silver_adm = df_silver_adm.filter(col("admission_date").isNotNull())

# Step 4: Write Silver table
df_silver_adm.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.silver_hospital_admissions")


# COMMAND ----------

spark.table("default.silver_hospital_admissions").printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC Emergency Room Patient Forecast

# COMMAND ----------

df_forecast = spark.table("default.bronze_emergency_room_data")
display(df_forecast)


# COMMAND ----------

from pyspark.sql.functions import to_date

# Load Bronze table
df_bronze_forecast = spark.table("default.bronze_emergency_room_data")

# Standardize
df_silver_forecast = (
    df_bronze_forecast
    .withColumn("date", to_date(col("date")))
    .filter(col("date").isNotNull())
)

# Write Silver table
df_silver_forecast.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.silver_er_forecast")
