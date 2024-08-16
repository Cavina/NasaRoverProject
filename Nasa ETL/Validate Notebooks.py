# Databricks notebook source
# MAGIC %run "./01 Clean and setup Databases" $bronze_base_dir=s3://databricks-workspace-stack-691e1-bucket/nasa_rover_bronze/raw_data $silver_base_dir=s3://databricks-workspace-stack-691e1-bucket/nasa_rover_silver/databases $gold_base_dir=s3://databricks-workspace-stack-691e1-bucket/nasa_rover_gold/databases

# COMMAND ----------

# MAGIC %run "./02 Extract to Bronze" $bronze_base_dir=s3://databricks-workspace-stack-691e1-bucket/nasa_rover_bronze/raw_data $earth_date=2016-10-17 $api_key=DEMO_KEY

# COMMAND ----------

# MAGIC %run "./03 Migrate To Silver" $bronze_base_dir=s3://databricks-workspace-stack-691e1-bucket/nasa_rover_bronze/raw_data

# COMMAND ----------

# MAGIC %run "./04 Expand to Gold" 

# COMMAND ----------

# MAGIC %run "./05 Stats Tables" 
