# Databricks notebook source
# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS nasa_rover_bronze CASCADE;
# MAGIC DROP DATABASE IF EXISTS nasa_rover_silver CASCADE;
# MAGIC DROP DATABASE IF EXISTS nasa_rover_gold CASCADE;

# COMMAND ----------

bronze_base_dir = 's3://databricks-workspace-stack-691e1-bucket/nasa_rover_bronze/raw_data'
silver_base_dir = 's3://databricks-workspace-stack-691e1-bucket/nasa_rover_silver/databases'
gold_base_dir = 's3://databricks-workspace-stack-691e1-bucket/nasa_rover_gold/databases'

# COMMAND ----------

dbutils.fs.rm(bronze_base_dir, recurse=True)
dbutils.fs.rm(silver_base_dir, recurse=True)
dbutils.fs.rm(gold_base_dir, recurse=True)
