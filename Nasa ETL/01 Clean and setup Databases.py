# Databricks notebook source
# MAGIC %md
# MAGIC Clean up databases and filepaths before beginning.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS nasa_rover_bronze CASCADE;
# MAGIC DROP DATABASE IF EXISTS nasa_rover_silver CASCADE;
# MAGIC DROP DATABASE IF EXISTS nasa_rover_gold CASCADE;

# COMMAND ----------

dbutils.fs.rm(dbutils.widgets.get('bronze_base_dir'), recurse=True)
dbutils.fs.rm(dbutils.widgets.get('silver_base_dir'), recurse=True)
dbutils.fs.rm(dbutils.widgets.get('gold_base_dir'), recurse=True)
