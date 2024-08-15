# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text('bronze_base_dir', '', label='Enter Target Base Dir')

# COMMAND ----------

bronze_base_dir = dbutils.widgets.get('bronze_base_dir')
