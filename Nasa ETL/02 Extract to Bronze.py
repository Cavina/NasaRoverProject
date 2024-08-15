# Databricks notebook source
import requests
import json
from pyspark.sql.functions import explode

# COMMAND ----------


params = {"earth_date":"2016-10-17", "api_key":"DEMO_KEY"}
f = r"https://api.nasa.gov/mars-photos/api/v1/rovers/curiosity/photos?"
data = requests.get(f, params = params)
payload = json.loads(data.text)
payload_rdd = spark.sparkContext.parallelize([payload])

df = spark.read.json(payload_rdd)

df.printSchema()


# COMMAND ----------

s3_bronze_path = "s3a://databricks-workspace-stack-691e1-bucket/nasa_rover_bronze/raw_data"

# COMMAND ----------

df.write.mode("overwrite").json(s3_bronze_path)
