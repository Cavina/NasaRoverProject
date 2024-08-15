# Databricks notebook source
import requests
import json


# COMMAND ----------

def fetch_rover_data(rover, earth_date, api_key):
    params = {"earth_date": earth_date, "api_key": api_key}
    url = f"https://api.nasa.gov/mars-photos/api/v1/rovers/{rover}/photos?"
    response = requests.get(url, params = params)
    data = response.json()
    photos = data.get('photos', [])

    if not photos:
        raise Exception(f"No photos found for {rover} on {earth_date}")
    else:
        return photos

# COMMAND ----------

s3_bronze_path = "s3a://databricks-workspace-stack-691e1-bucket/nasa_rover_bronze/raw_data"

# COMMAND ----------

rovers = ["curiosity", "opportunity", "spirit"]

for rover in rovers:
    try:
        photos = fetch_rover_data(rover, "2016-10-17", "DEMO_KEY")
        df = spark.read.json(spark.sparkContext.parallelize([photos]))
        df.write.mode("overwrite").json(f"{s3_bronze_path}/{rover}_data")
    except Exception as e:
        print(f"Exception: {e}")

