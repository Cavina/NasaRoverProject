# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

import requests
import json


# COMMAND ----------

'''
Fetches the rover data for the given rover and earth date.
Accepts the rover, earth_date, and the api_key as parameters.
Returns 
'''

def fetch_rover_data(rover, earth_date, api_key):
    params = {"earth_date": earth_date, "api_key": api_key}
    url = f"https://api.nasa.gov/mars-photos/api/v1/rovers/{rover}/photos?"
    response = requests.get(url, params = params)
    data = response.json()
    # If no photos are found, return an empty list.
    # Otherwise this is flattening the first level for us in the JSON.
    photos = data.get('photos', [])

    if not photos:
        raise Exception(f"No photos found for {rover} on {earth_date}")
    else:
        return photos

# COMMAND ----------

bronze_base_dir = dbutils.widgets.get('bronze_base_dir')
earth_date = dbutils.widgets.get('earth_date')
api_key = dbutils.widgets.get('api_key')

# COMMAND ----------

'''
For every rover in the list, fetch the data for the given earth date.
Write the data to the bronze directory.
'''

rovers = ["curiosity", "opportunity", "spirit"]

for rover in rovers:
    try:
        photos = fetch_rover_data(rover, earth_date, api_key)
        df = spark.read.json(spark.sparkContext.parallelize([photos]))
        df.write.mode("overwrite").json(f"{bronze_base_dir}/{rover}_data")
    except Exception as e:
        print(f"Exception: {e}")

