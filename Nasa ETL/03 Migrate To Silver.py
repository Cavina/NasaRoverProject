# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.functions import explode, col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, StructType, StructField, LongType, FloatType, DoubleType, DateType

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS nasa_rover_silver;

# COMMAND ----------

bronze_base_dir = dbutils.widgets.get('bronze_base_dir')
rovers = ["curiosity", "opportunity", "spirit"]

# COMMAND ----------

def read_rover_data_from_s3(rovers):
    rover_data = {}
    for rover in rovers:
        try:
            print(f"Reading data from {rover}")
            df = spark.read.json(f"{bronze_base_dir}/{rover}_data")
            df_with_rover_name = df.withColumn("rover_name", lit(rover))
            rover_data[rover] = df_with_rover_name
        except Exception as e:
            print(f"Error reading data from {rover}")
    return rover_data       
    

# COMMAND ----------

def union_rover_data(rover_data_dict):
    dataframes = list(rover_data_dict.values())
    unified_df = dataframes[0]
    for d in dataframes[1:]:
        unified_df = unified_df.union(d)

    return unified_df

# COMMAND ----------

rover_data_dict = read_rover_data_from_s3(rovers)

# COMMAND ----------

df_union = union_rover_data(rover_data_dict)

# COMMAND ----------

df_final = df_union.select(
    col("id").alias("photo_id"),
    "sol",
    "earth_date",
    "camera",
    "img_src",
    "rover",
    "rover_name"
)

df_final.show()

# COMMAND ----------

#This used to be save to the s3 bucket directly.
#Using saveAsTable saves it in the metastore.

df_final.write.format("delta").mode("overwrite").saveAsTable("nasa_rover_silver.silver_mars_rover")
