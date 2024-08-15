# Databricks notebook source
from pyspark.sql.functions import explode, col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, StructType, StructField, LongType, FloatType, DoubleType, DateType

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS nasa_rover_silver;

# COMMAND ----------

s3_bronze_path = "s3a://databricks-workspace-stack-691e1-bucket/nasa_rover_bronze/raw_data"
s3_silver_path = "s3a://databricks-workspace-stack-691e1-bucket/nasa_rover_silver"
rovers = ["curiosity", "opportunity", "spirit"]

# COMMAND ----------

def read_rover_data_from_s3(rovers):
    rover_data = {}
    for rover in rovers:
        try:
            print(f"Reading data from {rover}")
            df = spark.read.json(f"{s3_bronze_path}/{rover}_data")
            df_with_rover_name = df.withColumn("rover_name", lit(rover))
            rover_data[rover] = df_with_rover_name
        except Exception as e:
            print(f"Error reading data from {rover}")
    return rover_data       
    

# COMMAND ----------

rover_data_dict = read_rover_data_from_s3(rovers)

for rover in rovers:
    if rover in rover_data_dict:
        #rover_data_dict[rover].show()
        print(rover_data_dict[rover].count())

# COMMAND ----------

def union_rover_data(rover_data_dict):
    dataframes = list(rover_data_dict.values())
    unified_df = dataframes[0]
    for d in dataframes[1:]:
        unified_df = unified_df.union(d)

    return unified_df

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

# COMMAND ----------

##TOODoO
## Need to get schema and set up table with schema

# spark.sql(f"""
# CREATE TABLE IF NOT EXISTS silver_mars_rover
# (
#     id BIGINT,
#     sol BIGINT,
#     earth_date STRING,
#     camera STRUCT<full_name: STRING, id: BIGINT, name: STRING, rover_id: BIGINT>,
#     img_src STRING,
#     rover STRUCT<
#         cameras: ARRAY<STRUCT<full_name: STRING, name: STRING>>,
#         id: BIGINT,
#         landing_date: STRING,
#         launch_date: STRING,
#         max_date: STRING,
#         max_sol: BIGINT,
#         name: STRING,
#         status: STRING,
#         total_photos: BIGINT
#     >
# )
# USING DELTA
# OPTIONS (path "{s3_silver_path}")
# """)
