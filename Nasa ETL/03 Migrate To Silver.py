# Databricks notebook source
from pyspark.sql.functions import explode, col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, StructType, StructField, LongType, FloatType, DoubleType, DateType

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS nasa_rover_silver;

# COMMAND ----------

s3_bronze_path = "s3a://databricks-workspace-stack-691e1-bucket/nasa_rover_bronze/raw_data"
s3_silver_path = "s3a://databricks-workspace-stack-691e1-bucket/nasa_rover_silver"

# COMMAND ----------

bronze_df = spark.read.json(s3_bronze_path)
bronze_df.printSchema()


# COMMAND ----------

df_exploded = bronze_df.withColumn("photo", explode(bronze_df["photos"]))
df_final = df_exploded.drop("photos")

df_final = df_final.select(
    col("photo.id").alias("photo_id"),
    "photo.sol",
    "photo.earth_date",
    "photo.camera",
    "photo.img_src",
    "photo.rover",

)

df_final.printSchema()
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
