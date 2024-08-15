# Databricks notebook source
from pyspark.sql.functions import explode, col, lit

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS nasa_rover_gold;
# MAGIC

# COMMAND ----------

silver_df = spark.table("nasa_rover_silver.silver_mars_rover")



# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

photos_df = (silver_df.select(
    col("photo_id"),
    col("sol"),
    col("earth_date"),
    col("img_src"),
    col("camera.id").alias("camera_id"),
    col("rover.id").alias("rover_id")
)
)

photos_df.show()

# COMMAND ----------

photos_df.count()

# COMMAND ----------

manifest_exploded = silver_df.withColumn("cameras", explode(silver_df["rover.cameras"]))

mission_manifest_final_df = manifest_exploded.select(
    manifest_exploded["rover.id"].alias("rover_id"),
    manifest_exploded["rover.landing_date"].alias("rover_landing_date"),
    manifest_exploded["rover.launch_date"].alias("rover_launch_date"),
    manifest_exploded["rover.max_date"].alias("rover_max_date"),
    manifest_exploded["rover.max_sol"].alias("rover_max_sol"),
    manifest_exploded["rover.name"].alias("rover_name"),
    manifest_exploded["rover.status"].alias("rover_status"),
    manifest_exploded["rover.total_photos"].alias("rover_total_photos")
).distinct()

mission_manifest_final_df.show()

# COMMAND ----------

mission_manifest_final_df.count()

# COMMAND ----------

camera_exploded = silver_df.select(col("camera.*"))

camera_final_df = camera_exploded.select(
  col("id"),
  col("full_name"),
  col("name"),
  col("rover_id")
  ).distinct()

camera_final_df.show()

# COMMAND ----------

camera_final_df.count()

# COMMAND ----------

photos_df.write.format("delta").mode("overwrite").saveAsTable("nasa_rover_gold.photos")

# COMMAND ----------

mission_manifest_final_df.write.format("delta").mode("overwrite").saveAsTable("nasa_rover_gold.mission_manifest")

# COMMAND ----------

camera_final_df.write.format("delta").mode("overwrite").saveAsTable("nasa_rover_gold.camera")
