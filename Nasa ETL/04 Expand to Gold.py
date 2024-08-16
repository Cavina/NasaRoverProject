# Databricks notebook source
from pyspark.sql.functions import explode, col, lit

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS nasa_rover_gold;
# MAGIC

# COMMAND ----------

silver_df = spark.table("nasa_rover_silver.silver_mars_rover")



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

rover_cameras_df = silver_df.select(
    col("rover.id").alias("rover_id"),
    explode(col("rover.cameras")).alias("camera")
).select(
    col("rover_id"),
    col("camera.full_name").alias("camera_full_name"),
    col("camera.name").alias("camera_name")
).distinct()



# COMMAND ----------

photo_cameras_df = silver_df.select(
    col("camera.full_name").alias("photo_camera_full_name"),
    col("camera.name").alias("photo_camera_name"),
    col("camera.id").alias("photo_camera_id"),
    col("camera.rover_id").alias("photo_rover_id")
).distinct()


# COMMAND ----------

camera_details_df = rover_cameras_df.join(
    photo_cameras_df,
    (rover_cameras_df.camera_name == photo_cameras_df.photo_camera_name) & 
    (rover_cameras_df.rover_id == photo_cameras_df.photo_rover_id),
    how="left"
).select(
    col("rover_id"),
    col("camera_name"),
    col("camera_full_name").alias("photo_camera_full_name"),
    col("photo_camera_id").alias("camera_id")
).distinct()

camera_details_df.show()

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

photos_df.write.format("delta").mode("overwrite").saveAsTable("nasa_rover_gold.photos")

# COMMAND ----------

mission_manifest_final_df.write.format("delta").mode("overwrite").saveAsTable("nasa_rover_gold.mission_manifest")

# COMMAND ----------

camera_details_df.write.format("delta").mode("overwrite").saveAsTable("nasa_rover_gold.camera")
