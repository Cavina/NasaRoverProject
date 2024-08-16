# Databricks notebook source
from pyspark.sql.functions import count, countDistinct, col, avg, max, datediff, current_date, first

# COMMAND ----------

photos_df = spark.table("nasa_rover_gold.photos")
mission_manifest_df = spark.table("nasa_rover_gold.mission_manifest")
cameras_df = spark.table("nasa_rover_gold.camera")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Photo Summary Table

# COMMAND ----------

photos_summary_df = photos_df.groupBy("sol").agg(
    count("photo_id").alias("photo_count"),
    countDistinct("camera_id").alias("camera_count"),
    first("rover_id").alias("rover_id")
).orderBy("sol")

photos_summary_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Camera Usage Table

# COMMAND ----------

camera_usage_df = photos_df.groupBy("camera_id").agg(
    count("photo_id").alias("photo_count")
).join(
    cameras_df,
    photos_df["camera_id"] == cameras_df["camera_id"], 
    how="left"
).select(
    photos_df["camera_id"],
    col("camera_name").alias("camera_name"),
    col("rover_id"),
    col("photo_count")
).orderBy(col("photo_count").desc())

camera_usage_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rover Activity Table

# COMMAND ----------

rover_activity_df = photos_df.groupBy("rover_id").agg(
    count("photo_id").alias("total_photos"),
    avg("sol").alias("average_sol_photos"),
    max("sol").alias("max_sol")
).join(mission_manifest_df, on="rover_id", how="left").select(
    col("rover_id"),
    col("rover_name"),
    col("total_photos"),
    col("average_sol_photos"),
    col("max_sol")
).orderBy(col("total_photos").desc())

rover_activity_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Camera Efficiency Metrics

# COMMAND ----------

camera_efficiency_df = photos_df.groupBy("camera_id", "rover_id").agg(
    count("photo_id").alias("photo_count"),
    (count("photo_id") / countDistinct("sol")).alias("efficiency_score")
).join(cameras_df, photos_df["camera_id"] == cameras_df["camera_id"], how="left") \
.select(
    photos_df["camera_id"],
    cameras_df["camera_name"].alias("camera_name"),
    photos_df["rover_id"],
    "photo_count",
    "efficiency_score"
).orderBy(col("efficiency_score").desc())

camera_efficiency_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Rover Deployment Timeline

# COMMAND ----------

# Calculate rover deployment timeline
rover_deployment_df = mission_manifest_df.select(
    "rover_id",
    "rover_name",
    "rover_landing_date",
    "rover_launch_date",
    "rover_status",
    datediff(current_date(), col("rover_landing_date")).alias("mission_duration")
).orderBy("rover_landing_date")

rover_deployment_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Photo Distribution by Camera and Rover
# MAGIC
# MAGIC

# COMMAND ----------

photo_distribution_df = photos_df.groupBy("rover_id", "camera_id").agg(
    count("photo_id").alias("photo_count")
).join(
    cameras_df, 
    photos_df["camera_id"] == cameras_df["camera_id"], 
    how="left"
).select(
    photos_df["rover_id"],
    photos_df["camera_id"],
    cameras_df["camera_name"],
    "photo_count"
).orderBy(col("photo_count").desc())

photo_distribution_df.show()
