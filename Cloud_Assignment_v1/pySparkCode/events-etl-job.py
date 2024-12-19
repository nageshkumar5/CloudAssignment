import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

# Build Spark Session
spark = SparkSession.builder \
    .appName("Flatten JSON Example with Fixed Records") \
    .getOrCreate()

# Set Input and Output Paths
input_directory = "/Users/aakashgangurde/Desktop/CSC1142_Cloud_Technologies/Assignment/raw-data/events/*.json" 
output_directory = "/Users/aakashgangurde/Desktop/CSC1142_Cloud_Technologies/Assignment/standard-data/events"

# Create a dataframe for reading raw JSON data
df = spark.read.json(input_directory, multiLine=True)

# Flatten the raw JSON data in a dataframe
flattened_df = df.withColumn("lineup", explode(col("tactics.lineup"))) \
    .select(
        col("id").alias("event_id"),
        col("index"),
        col("period"),
        col("timestamp"),
        col("minute"),
        col("second"),
        col("type.id").alias("type_id"),
        col("type.name").alias("type_name"),
        col("team.id").alias("team_id"),
        col("team.name").alias("team_name"),
        col("tactics.formation").alias("formation"),
        col("lineup.player.id").alias("player_id"),
        col("lineup.player.name").alias("player_name"),
        col("lineup.position.id").alias("position_id"),
        col("lineup.position.name").alias("position_name"),
        col("lineup.jersey_number").alias("jersey_number")
    )

# Making sure each partition has 1000 records max
partition_count = max(1, -(-flattened_df.count() // 1000))  # Ceil logic using negative division
flattened_df = flattened_df.repartition(partition_count)

# Save flattenened JSON files as in a standardized CSV format, overwrite existing files
flattened_df.write.option("header", "true").mode("overwrite").csv(output_directory)

print(f'Files processed to {output_directory} directory!')

# Stop spark session
spark.stop()