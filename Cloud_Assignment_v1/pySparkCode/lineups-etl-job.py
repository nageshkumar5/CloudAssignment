import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

# Build Spark Session
spark = SparkSession.builder \
    .appName("Flatten Lineup JSON Example with Cards") \
    .getOrCreate()

# Set Input and Output Paths
input_directory = "/Users/aakashgangurde/Desktop/CSC1142_Cloud_Technologies/Assignment/raw-data/lineups/*.json"
output_directory = "/Users/aakashgangurde/Desktop/CSC1142_Cloud_Technologies/Assignment/standard-data/lineups"

# Create a dataframe for reading raw JSON data
df = spark.read.json(input_directory, multiLine=True)

# Flatten the raw JSON data in a dataframe
flattened_df = df.withColumn("player", explode(col("lineup"))) \
    .select(
        col("team_id"),
        col("team_name"),
        col("player.player_id"),
        col("player.player_name"),
        col("player.player_nickname"),
        col("player.jersey_number"),
        col("player.country.id").alias("country_id"),
        col("player.country.name").alias("country_name"),
        explode("player.positions").alias("position")
    ) \
    .select(
        col("team_id"),
        col("team_name"),
        col("player_id"),
        col("player_name"),
        col("player_nickname"),
        col("jersey_number"),
        col("country_id"),
        col("country_name"),
        col("position.position_id").alias("position_id"),
        col("position.position").alias("position_name"),
        col("position.from").alias("position_start_time"),
        col("position.to").alias("position_end_time"),
        col("position.from_period").alias("start_period"),
        col("position.to_period").alias("end_period"),
        col("position.start_reason"),
        col("position.end_reason")
    )

# Handle exploding cards attribute separately
cards_df = df.withColumn("player", explode(col("lineup"))) \
    .select(
        col("team_id"),
        col("team_name"),
        col("player.player_id"),
        col("player.cards")
    ) \
    .filter(col("cards").isNotNull()) \
    .withColumn("card", explode(col("cards"))) \
    .select(
        col("team_id"),
        col("team_name"),
        col("player_id"),
        col("card.time").alias("card_time"),
        col("card.card_type"),
        col("card.reason"),
        col("card.period").alias("card_period")
    )

# Join the Flattened dataframe with cards data
result_df = flattened_df.join(
    cards_df,
    ["team_id", "team_name", "player_id"],
    how="left"
)

# Making sure each partition has 1000 records max
total_records = result_df.count()
partition_count = max(1, -(-total_records // 1000))
result_df = result_df.repartition(partition_count)

# Save flattenened JSON files as in a standardized CSV format, overwrite existing files
result_df.write.option("header", "true").mode("overwrite").csv(output_directory)

print(f"Total Records Processed: {total_records}")
print(f"Files processed successfully to {output_directory}")

# Stop spark session
spark.stop()