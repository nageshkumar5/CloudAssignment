import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, when

# Build Spark Session
spark = SparkSession.builder \
    .appName("Flatten Match JSON Example") \
    .getOrCreate()

# Set Input and Output Paths
input_directory = "/Users/aakashgangurde/Desktop/CSC1142_Cloud_Technologies/Assignment/raw-data/matches"
output_directory = "/Users/aakashgangurde/Desktop/CSC1142_Cloud_Technologies/Assignment/standard-data/matches"

# Create a dataframe for reading raw JSON data
df = spark.read.option("recursiveFileLookup", "true").json(input_directory, multiLine=True)

# Flatten the raw JSON data in a dataframe
# Flatten home team data
home_team_df = df.select(
    col("match_id"),
    col("match_date"),
    col("kick_off"),
    col("match_week"),
    col("home_score"),
    col("away_score"),
    col("competition.competition_id").alias("competition_id"),
    col("competition.competition_name").alias("competition_name"),
    col("competition.country_name").alias("country_name"),
    col("season.season_id").alias("season_id"),
    col("season.season_name").alias("season_name"),
    col("home_team.home_team_id").alias("team_id"),
    col("home_team.home_team_name").alias("team_name"),
    col("home_team.home_team_gender").alias("team_gender"),
    col("home_team.country.id").alias("team_country_id"),
    col("home_team.country.name").alias("team_country_name"),
    explode("home_team.managers").alias("team_manager"),
).select(
    col("match_id"),
    col("match_date"),
    col("kick_off"),
    col("match_week"),
    col("home_score"),
    col("away_score"),
    col("competition_id"),
    col("competition_name"),
    col("country_name"),
    col("season_id"),
    col("season_name"),
    col("team_id"),
    col("team_name"),
    col("team_gender"),
    col("team_country_id"),
    col("team_country_name"),
    col("team_manager.id").alias("manager_id"),
    col("team_manager.name").alias("manager_name"),
    col("team_manager.nickname").alias("manager_nickname"),
    col("team_manager.dob").alias("manager_dob"),
    col("team_manager.country.id").alias("manager_country_id"),
    col("team_manager.country.name").alias("manager_country_name"),
).withColumn("home_or_away", lit("home"))

# Flatten away team data
away_team_df = df.select(
    col("match_id"),
    col("match_date"),
    col("kick_off"),
    col("match_week"),
    col("home_score"),
    col("away_score"),
    col("competition.competition_id").alias("competition_id"),
    col("competition.competition_name").alias("competition_name"),
    col("competition.country_name").alias("country_name"),
    col("season.season_id").alias("season_id"),
    col("season.season_name").alias("season_name"),
    col("away_team.away_team_id").alias("team_id"),
    col("away_team.away_team_name").alias("team_name"),
    col("away_team.away_team_gender").alias("team_gender"),
    col("away_team.country.id").alias("team_country_id"),
    col("away_team.country.name").alias("team_country_name"),
    explode("away_team.managers").alias("team_manager"),
).select(
    col("match_id"),
    col("match_date"),
    col("kick_off"),
    col("match_week"),
    col("home_score"),
    col("away_score"),
    col("competition_id"),
    col("competition_name"),
    col("country_name"),
    col("season_id"),
    col("season_name"),
    col("team_id"),
    col("team_name"),
    col("team_gender"),
    col("team_country_id"),
    col("team_country_name"),
    col("team_manager.id").alias("manager_id"),
    col("team_manager.name").alias("manager_name"),
    col("team_manager.nickname").alias("manager_nickname"),
    col("team_manager.dob").alias("manager_dob"),
    col("team_manager.country.id").alias("manager_country_id"),
    col("team_manager.country.name").alias("manager_country_name"),
).withColumn("home_or_away", lit("away"))

# Combine both home and away team data
flattened_df = home_team_df.union(away_team_df)

# Addin a extra result column to store data if the team won, lost or drew
flattened_df = flattened_df.withColumn(
    "result",
    when(
        (col("home_or_away") == "home") & (col("home_score") > col("away_score")), "win"
    ).when(
        (col("home_or_away") == "away") & (col("away_score") > col("home_score")), "win"
    ).when(
        col("home_score") == col("away_score"), "draw"
    ).otherwise("loss")
)

# Making sure each output partition has 1000 records max
total_records = flattened_df.count()
num_partitions = (total_records // 1000) + (1 if total_records % 1000 != 0 else 0)

# Save flattenened JSON files as in a standardized CSV format, overwrite existing files
flattened_df.repartition(num_partitions).write.option("header", "true").mode("overwrite").csv(output_directory)

print(f"Files successfully processed to {output_directory}")

# Stop spark session
spark.stop()