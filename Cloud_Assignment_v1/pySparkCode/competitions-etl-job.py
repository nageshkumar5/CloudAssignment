import config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Build Spark Session
spark = SparkSession.builder \
    .appName("Preprocess competitions data") \
    .getOrCreate()

# Set Input and Output Paths
input_directory = f"{config.INPUT_NAMENODE}/competitions/*.json"
output_directory = f"{config.OUTPUT_NAMENODE}/competitions"

# Create a dataframe for reading raw JSON data
df = spark.read.json(input_directory, multiLine=True)

# Flatten the raw JSON data in a dataframe
flattened_df = df.select(
    col("competition_id"),
    col("season_id"),
    col("country_name"),
    col("competition_name"),
    col("competition_gender"),
    col("competition_youth"),
    col("competition_international"),
    col("season_name"),
    col("match_updated"),
    col("match_updated_360"),
    col("match_available_360"),
    col("match_available")
)

# Making sure each partition has 1000 records max
partition_count = max(1, int(flattened_df.count() / 1000))
flattened_df = flattened_df.repartition(partition_count)

# Save flattenened JSON files as in a standardized CSV format
flattened_df.write.option("header", "true").mode("overwrite").csv(output_directory)

print(f'Files processed successfully to {output_directory}')

# Stop spark session
spark.stop()