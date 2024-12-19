from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session with optimized configurations
spark = SparkSession.builder \
    .appName("Flatten Competition JSON Example") \
    .config("spark.network.maxRemoteBlockSizeFetchToMem", "128m") \
    .config("spark.rpc.message.maxSize", "256") \
    .config("spark.network.compress", "true") \
    .getOrCreate()

# Define Input and Output Paths
input_directory = "/raw_data/competitions/*.json"
output_directory = "/standard_data/competitions"

# Read the JSON file into a DataFrame
df = spark.read.json(input_directory, multiLine=True).repartition(10)  # Adjust partition size if necessary

# Flatten the JSON structure
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

# Save the flattened DataFrame as a CSV
flattened_df.coalesce(1).write.option("header", "true").csv(output_directory)

# Stop the Spark session
spark.stop()
