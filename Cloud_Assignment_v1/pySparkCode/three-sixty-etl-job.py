from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, concat_ws

# Build Spark Session
spark = SparkSession.builder \
    .appName("Flatten Event JSON") \
    .getOrCreate()

# Set Input and Output Paths
input_directory = "/Users/aakashgangurde/Desktop/CSC1142_Cloud_Technologies/Assignment/raw-data/three-sixty/*.json"
output_directory = "/Users/aakashgangurde/Desktop/CSC1142_Cloud_Technologies/Assignment/standard-data/three-sixty"

# Create a dataframe for reading raw JSON data
df = spark.read.option("multiline", "true").json(input_directory)

# Flatten the raw JSON data in a dataframe
flattened_df = df.select(
    col("event_uuid"),
    concat_ws(",", col("visible_area")).alias("visible_area_str"),  # Convert array to string
    explode("freeze_frame").alias("freeze_frame")
).select(
    col("event_uuid"),
    col("visible_area_str"),
    col("freeze_frame.teammate").alias("teammate"),
    col("freeze_frame.actor").alias("actor"),
    col("freeze_frame.keeper").alias("keeper"),
    col("freeze_frame.location")[0].alias("location_x"),
    col("freeze_frame.location")[1].alias("location_y")
)

# Join the Flattened dataframe with cards data
flattened_df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_directory)

print(f"Files processed successfully to {output_directory}")

# Stop spark session
spark.stop() 

