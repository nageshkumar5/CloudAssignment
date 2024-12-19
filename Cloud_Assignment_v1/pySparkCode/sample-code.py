from pyspark.sql import SparkSession
from pyspark.sql.functions import col

print("checkpoint........1")

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SparkHDFSExample") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

print("checkpoint.....2")
# HDFS path to the JSON file
hdfs_path = "hdfs://namenode:9000/competitions.json"
output_directory = "hdfs://namenode:9000/data/output"
print(hdfs_path)

# Read the JSON file into a DataFrame
df = spark.read.json(hdfs_path)
print("checkpoint.....3")
# Read the JSON file into a DataFrame
df = spark.read.json(hdfs_path, multiLine=True).repartition(10)  # Adjust partition size if necessary

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
print("checkpoint.....4")
# Save the flattened DataFrame as a CSV
flattened_df.coalesce(1).write.option("header", "true").csv(output_directory)
print("checkpoint.....5")
# Stop the Spark session
spark.stop()
