import os
import requests
from pyspark.sql import SparkSession

# Initializing SparkSession
spark = SparkSession.builder.appName("Extract files from GitHub API").getOrCreate()

def extract_files_from_source(source_api_url, destination_path):
    """
    This function extracts raw source data from the github source
    Params:
        source_api_url: URL of the source files
        destination_path: Destination directory
    """
    response = requests.get(source_api_url)
    if response.status_code == 200:
        items = response.json()
        for item in items:
            if item["type"] == "file":
                # Downloading file from github source
                file_url = item["download_url"]
                file_path = os.path.join(destination_path, item["name"])
                print(f"Downloading file to destination path: {file_path}")
                file_data = requests.get(file_url)
                with open(file_path, "wb") as f:
                    f.write(file_data.content)
            elif item["type"] == "dir":
                # creating the destination directory, and downloading from source
                subdir_path = os.path.join(destination_path, item["name"])
                os.makedirs(subdir_path, exist_ok=True)
                extract_files_from_source(item["url"], subdir_path)
    else:
        print(f"Failed to connect to source {source_api_url}: {response.status_code}")

# Defining source and destination path
source_api_url = "https://api.github.com/repos/statsbomb/open-data/contents/data/competitions.json"
destination_path = "jsonDataset"

os.makedirs(destination_path, exist_ok=True)

# Extracting files recursively
extract_files_from_source(source_api_url, destination_path)

# Printing success message
print(f"All files successfully downloaded to {destination_path}")