from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import os
import time

def simulate_upload_spark(folder_path, output_path):
    spark = SparkSession.builder.appName("SimulateUpload").getOrCreate()

    # Generate file metadata as a list of dictionaries
    metadata = []
    for file_name in os.listdir(folder_path):
        if file_name.endswith(('.pdf', '.docx')):
            metadata.append({
                "file_name": file_name,
                "upload_timestamp": time.time(),
                "file_path": os.path.join(folder_path, file_name)
            })

    # Create a DataFrame and save it as JSON
    metadata_df = spark.createDataFrame(metadata)
    metadata_df.write.json(output_path, mode="overwrite")
    print(f"Metadata saved to {output_path}")

simulate_upload_spark("CVs", "queue.json")
