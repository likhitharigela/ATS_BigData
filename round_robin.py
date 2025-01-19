import os
import time
import json 
from pyspark.sql import SparkSession

def round_robin_distribute_spark(input_path, output_path):
    spark = SparkSession.builder.appName("RoundRobinDistribution").getOrCreate()

    # Read the input JSON file into a DataFrame
    queue_df = spark.read.json(input_path)

    # Add an index column to distribute records alternately
    indexed_df = queue_df.rdd.zipWithIndex().toDF(["data", "index"])

    # Separate into two DataFrames using modulo operation
    spark_a = indexed_df.filter("index % 2 = 0").select("data.*")
    spark_b = indexed_df.filter("index % 2 = 1").select("data.*")

    # Save the two DataFrames as separate JSON outputs
    spark_a.write.json(os.path.join(output_path, "Spark_Module_A"), mode="overwrite")
    spark_b.write.json(os.path.join(output_path, "Spark_Module_B"), mode="overwrite")

    print(f"Round-robin distribution saved to {output_path}")

round_robin_distribute_spark("queue.json", "distribution")
