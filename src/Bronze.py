import sys
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# Initialize Spark Session
spark = SparkSession.builder.getOrCreate()

def run_bronze(config_path: str):
    # Read the declarative configuration file
    with open(config_path, 'r') as f:
        bronze_config = yaml.safe_load(f)["bronze"]

    active_streams = []

    # Loop through all entities defined in the YAML (orders, order_items, etc.)
    for entity_name, cfg in bronze_config.items():
        print(f"Initializing Auto Loader stream for: {entity_name}")
        
        # 1. Read Stream using Auto Loader (cloudFiles)
        raw_stream = spark.readStream \
            .format("cloudFiles") \
            .option("cloudFiles.format", "csv") \
            .option("cloudFiles.schemaLocation", f"{cfg['checkpoint']}/schema") \
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
            .option("header", "true") \
            .load(cfg["source_path"])

        # 2. Write Stream to Unity Catalog Delta Table
        # trigger(availableNow=True) ensures it processes all new files and then turns off
        query = raw_stream.withColumn("_bronze_ingest_ts", current_timestamp()) \
            .writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", cfg["checkpoint"]) \
            .option("mergeSchema", "true") \
            .trigger(availableNow=True) \
            .toTable(cfg["target_table"])
            
        active_streams.append(query)

    # 3. Await all streams concurrently to maximize cluster compute efficiency
    print("Waiting for all Bronze micro-batches to complete...")
    for q in active_streams:
        q.awaitTermination()
        
    print("Bronze ingestion complete for all entities.")


    if __name__ == "__main__":
    if len(sys.argv) > 1:
        config_file_path = sys.argv[1] 
        print(f"Starting execution using config: {config_file_path}")
        
        # Replace 'run_bronze' with 'run_silver' or 'run_gold' depending on the file
        run_bronze(config_file_path) 
    else:
        print("Error: No configuration file path provided.")
        sys.exit(1)