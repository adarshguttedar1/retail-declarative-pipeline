import sys
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StringType

spark = SparkSession.builder.getOrCreate()

def run_silver(config_path: str):
    with open(config_path, 'r') as f:
        silver_config = yaml.safe_load(f)["silver"]

    active_streams = []

    for entity_name, cfg in silver_config.items():
        print(f"Processing Silver stream for: {entity_name}")
        
        # 1. Read Stream directly from the Bronze Delta Table
        df = spark.readStream \
            .format("delta") \
            .table(cfg["source_table"])

        # 2. Dynamic Data Quality Engine (Reads rules from YAML)
        filters = cfg.get("data_quality_filters", {})
        for column, rule in filters.items():
            if rule.lower() == "is_not_null":
                df = df.filter(col(column).isNotNull())
            else:
                # Cast string columns to double only for numeric comparisons (not string filters)
                if isinstance(df.schema[column].dataType, StringType) and "'" not in rule:
                    df = df.withColumn(column, col(column).cast("double"))
                df = df.filter(expr(rule))

        # 3. Write Stream to Silver Delta Table
        query = df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", cfg["checkpoint"]) \
            .option("mergeSchema", "true") \
            .trigger(availableNow=True) \
            .toTable(cfg["target_table"])
            
        active_streams.append(query)

    # 4. Await concurrent execution
    print("Waiting for all Silver micro-batches to complete...")
    for q in active_streams:
        q.awaitTermination()
        
    print("Silver processing complete for all entities.")

if __name__ == "__main__":
    # If running interactively in a Notebook, replace sys.argv[1] with your absolute Workspace path
    # Example: config_file_path = "/Workspace/Users/your.email@company.com/retail_project/resources/retail_pipeline.yml"
    config_file_path = sys.argv[1] 
    yaml_path = "/Workspace/Users/adarsh.guttedar@simbustech.com/retail-declarative-pipeline/Retail_project/resources/dataconfig.yml"
    run_silver(yaml_path)
