#!/bin/bash

# Print current working directory for debugging
echo "Current working directory: $(pwd)"

# Zip Python files
echo "Zipping Python files..."
zip -r ../lib/spark_job.zip ../src/*.py

# Submit Spark job
echo "Submitting Spark job..."
spark-submit \
  --master spark://dev-server01:7077 \
  --deploy-mode client \
  --driver-memory 4G \
  --driver-cores 4 \
  --executor-memory 8G \
  --executor-cores 2 \
  --total-executor-cores 12 \
  --py-files ../lib/spark_job.zip \
  ../src/SparkDataFrameLatencyProcessor.py \
  --input_data_dir /sandbox/storage/data/ip_cidr_data/dataset/ip_cidr_data_parquet \
  --extract_output_data_dir /sandbox/storage/data/ip_cidr_data/filter_data/pyspark_extracted_data \
  --extract_name find_all_male_person \
  --extract_name count_total_iid_each_state
