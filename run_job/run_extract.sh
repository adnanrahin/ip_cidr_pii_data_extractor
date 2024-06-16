#!/bin/bash

# Set variables for directories
PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
SRC_DIR="$PROJECT_DIR/src"
LIB_DIR="$PROJECT_DIR/lib"
SPARK_JOB_ZIP="spark_job.zip"

# Print current working directory for debugging
echo "Current working directory: $PROJECT_DIR"

# Change to source directory
cd "$SRC_DIR" || exit

# Zip Python files
echo "Zipping Python files..."
zip -r "$LIB_DIR/$SPARK_JOB_ZIP" ./*.py ./data_loader/*.py ./data_writer/*.py ./data_extractor/*.py

# Change back to the original directory
cd - || exit

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
  --py-files "$LIB_DIR/$SPARK_JOB_ZIP" \
  "$SRC_DIR/SparkDataFrameLatencyProcessor.py" \
  --input_data_dir /sandbox/storage/data/ip_cidr_data/dataset/ip_cidr_data_parquet \
  --extract_output_data_dir /sandbox/storage/data/ip_cidr_data/filter_data/pyspark_extracted_data \
  --extract_name find_all_male_person \
  --extract_name count_total_iid_each_state \
  --extract_name top_cities_by_population \
  --extract_name find_persons_with_invalid_emails \
  --extract_name statewise_male_female_count \
  --extract_name top_states_by_persons \
  --extract_name count_unique_ips_per_state \
  --extract_name find_persons_with_valid_emails \
  --extract_name citywise_gender_distribution \
  --extract_name find_people_under_same_public_ip4 \
  --extract_name count_total_iid_each_state