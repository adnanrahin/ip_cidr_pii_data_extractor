import argparse
from pyspark.sql import SparkSession
from data_loader.DataLoader import IpCidrCustomDomainUserDataLoader
from data_writer.DataFileWriterLocal import DataFileWriterLocal
from data_extractor.DataExtractor import DataExtractor


def main(spark, input_data_dir, extract_output_data_dir, extract_names):
    # Initialize DataLoader and load DataFrame
    person_domain_data_loader = IpCidrCustomDomainUserDataLoader(input_data_dir, spark)
    person_domain_df = person_domain_data_loader.load_df()

    # Initialize DataExtractor with loaded DataFrame
    data_extractor = DataExtractor(person_domain_df)

    # Execute selected extractions
    results = data_extractor.run_extraction(extract_names)

    # Write results to output directory
    for extract_name, df in results.items():
        DataFileWriterLocal.data_writer_parquet(df, extract_output_data_dir, f"spark_df_{extract_name}")


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run Spark DataFrame extractions.")
    parser.add_argument("--input_data_dir", type=str, required=True, help="Path to the source data.")
    parser.add_argument("--extract_output_data_dir", type=str, required=True, help="Path to save the output data.")
    parser.add_argument("--extract_name", action="append", required=True, help="Name of the extraction to run.")

    args = parser.parse_args()

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("IpCidrPIIDataExtractor") \
        .getOrCreate()

    try:
        # Call main function with parsed arguments
        main(spark, args.input_data_dir, args.extract_output_data_dir, args.extract_name)
    finally:
        # Stop SparkSession at the end
        spark.stop()
