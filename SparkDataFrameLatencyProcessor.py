import argparse
from pyspark.sql import SparkSession
from dataloader import IpCidrCustomDomainUserDataLoader
from datawriter import DataFileWriterLocal
from extract import run_extraction


def main(input_data_dir, extract_output_data_dir, extracts):
    spark = SparkSession.builder \
        .appName("SparkDataFrameLatencyProcessor") \
        .getOrCreate()

    person_domain_data_loader = IpCidrCustomDomainUserDataLoader(input_data_dir, spark)
    person_domain_df = person_domain_data_loader.load_df()

    results = run_extraction(person_domain_df, extracts)

    for name, df in results.items():
        DataFileWriterLocal.data_writer(df, extract_output_data_dir, f"spark_df_{name}")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Spark DataFrame extractions.")
    parser.add_argument("--input_data_dir", type=str, required=True, help="Path to the source data.")
    parser.add_argument("--extract_output_data_dir", type=str, required=True, help="Path to save the output data.")
    parser.add_argument("--extract_name", action="append", required=True, help="Name of the extraction to run.")

    args = parser.parse_args()

    if not args.extract_name:
        print("No extraction functions specified. Exiting.")
    else:
        main(args.input_data_dir, args.extract_output_data_dir, args.extract_name)
