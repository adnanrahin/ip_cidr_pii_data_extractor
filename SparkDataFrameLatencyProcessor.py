import argparse
from pyspark.sql import SparkSession
from dataloader import IpCidrCustomDomainUserDataLoader
from datawriter import DataFileWriterLocal
from extract import run_extraction


def main(data_source_path, data_path, extracts):
    spark = SparkSession.builder \
        .appName("SparkDataFrameLatencyProcessor") \
        .getOrCreate()

    person_domain_data_loader = IpCidrCustomDomainUserDataLoader(data_source_path, spark)
    person_domain_df = person_domain_data_loader.load_df()

    results = run_extraction(person_domain_df, extracts)

    for name, df in results.items():
        DataFileWriterLocal.data_writer(df, data_path, f"spark_df_{name}")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Spark DataFrame extractions.")
    parser.add_argument("data_source_path", type=str, help="Path to the source data.")
    parser.add_argument("data_path", type=str, help="Path to save the output data.")
    parser.add_argument("--extract_name", action="append", help="Name of the extraction to run.")

    args = parser.parse_args()

    if args.extract_name is None:
        print("No extraction functions specified. Exiting.")
    else:
        main(args.data_source_path, args.data_path, args.extract_name)
