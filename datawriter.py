from pyspark.sql import DataFrame


class DataFileWriterLocal:

    @staticmethod
    def data_writer(data_frame: DataFrame, data_path: str, directory_name: str) -> None:
        destination_directory = f"{data_path}/{directory_name}"
        data_frame.write.mode("overwrite").parquet(destination_directory)
