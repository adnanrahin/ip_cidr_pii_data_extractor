from pyspark.sql import DataFrame


class DataFileWriterLocal:

    @staticmethod
    def data_writer_parquet(data_frame: DataFrame, data_path: str, directory_name: str) -> None:
        """
        Writes DataFrame to specified directory in Parquet format.

        Args:
        - data_frame (DataFrame): The DataFrame to write.
        - data_path (str): The base directory path where data should be written.
        - directory_name (str): The name of the directory within data_path to write to.
        """
        destination_directory = f"{data_path}/{directory_name}"
        data_frame.write.mode("overwrite").parquet(destination_directory)

    @staticmethod
    def data_writer_csv(data_frame: DataFrame, data_path: str, directory_name: str) -> None:
        """
        Writes DataFrame to specified directory in CSV format.

        Args:
        - data_frame (DataFrame): The DataFrame to write.
        - data_path (str): The base directory path where data should be written.
        - directory_name (str): The name of the directory within data_path to write to.
        """
        destination_directory = f"{data_path}/{directory_name}"
        data_frame.write.mode("overwrite").csv(destination_directory)

    @staticmethod
    def data_writer_json(data_frame: DataFrame, data_path: str, directory_name: str) -> None:
        """
        Writes DataFrame to specified directory in JSON format.

        Args:
        - data_frame (DataFrame): The DataFrame to write.
        - data_path (str): The base directory path where data should be written.
        - directory_name (str): The name of the directory within data_path to write to.
        """
        destination_directory = f"{data_path}/{directory_name}"
        data_frame.write.mode("overwrite").json(destination_directory)
