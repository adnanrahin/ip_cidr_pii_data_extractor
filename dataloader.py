from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType


class IpCidrCustomDomainUserDataLoader:

    def __init__(self, file_path: str, spark: SparkSession):
        self.file_path = file_path
        self.spark = spark

    def load_df(self) -> DataFrame:
        schema = StructType([
            StructField("firstName", StringType(), True),
            StructField("lastName", StringType(), True),
            StructField("email", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("ipV4", StringType(), True),
            StructField("ipV6", StringType(), True),
            StructField("address", StringType(), True),
            StructField("state", StringType(), True),
            StructField("city", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("guId", StringType(), True),
            StructField("ipV4Cidr", StringType(), True),
            StructField("ipV6Cidr", StringType(), True)
        ])

        person_domain_df = self.spark.read \
            .option("header", "true") \
            .option("delimiter", "\t") \
            .schema(schema) \
            .parquet(self.file_path)

        return person_domain_df
