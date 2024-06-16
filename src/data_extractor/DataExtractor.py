from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, avg, row_number
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel


class DataExtractor:
    def __init__(self, person_domain_df: DataFrame):
        self.person_domain_df = person_domain_df

    def find_all_male_person(self) -> DataFrame:
        male_df = self.person_domain_df.filter(lower(col("gender")) == "male")
        male_df.persist(StorageLevel.MEMORY_AND_DISK)
        return male_df

    def count_total_iid_each_state(self) -> DataFrame:
        result_df = self.person_domain_df.groupBy(col("state")).count()
        result_df.persist(StorageLevel.MEMORY_AND_DISK)
        return result_df

    def top_cities_by_population(self, top_n: int = 10) -> DataFrame:
        city_population_df = self.person_domain_df.groupBy(col("city")).count().alias("population")
        window_spec = Window.orderBy(col("count").desc())
        ranked_cities_df = city_population_df.withColumn("rank", row_number().over(window_spec))
        top_cities_df = ranked_cities_df.filter(col("rank") <= top_n)
        top_cities_df.persist(StorageLevel.MEMORY_AND_DISK)
        return top_cities_df

    def find_persons_with_invalid_emails(self) -> DataFrame:
        invalid_email_df = self.person_domain_df.filter(
            ~col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"))
        invalid_email_df.persist(StorageLevel.MEMORY_AND_DISK)
        return invalid_email_df

    def statewise_male_female_count(self) -> DataFrame:
        male_female_count_df = self.person_domain_df.groupBy("state", "gender").count().alias("count")
        pivot_df = male_female_count_df.groupBy("state").pivot("gender").sum("count")
        pivot_df.persist(StorageLevel.MEMORY_AND_DISK)
        return pivot_df

    def run_extraction(self, extracts: list) -> dict:
        results = {}
        for extract in extracts:
            if hasattr(self, extract):
                results[extract] = getattr(self, extract)()
            else:
                print(f"Unknown extraction: {extract}")
        return results
