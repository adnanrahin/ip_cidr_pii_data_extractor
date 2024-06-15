from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, avg, row_number
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel


def find_all_male_person(person_domain_df: DataFrame) -> DataFrame:
    male_df = person_domain_df.filter(lower(col("gender")) == "male")
    male_df.persist(StorageLevel.MEMORY_AND_DISK)
    return male_df


def count_total_iid_each_state(person_domain_df: DataFrame) -> DataFrame:
    result_df = person_domain_df.groupBy(col("state")).count()
    result_df.persist(StorageLevel.MEMORY_AND_DISK)
    return result_df


def average_age_each_state(person_domain_df: DataFrame) -> DataFrame:
    result_df = person_domain_df.groupBy(col("state")).agg(avg(col("age")).alias("average_age"))
    result_df.persist(StorageLevel.MEMORY_AND_DISK)
    return result_df


def top_cities_by_population(person_domain_df: DataFrame, top_n: int = 10) -> DataFrame:
    city_population_df = person_domain_df.groupBy(col("city")).count().alias("population")
    window_spec = Window.orderBy(col("count").desc())
    ranked_cities_df = city_population_df.withColumn("rank", row_number().over(window_spec))
    top_cities_df = ranked_cities_df.filter(col("rank") <= top_n)
    top_cities_df.persist(StorageLevel.MEMORY_AND_DISK)
    return top_cities_df


def find_persons_with_invalid_emails(person_domain_df: DataFrame) -> DataFrame:
    invalid_email_df = person_domain_df.filter(~col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"))
    invalid_email_df.persist(StorageLevel.MEMORY_AND_DISK)
    return invalid_email_df


def statewise_male_female_count(person_domain_df: DataFrame) -> DataFrame:
    male_female_count_df = person_domain_df.groupBy("state", "gender").count().alias("count")
    pivot_df = male_female_count_df.groupBy("state").pivot("gender").sum("count")
    pivot_df.persist(StorageLevel.MEMORY_AND_DISK)
    return pivot_df


EXTRACTION_FUNCTIONS = {
    "find_all_male_person": find_all_male_person,
    "count_total_iid_each_state": count_total_iid_each_state,
    "average_age_each_state": average_age_each_state,
    "top_cities_by_population": top_cities_by_population,
    "find_persons_with_invalid_emails": find_persons_with_invalid_emails,
    "statewise_male_female_count": statewise_male_female_count
}


def run_extraction(person_domain_df: DataFrame, extracts: list) -> dict:
    results = {}
    for extract in extracts:
        if extract in EXTRACTION_FUNCTIONS:
            results[extract] = EXTRACTION_FUNCTIONS[extract](person_domain_df)
        else:
            print(f"Unknown extraction: {extract}")
    return results
