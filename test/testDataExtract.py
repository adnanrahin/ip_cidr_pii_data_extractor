import unittest
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import Row
from src.data_extractor.DataExtractor import DataExtractor


class TestDataExtractor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize a Spark session
        cls.spark = SparkSession.builder.master("local").appName("DataExtractorTest").getOrCreate()

        # Sample data matching the provided schema
        cls.sample_data = [
            Row(firstName="Gray", lastName="McElvine", email="gmcelvine1@apple.com", gender="Male",
                ipV4="91.96.204.252", ipV6="c948:4707:471e:3126:92de:d23:10df:7815",
                address="7 Comanche Point", state="California", city="Inglewood", longitude=-118.3258, latitude=33.9583,
                guId="5dd073d0-c9c7-4014-a39f-587ba71720ea", ipV4Cidr="172.58.36.112/9",
                ipV6Cidr="22f:c4a0:f6ca:e189:1685:d203:3121:824b/1"),
            Row(firstName="Lillian", lastName="Crossfeld", email="lcrossfeld2@europa.eu", gender="Female",
                ipV4="203.58.118.75", ipV6="8cd4:fb1c:c444:3d51:7434:8af5:4bdb:bf60",
                address="41 Birchwood Parkway", state="Virginia", city="Alexandria", longitude=-77.1073,
                latitude=38.7192,
                guId="0b5c516e-ff89-4d39-b780-255cb6bec9c2", ipV4Cidr="134.219.136.116/30",
                ipV6Cidr="7d17:c2b2:d8bb:3640:3048:3cae:aaad:8c07/127"),
            Row(firstName="Donelle", lastName="Forri", email="dforri3@marriott.com", gender="Female",
                ipV4="238.236.189.58", ipV6="ba9f:8bfc:b368:34c7:b5a9:2556:7127:df29",
                address="485 Huxley Avenue", state="California", city="Sacramento", longitude=-121.4444,
                latitude=38.3774,
                guId="8e2f610d-0f0f-493d-875c-6c2306c72b93", ipV4Cidr="10.162.239.102/19",
                ipV6Cidr="4029:b268:6c28:c655:1a7b:1556:7a18:d00f/97"),
            Row(firstName="Yankee", lastName="Jannex", email="yjannex4@etsy.com", gender="Male",
                ipV4="199.4.136.89", ipV6="9164:e38:f02b:ea02:c959:ea79:e036:2517",
                address="82535 Dixon Pass", state="Texas", city="Dallas", longitude=-96.7776, latitude=32.7673,
                guId="c67138c6-16b4-48e2-a04d-24214c2e684b", ipV4Cidr="14.30.89.107/24",
                ipV6Cidr="9fcf:8d6e:d457:819a:efc2:e582:b8b:8157/39")
        ]

        # Create DataFrame from sample data
        cls.sample_df = cls.spark.createDataFrame(cls.sample_data)
        cls.data_extractor = DataExtractor(cls.sample_df)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_find_all_male_person(self):
        # Call the method
        male_df = self.data_extractor.find_all_male_person()

        # Collect the results to verify
        male_data = male_df.collect()

        # Check that the returned DataFrame only contains male rows
        self.assertTrue(all(row.gender.lower() == "male" for row in male_data))

        # Check the number of rows matches expected
        self.assertEqual(len(male_data), 2)  # Only "Gray McElvine" and "Yankee Jannex" are male in sample data

    def test_count_total_iid_each_state(self):
        # Call the method
        state_count_df = self.data_extractor.count_total_iid_each_state()

        # Collect the results to verify
        state_count_data = {row.state: row["count"] for row in state_count_df.collect()}

        # Check the counts per state
        self.assertEqual(state_count_data["California"], 2)  # 2 entries for California in sample data
        self.assertEqual(state_count_data["Virginia"], 1)  # 1 entry for Virginia
        self.assertEqual(state_count_data["Texas"], 1)  # 1 entry for Texas


if __name__ == "__main__":
    unittest.main()
