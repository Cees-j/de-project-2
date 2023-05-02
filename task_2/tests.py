import unittest
from pyspark.sql import SparkSession, Row
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, collect_list, col, round, rand
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import random
from datetime import datetime, timedelta

class TestMovingAverage(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("Moving Average Test") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_moving_average(self):
        # Sample data
        data = [
            Row(Symbol='AAPL', Date='2021-01-01', Volume=100),
            Row(Symbol='AAPL', Date='2021-01-02', Volume=200),
            Row(Symbol='AAPL', Date='2021-01-03', Volume=300),
            Row(Symbol='AAPL', Date='2021-01-04', Volume=400),
            Row(Symbol='AAPL', Date='2021-01-30', Volume=500),
            Row(Symbol='AAPL', Date='2021-02-01', Volume=1000),
            Row(Symbol='AAPL', Date='2021-02-23', Volume=2000)
        ]

        expected_moving_average = (100.0, 150.0, 200.0, 250.0, 300.0, 480, 1166.67)
        # Create a DataFrame from the sample data
        data_df = self.spark.createDataFrame(data)

        # Calculate the moving average
        moving_average_window = Window.partitionBy('Symbol').orderBy(col('Date').cast('timestamp').cast('long')).rangeBetween(-30 * 86400, 0)
        result_df = data_df.withColumn('vol_moving_avg', round(avg('Volume').over(moving_average_window),2))

        # Format values into tuple
        actual_moving_averages = result_df.select('vol_moving_avg')
        rows = actual_moving_averages.collect()
        data_list = [row.asDict() for row in rows]
        numeric_values_list = [value for dct in data_list for value in dct.values() if isinstance(value, (int, float))]


        self.assertEqual(tuple(numeric_values_list), expected_moving_average)


    def test_moving_average_larger_date_set(self):
        base_date = datetime.strptime("2021-01-01", "%Y-%m-%d")
        data = []

        # TO DO calculate these averages
        expected_moving_average = (10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0,
                                    70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 100.0, 105.0, 110.0, 115.0, 120.0,
                                    125.0, 130.0, 135.0, 140.0, 145.0, 150.0, 155.0, 160.5, 166.0, 171.5,
                                    177.0, 182.5, 188.0, 193.5, 199.0, 204.5, 210.0)
        # Create large data
        for i in range(40):
            date = (base_date + timedelta(days=i)).strftime("%Y-%m-%d")
            volume = (i + 1) * 10
            data.append(Row(Symbol='AAPL', Date=date, Volume=volume))

        print(data)
        df = self.spark.createDataFrame(data)
        df.show(40)

        # Apply actual logic
        moving_average_window = Window.partitionBy('Symbol').orderBy(col('Date').cast('timestamp').cast('long')).rangeBetween(-30 * 86400, 0)
        result_df = df.withColumn('vol_moving_avg', round(avg('Volume').over(moving_average_window),2))

        # Format values into tuple
        actual_moving_averages = result_df.select('vol_moving_avg')
        rows = actual_moving_averages.collect()
        data_list = [row.asDict() for row in rows]
        numeric_values_list = [value for dct in data_list for value in dct.values() if isinstance(value, (int, float))]

        print('PRINTING', tuple(numeric_values_list))

        self.assertEqual(tuple(numeric_values_list), expected_moving_average)


if __name__ == '__main__':
    unittest.main()
