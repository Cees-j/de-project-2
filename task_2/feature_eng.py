from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import avg, expr, row_number, lag, sum, col

spark = SparkSession.builder.master("local").appName("Moving Average").getOrCreate()

data = spark.read.parquet('task_1/output/final_dataset.parquet')

# Moving Average Window
moving_average_window = Window.partitionBy('Symbol').orderBy(col('Date').cast('timestamp').cast('long')).rangeBetween(-30 * 86400, 0)
data = data.withColumn('vol_moving_avg', avg('Volume').over(moving_average_window))
data.show()


# Rolling Median Window
# rolling_median_window = Window.partitionBy('Symbol').orderBy('Date')


data.show()
