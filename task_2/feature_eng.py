from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import avg, expr, col, round
from pyspark.sql.types import FloatType, IntegerType


spark = SparkSession.builder.master("local").appName("Moving Average").getOrCreate()
data = spark.read.parquet('task_1/output/final_dataset.parquet')


data = data.withColumn("Open", col("Open").cast(FloatType()))
data = data.withColumn("High", col("High").cast(FloatType()))
data = data.withColumn("Low", col("Low").cast(FloatType()))
data = data.withColumn("Close", col("Close").cast(FloatType()))
data = data.withColumn("Adj_Close", col("Adj_Close").cast(FloatType()))
data = data.withColumn("Volume", col("Volume").cast(IntegerType()))


# Moving Average Window
moving_average_window = Window.partitionBy('Symbol').orderBy(col('Date').cast('timestamp').cast('long')).rangeBetween(-30 * 86400, 0)
data = data.withColumn('vol_moving_avg', avg('Volume').over(moving_average_window))
data.show()


# Rolling Median Window
rolling_median_window = Window.partitionBy('Symbol').orderBy(col('Date').cast('timestamp').cast('long')).rangeBetween(-30 * 86400, 0)
median_percentile = expr('percentile_approx(Adj_Close, 0.5)')
rolling_med_data = data.withColumn('med_val', median_percentile.over(rolling_median_window))
rolling_med_data.show()

# Round values 
for c_name, c_type in data.dtypes:
    if c_type == 'float':
        data = data.withColumn(c_name, round(col(c_name),4))


data.write.parquet("task_2/output/final_dataset.parquet", mode="overwrite")

