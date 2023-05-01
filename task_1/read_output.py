from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read Parquet and Csv File") \
    .getOrCreate()


parquet_file = "task_1/output/final_dataset.parquet"
df = spark.read.parquet(parquet_file)

sample = df.sample(False, 0.01)
x = sample.collect()
print(x)


