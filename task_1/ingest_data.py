from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract


spark = SparkSession.builder.master("local").appName("CombineStockEtf").getOrCreate()
schema = "Symbol STRING, Security_Name STRING, Date STRING, Open FLOAT, High FLOAT, Low FLOAT, Close FLOAT, Adj_Close FLOAT, Volume INT"
final_dataset = spark.createDataFrame([], schema)


stock_df = spark.read.csv("task_1/stocks/*.csv", header=True).withColumn("input_file", input_file_name())
etf_df = spark.read.csv("task_1/etfs/*.csv", header=True).withColumn("input_file", input_file_name())
lookup_df = spark.read.csv("task_1/symbols_valid_meta.csv", header=True)  # Assuming it has columns: Symbol and Security Name


stock_df = stock_df.withColumnRenamed("Adj Close", "Adj_Close")
etf_df = etf_df.withColumnRenamed("Adj Close", "Adj_Close")
lookup_df = lookup_df.withColumnRenamed("Security Name", "Security_Name")


symbol_pattern = r".*/(.*).csv"
stock_df = stock_df.withColumn("Symbol", regexp_extract("input_file", symbol_pattern, 1))
etf_df = etf_df.withColumn("Symbol", regexp_extract("input_file", symbol_pattern, 1))


stock_df = stock_df.drop("input_file")
etf_df = etf_df.drop("input_file")


combined_df = stock_df.union(etf_df)
final_df = combined_df.join(lookup_df, on='Symbol', how='inner')


final_dataset = final_df.select("Symbol", "Security_Name", "Date", "Open", "High", "Low", "Close", "Adj_Close", "Volume")


# final_dataset.write.csv("task_1/output/final_dataset.csv", mode="overwrite", header=True)
# final_dataset.write.parquet("task_1/output/final_dataset.parquet", mode="overwrite")

print('dtypes', final_dataset.dtypes)
