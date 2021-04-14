from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local').appName('Spark Write csv app').getOrCreate()

orders = spark.read.option('Header','True').csv('/Users/jayarajasekar/Documents/output/op.csv')

print(orders.count())