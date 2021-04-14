from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local').appName('Spark Write csv app').getOrCreate()

orders = spark.read.format('csv').schema('order_id int, order_date date, customer_id int, order_status String') \
    .load('/Users/jayarajasekar/Documents/data-master/retail_db/orders/part-00000')

orders.write.option('Header','True').csv('/Users/jayarajasekar/Documents/output/op.csv')

