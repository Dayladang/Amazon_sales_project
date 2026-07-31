
import pyspark
import os 
from pyspark.sql import SparkSession, types
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

credentials_location = '/D:/Apps/Google_Cloud_SDK/service-account-key/Dang/project-511c440a-e4ba-41b7-b08-a0d6b5fece0b.json'

conf = (
    SparkConf()
    .setMaster('local[*]')
    .setAppName('silver_to_BigQuery')

    # thêm các thư viện kết nối kèm và đường dẫn tạm thời cho Spark
    .set("spark.jars", "./../lib/gcs-connector-hadoop3-2.2.5.jar,./../lib/spark-4.1-bigquery-0.44.2-preview.jar") \
    .set("spark.local.dir", "D:/Documents/coding_stuff/python_nerd/pipeline/spark/tmp") \

    # Cấu hình GCS
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)

    # Cấu hình BigQuery
    .set("spark.datasource.bigquery.temporaryGcsBucket", "dang_bucket_1")
    .set("spark.datasource.bigquery.parentProject", "project-511c440a-e4ba-41b7-b08")
    .set("spark.datasource.bigquery.credentialsFile", credentials_location)
)

# os.environ['HADOOP_HOME'] = 'D:/Apps/hadoop'

sc = SparkContext(conf=conf).getOrCreate()

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

df_amazon = spark.read.csv('gs://dang_bucket_1/bronze/*', header=True, inferSchema=True)

df_pd = df_amazon.toPandas()

spark.createDataFrame(df_pd).schema

my_schema = types.StructType([
    types.StructField('OrderID', types.StringType(), True), 
    types.StructField('OrderDate', types.DateType(), True), 
    types.StructField('CustomerID', types.StringType(), True), 
    types.StructField('CustomerName', types.StringType(), True), 
    types.StructField('ProductID', types.StringType(), True), 
    types.StructField('ProductName', types.StringType(), True), 
    types.StructField('Category', types.StringType(), True), 
    types.StructField('Brand', types.StringType(), True), 
    types.StructField('Quantity', types.LongType(), True), 
    types.StructField('UnitPrice', types.DoubleType(), True), 
    types.StructField('Discount', types.DoubleType(), True), 
    types.StructField('Tax', types.DoubleType(), True), 
    types.StructField('ShippingCost', types.DoubleType(), True), 
    types.StructField('TotalAmount', types.DoubleType(), True), 
    types.StructField('PaymentMethod', types.StringType(), True), 
    types.StructField('OrderStatus', types.StringType(), True), 
    types.StructField('City', types.StringType(), True), 
    types.StructField('State', types.StringType(), True), 
    types.StructField('Country', types.StringType(), True), 
    types.StructField('SellerID', types.StringType(), True)
])

df_amazon = spark.read \
    .option("header", "true") \
    .schema(my_schema) \
    .csv('gs://dang_bucket_1/bronze/*')

df_amazon.columns

df_amazon = df_amazon.na.drop()

# push to warehouse
(
    df_amazon.write
    .format("bigquery")
    .option("table", "project-511c440a-e4ba-41b7-b08.amazon_sales.silver_amazon_sales")
    .mode("append")
    .save()
)

