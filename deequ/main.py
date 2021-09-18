from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, split, trim, mean, stddev

import pydeequ
from pydeequ.analyzers import *
from pydeequ.checks import *
from pydeequ.verification import *


schema = StructType([ \
    StructField("order_id", IntegerType(), True), \
    StructField("product_name", StringType(), True), \
    StructField("pieces_sold", IntegerType(), False), \
    StructField("price_per_item", DoubleType(), True), \
    StructField("order_date", DateType(), True), \
    StructField("shop_id", StringType(), False) \
  ])

spark = (SparkSession
    .builder
    .config("spark.jars.packages", pydeequ.deequ_maven_coord)
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
    .getOrCreate())

df = spark.read.csv("MOCK_DATA.csv",  header=True, schema=schema)


check = Check(spark, CheckLevel.Warning, "Review Check")

checkResult = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(
        check.hasSize(lambda x: x == 1000)
        .isComplete('shop_id')
        .hasMax('pieces_sold', lambda x: x == 15)
        .hasMaxLength('product_name', lambda x: x == 20)
        .hasMinLength('product_name', lambda x: x == 1)
        .hasUniqueness(['product_name'], lambda x: x >= 0.9)
    ) \
    .run()

checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.show()

print(checkResult_df.select('constraint_message').collect())

# Sprawdź czy istneiją zamówienia w wybranej przez siebie kategorii.
# Sprawdź czy istnieją zamówienia których cena pojedynczego przedmiotu jest większa niż: średnia cena przedmiotu + dwukrotność odchylenia standardowego

price_mean, price_std = df.select(
    mean(col('price_per_item')).alias('mean'),
    stddev(col('price_per_item')).alias('std')
).collect()[0]

print(price_mean)
print(price_std)

dataset_to_check = df.withColumn('category', trim(split(df['product_name'], '-').getItem(0)))

check = Check(spark, CheckLevel.Warning, "Review Check")

checkResult = VerificationSuite(spark) \
    .onData(dataset_to_check) \
    .addCheck(
        check
        .isContainedIn('category', ['Tea'])
        .hasMax('pieces_sold', lambda x: x == price_mean + (2*price_std))
    ) \
    .run()

checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.show()

print(checkResult_df.select('constraint_message').collect())

spark.sparkContext._gateway.shutdown_callback_server()