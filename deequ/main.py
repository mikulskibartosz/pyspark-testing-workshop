from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, DateType

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
        check.hasSize(lambda x: x == 1000)) \
    .run()

checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.show()

spark.sparkContext._gateway.shutdown_callback_server()