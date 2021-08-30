from pyspark.sql import SparkSession, Row
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

df = spark.read.csv("../MOCK_DATA.csv",  header=True, schema=schema)

df.show()

analysisResult = AnalysisRunner(spark) \
                    .onData(df) \
                    .addAnalyzer(Size()) \
                    .addAnalyzer(Completeness("product_name")) \
                    .addAnalyzer(Completeness("pieces_sold")) \
                    .run()

analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
analysisResult_df.show()

check = Check(spark, CheckLevel.Warning, "Review Check")

checkResult = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(
        check.hasSize(lambda x: x >= 3) \
        .hasMin("price_per_item", lambda x: x == 0) \
        .isComplete("pieces_sold")  \
        .isUnique("product_name")  \
        .isContainedIn("shop_id", ["shop_1", "shop_2", "shop_3"]) \
        .isNonNegative("price_per_item")) \
    .run()

checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.show()