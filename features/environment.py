from behave import *
from pyspark.sql import SparkSession


def before_all(context):
    context.spark = SparkSession.builder \
        .master("local[*]") \
        .appName("behave-with-spark") \
        .getOrCreate()


def after_all(context):
    if 'spark' in context:
        context.spark.sparkContext.stop()