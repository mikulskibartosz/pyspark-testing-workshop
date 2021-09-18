from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

test_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("product_name", StringType()),
    StructField("pieces_sold", IntegerType()),
    StructField("price_per_item", DoubleType()),
    StructField("order_date", DateType()),
    StructField("shop_id", StringType()),
])