import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DateType, DoubleType
from pyspark.sql.functions import col, split, trim
from spark.build_dataset import DatasetBuilder, to_date
from spark.assert_df import assert_datasets


pytestmark = pytest.mark.usefixtures("spark_session")


def count_product(dataset):
    return dataset.select(['product_name']).distinct().count()


def count_products_per_shop(dataset):
    return dataset.groupby('shop_id').count()


def find_most_popular(dataset):
    return dataset.groupby('product_name').count().orderBy(col('count').desc()).first()[0]


def find_most_popular_category(dataset):
    result = dataset \
        .withColumn('category', trim(split(dataset['product_name'], '-').getItem(0))) \
        .where(col('product_name').contains('-')) \
        .groupby('category').count() \
        .orderBy(col('count').desc()) \
        .select('category')
    result.show()
    return result.first()[0]


def find_unique_products_per_day(dataset):
    return dataset.select(['product_name', 'order_date']).distinct() \
        .groupby('order_date').count() \
        .orderBy(col('order_date'))


def find_average_purchase_price(dataset):
    return dataset.na.fill({'pieces_sold': 1}) \
        .withColumn('purchase', col('pieces_sold') * col('price_per_item')) \
        .select('purchase').agg({'purchase': 'avg'}).first()[0]


def find_average_income_per_shop_per_day(dataset):
    return dataset.na.fill({'pieces_sold': 1}) \
        .withColumn('purchase', col('pieces_sold') * col('price_per_item')) \
        .groupby('shop_id', 'order_date').sum('purchase') \
        .groupby('shop_id').avg('sum(purchase)') \
        .orderBy(col('shop_id'))


def find_who_skips_number_of_products_most_often(dataset):
    return dataset.filter(col('pieces_sold').isNull()) \
        .groupby('shop_id').count() \
        .orderBy(col('count').desc()) \
        .select('shop_id')\
        .first()[0]


def find_day_with_most_purchases(dataset):
    return dataset.na.fill({'pieces_sold': 1}) \
        .groupby('order_date').sum('pieces_sold') \
        .orderBy(col('sum(pieces_sold)').desc()) \
        .select('order_date') \
        .first()[0]


def test_count_products(spark_session):
    dataset = DatasetBuilder() \
        .add_row(1, 'product A', 1, 1., '2021-09-19', 'shop_A') \
        .add_row(2, 'product B', 1, 1., '2021-09-19', 'shop_A') \
        .add_row(3, 'product B', 1, 1., '2021-09-19', 'shop_A') \
        .dataset(spark_session)

    dataset.show()

    counted_products = count_product(dataset)

    assert counted_products == 2


def test_count_products_per_shop(spark_session):
    dataset = DatasetBuilder() \
        .add_row(1, 'product A', 1, 1., '2021-09-19', 'shop_A') \
        .add_row(2, 'product B', 1, 1., '2021-09-19', 'shop_A') \
        .add_row(3, 'product B', 1, 1., '2021-09-19', 'shop_B') \
        .add_row(4, 'product C', 1, 1., '2021-09-19', 'shop_B') \
        .add_row(5, 'product A', 1, 1., '2021-09-19', 'shop_B') \
        .dataset(spark_session)

    expected_dataset = spark_session.createDataFrame([
        ['shop_A', 2],
        ['shop_B', 3]
    ], schema=StructType([
        StructField("shop_id", StringType()),
        StructField("count", LongType())
    ]))

    products_per_shop = count_products_per_shop(dataset).orderBy('shop_id')

    assert_datasets(products_per_shop, expected_dataset)


def test_find_most_popular_product(spark_session):
    dataset = DatasetBuilder() \
        .add_row(2, 'product B', 1, 1., '2021-09-19', 'shop_A') \
        .add_row(1, 'product A', 1, 1., '2021-09-20', 'shop_A') \
        .add_row(1, 'product A', 1, 1., '2021-09-19', 'shop_A') \
        .add_row(3, 'product B', 1, 1., '2021-09-19', 'shop_B') \
        .add_row(4, 'product C', 1, 1., '2021-09-19', 'shop_B') \
        .add_row(5, 'product A', 1, 1., '2021-09-19', 'shop_B') \
        .dataset(spark_session)

    most_popular_product = find_most_popular(dataset)

    assert most_popular_product == 'product A'


def test_find_most_popular_category(spark_session):
    dataset = DatasetBuilder() \
        .add_row(2, 'Coffee - Frthy Coffee Crisp', 1, 1., '2021-09-19', 'shop_A') \
        .add_row(1, 'Coffee - Some Other Coffee', 1, 1., '2021-09-19', 'shop_A') \
        .add_row(3, 'Tea - Banana Green Tea', 1, 1., '2021-09-19', 'shop_B') \
        .add_row(4, 'Tea - Apple Green Tea', 1, 1., '2021-09-19', 'shop_B') \
        .add_row(5, 'Tea - Apple Green Tea', 1, 1., '2021-09-19', 'shop_B') \
        .add_row(6, 'Bar Bran Honey Nut', 1, 1., '2021-09-19', 'shop_B') \
        .dataset(spark_session)

    most_popular_category = find_most_popular_category(dataset)

    assert most_popular_category == 'Tea'


def test_count_unique_products_per_day(spark_session):
    dataset = DatasetBuilder() \
        .add_row(2, 'product A', 1, 1., '2021-09-19', 'shop_A') \
        .add_row(1, 'product A', 1, 1., '2021-09-19', 'shop_A') \
        .add_row(3, 'product B', 1, 1., '2021-09-19', 'shop_B') \
        .add_row(4, 'product C', 1, 1., '2021-09-20', 'shop_B') \
        .add_row(5, 'product A', 1, 1., '2021-09-20', 'shop_B') \
        .dataset(spark_session)

    expected_dataset = spark_session.createDataFrame([
        [to_date('2021-09-19'), 2],
        [to_date('2021-09-20'), 2]
    ], schema=StructType([
        StructField("order_date", DateType()),
        StructField("count", LongType())
    ]))

    unique_products_per_day = find_unique_products_per_day(dataset)

    assert_datasets(unique_products_per_day, expected_dataset)


def test_average_purchase_price(spark_session):
    dataset = DatasetBuilder() \
        .add_row(2, 'product A', 3, 5., '2021-09-19', 'shop_A') \
        .add_row(1, 'product A', None, 5., '2021-09-19', 'shop_A') \
        .add_row(3, 'product B', None, 10., '2021-09-19', 'shop_B') \
        .dataset(spark_session)

    average_price = find_average_purchase_price(dataset)

    assert average_price == 10


def test_average_income_per_shop_per_day(spark_session):
    dataset = DatasetBuilder() \
        .add_row(1, 'product A', 5, 10., '2021-09-19', 'shop_A') \
        .add_row(2, 'product B', None, 10., '2021-09-19', 'shop_A') \
        .add_row(3, 'product B', None, 30., '2021-09-19', 'shop_B') \
        .add_row(4, 'product C', 3, 10., '2021-09-19', 'shop_B') \
        .add_row(5, 'product A', 1, 2., '2021-09-20', 'shop_B') \
        .dataset(spark_session)

    expected_dataset = spark_session.createDataFrame([
        ['shop_A', 60.0],
        ['shop_B', 31.0]
    ], schema=StructType([
        StructField("shop_id", StringType()),
        StructField("avg", DoubleType())
    ]))

    result = find_average_income_per_shop_per_day(dataset)

    assert_datasets(expected_dataset, result)


def test_who_skips_number_of_products(spark_session):
    dataset = DatasetBuilder() \
        .add_row(1, 'product A', None, 10., '2021-09-19', 'shop_A') \
        .add_row(2, 'product B', None, 10., '2021-09-19', 'shop_A') \
        .add_row(3, 'product B', None, 30., '2021-09-19', 'shop_B') \
        .add_row(4, 'product C', 3, 10., '2021-09-19', 'shop_B') \
        .add_row(5, 'product A', 1, 2., '2021-09-20', 'shop_B') \
        .dataset(spark_session)

    result = find_who_skips_number_of_products_most_often(dataset)

    assert result == 'shop_A'


def test_max_products_per_day(spark_session):
    dataset = DatasetBuilder() \
        .add_row(1, 'product A', None, 10., '2021-09-19', 'shop_A') \
        .add_row(2, 'product B', None, 10., '2021-09-19', 'shop_A') \
        .add_row(3, 'product B', None, 30., '2021-09-19', 'shop_B') \
        .add_row(4, 'product C', 1, 10., '2021-09-20', 'shop_B') \
        .add_row(5, 'product A', 1, 2., '2021-09-20', 'shop_B') \
        .dataset(spark_session)

    result = find_day_with_most_purchases(dataset)

    assert result == to_date('2021-09-19').date()
