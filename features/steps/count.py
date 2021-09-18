from behave import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DateType, DoubleType
from pyspark.sql.functions import col, split, trim
from tests.spark.build_dataset import DatasetBuilder, to_date
from tests.spark.assert_df import assert_datasets


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


@given(u'a dataset of purchases')
def step_impl(context):
    builder = DatasetBuilder()
    for index, row in enumerate(context.table):
        pieces_sold = row['pieces_sold']
        if pieces_sold:
            pieces_sold = int(pieces_sold)
        else:
            pieces_sold = None
        builder.add_row(
            order_id=int(row['order_id']),
            product_name=row['product_name'],
            pieces_sold=pieces_sold,
            price_per_item=float(row['price_per_item']),
            order_date=row['order_date'],
            shop_id=row['shop_id']
        )
    context.data = builder.dataset(context.spark)


@when(u'we count the products')
def step_impl(context):
    context.result = count_product(context.data)


@then(u'the number of products is "{products:d}"')
def step_impl(context, products):
    assert context.result == products


@when(u'we count the products sold per shop')
def step_impl(context):
    context.result = count_products_per_shop(context.data).orderBy('shop_id')


@then(u'the result dataset should equal')
def step_impl(context):
    transformations = {
        'str': lambda x: x,
        'long': lambda x: int(x),
        'date': lambda x: to_date(x),
        'double': lambda x: float(x)
    }
    types = {
        'str': StringType(),
        'long': LongType(),
        'date': DateType(),
        'double': DoubleType()
    }
    fields = []
    for key in context.table.headings:
        name, type_name = key.split('-')
        fields.append(StructField(name.strip(), types[type_name.strip()]))

    values = []
    for index, row in enumerate(context.table):
        single_row = []
        for key in context.table.headings:
            _, type_name = key.split('-')
            value = transformations[type_name.strip()](row[key])
            single_row.append(value)
        values.append(single_row)

    expected_dataset = context.spark.createDataFrame(values, schema=StructType(fields))
    assert_datasets(expected_dataset, context.result)


@when(u'we look for the most popular product')
def step_impl(context):
    context.result = find_most_popular(context.data)


@then(u'the product name should be "{product}"')
def step_impl(context, product):
    assert context.result == product, f'{context.result} == {product}'


@when(u'we look for the most popular category')
def step_impl(context):
    context.result = find_most_popular_category(context.data)


@then(u'the category name should be "{category}"')
def step_impl(context, category):
    assert context.result == category, f'{context.result} == {category}'


@when(u'we look for the number of unique products sold per day')
def step_impl(context):
    context.result = find_unique_products_per_day(context.data)


@when(u'we look for the average purchase price')
def step_impl(context):
    context.result = find_average_purchase_price(context.data)


@then(u'the average price should be "{price:d}"')
def step_impl(context, price):
    assert context.result == price, f'{context.result} == {price}'


@when(u'we look for the average income per shop per day')
def step_impl(context):
    context.result = find_average_income_per_shop_per_day(context.data)


@when(u'we look for the shop that does not include the number of products')
def step_impl(context):
    context.result = find_who_skips_number_of_products_most_often(context.data)


@then(u'the shop name should be "{shop}"')
def step_impl(context, shop):
    assert context.result == shop, f'{context.result} == {shop}'


@when(u'we look for the day when the largest number of products was sold')
def step_impl(context):
    context.result = find_day_with_most_purchases(context.data)


@then(u'the date should be "{date}"')
def step_impl(context, date):
    assert context.result == to_date(date).date(), f'{context.result} == {date}'
