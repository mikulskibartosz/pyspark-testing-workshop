from datetime import datetime
from .schema import test_schema


def to_date(order_date):
    return datetime.strptime(order_date, '%Y-%m-%d')


class DatasetBuilder:
    def __init__(self):
        self.rows = []

    def add_row(self,
                order_id: int, product_name: str, pieces_sold: int,
                price_per_item: float, order_date: str, shop_id: str
                ):
        order_date = to_date(order_date)
        self.rows.append([order_id, product_name, pieces_sold, float(price_per_item), order_date, shop_id])
        return self

    def dataset(self, spark_session):
        return spark_session.createDataFrame(self.rows, schema=test_schema)
