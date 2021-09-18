import pytest
from spark.schema import test_schema


pytestmark = pytest.mark.usefixtures("spark_session")


def test_load_dataset(spark_session):
    data = spark_session.read.format("csv") \
        .option("header", "true") \
        .schema(test_schema) \
        .load("MOCK_DATA.csv")

    data.show()

    assert data.count() == 1000
