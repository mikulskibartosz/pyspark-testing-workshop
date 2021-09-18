from numpy.testing import assert_array_equal


def assert_datasets(expected, actual):
    print('Expected')
    expected.show()

    print('Actual')
    actual.show()

    assert_array_equal(expected.toPandas().values, actual.toPandas().values, verbose=True)
