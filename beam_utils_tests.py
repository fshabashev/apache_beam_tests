import apache_beam as beam
from beam_utils import aggregate_by_key, outer_join


def test_aggregate_by_key():
    # test aggregate_by_key function
    with beam.Pipeline() as pipeline:
        rows_ds1 = pipeline | 'Read CSV 1' >> beam.Create([{'a': 1, 'b': 1}, {'a': 1, 'b': 2}, {'a': 2, 'b': 3}])
        aggregated_rows = aggregate_by_key(['a', 'a'], {'b': lambda x, y: x+y}, rows_ds1)
        aggregated_rows | 'Print Output' >> beam.Map(print)


def test_outer_join1():
    # test outer_join function
    with beam.Pipeline() as pipeline:
        rows_ds1 = pipeline | 'Read CSV 1' >> beam.Create([{'a': 1, 'b': 1}])
        rows_ds2 = pipeline | 'Read CSV 2' >> beam.Create([{'a': 1, 'c': 5}])
        joined = outer_join(rows_ds1, rows_ds2, ['a'], ['a'])
        joined | 'Print Output' >> beam.Map(print)


def test_outer_join2():
    # test outer_join function
    with beam.Pipeline() as pipeline:
        test_data1 = pipeline | 'Create test data 2' >> beam.Create([{'a': 2, 'b': 2},
                                                                     {'a': 3, 'b': 4}])

        test_data2 = pipeline | 'Create test data zxcvzs' >> beam.Create([{'a': 1, 'c': 5},
                                                                          {'a': 1, 'c': 6},
                                                                          {'a': 3, 'c': 7}])
        joined = outer_join(test_data1, test_data2, ['a'], ['a'], ['b'], ['c'])
        joined | 'Print Output e' >> beam.Map(print)


def run_tests():
    test_outer_join1()
    test_outer_join2()
    test_aggregate_by_key()


if __name__ == '__main__':
    run_tests()