import apache_beam as beam
from typing import Callable
from functools import reduce
import itertools

def aggregate_by_key(key_column_names, value_colname2aggfun: dict[str, Callable], collection, txt_suffix=''):
    # extract key columns
    key_columns = collection | 'Extract Key Columns ' + str(key_column_names) + txt_suffix >> beam.Map(lambda x: (tuple(x[key] for key in key_column_names), x))
    # group by key columns
    grouped_rows = key_columns | 'Group Rows ' + str(key_column_names) + txt_suffix >> beam.GroupByKey()

    # aggregate the values
    def aggfun(x):
        values = {}
        # add key columns
        for i, key_colname in enumerate(key_column_names):
            values[key_colname] = x[0][i]

        for value_colname, aggfun in value_colname2aggfun.items():
            extracted_values = [item[value_colname] for item in x[1]]
            values[value_colname] = reduce(aggfun, extracted_values)

        return values

    aggregated_rows = grouped_rows | 'Aggregate Rows ' + str(key_column_names) + txt_suffix >> beam.Map(aggfun)

    return aggregated_rows


def outer_join(left, right, left_key_fields, right_key_fields, left_value_field=[], right_value_field=[], txt_suffix=''):
    # extract key columns
    key_columns_left = left | 'Extract Key Columns Left' + txt_suffix >> beam.Map(lambda x: (tuple(x[key] for key in left_key_fields), x))
    key_columns_right = right | 'Extract Key Columns Right' + txt_suffix >> beam.Map(lambda x: (tuple(x[key] for key in right_key_fields), x))

    # join by key columns
    def flatten(x):
        if not x[1]['pcol1']:
            new_records = []
            for row in x[1]['pcol2']:
                new_record = row
                for field in left_value_field:
                    new_record[field] = None
                new_records.append(new_record)
            return new_records
        if not x[1]['pcol2']:
            new_records = []
            for row in x[1]['pcol1']:
                new_record = row
                for field in right_value_field:
                    new_record[field] = None
                new_records.append(new_record)
            return new_records

        output = []
        for row1 in x[1]['pcol1']:
            for row2 in x[1]['pcol2']:
                output.append(row1 | row2)
        return output

    joined = ({'pcol1': key_columns_left, 'pcol2': key_columns_right}
              | 'CoGroupByKey' + str(left_key_fields) + str(right_key_fields) + txt_suffix >> beam.CoGroupByKey()
              | 'Flatten PCollection1' + str(left_key_fields) + str(right_key_fields) + txt_suffix >> beam.FlatMap(flatten)
              )
    return joined


def fill_na(collection, col_name, value):
    def replace_nans(x):
        patched_x = x
        if not x.get(col_name, None):
            patched_x[col_name] = value
        return patched_x
    return collection | 'Fill NA ' + col_name >> beam.Map(replace_nans)


def rename_column(collection, col_name, new_name, txt_suffix=''):
    def rename(x):
        patched_x = {key: value for key, value in x.items() if key != col_name}
        patched_x[new_name] = x[col_name]
        return patched_x
    return collection | 'Rename Column ' + col_name + ' to ' + new_name + txt_suffix >> beam.Map(rename)


def add_column(collection, col_name, value, txt_suffix=''):
    def add_constant(x):
        patched_x = x
        patched_x[col_name] = value
        return patched_x
    return collection | 'Add Constant Column ' + col_name + txt_suffix >> beam.Map(add_constant)
