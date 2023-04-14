import apache_beam as beam
import itertools
from beam_utils import aggregate_by_key, outer_join, rename_column, add_column, fill_na


col_names_dataset1 = ['invoice_id', 'legal_entity', 'counter_party', 'rating', 'status', 'value']
col_types_dataset1 = [str, str, str, int, str, float]

col_names_dataset2 = ['counter_party', 'tier']
col_types_dataset2 = [str, str]


# Define a function to parse each row of the input file
def parse_row_dataset1(row):
    # Split the row into columns
    columns = row.strip('\n').split(',')
    # Extract the legal_entity and value columns
    value_dict = {}
    for i, col_name in enumerate(col_names_dataset1):
        value_dict[col_name] = col_types_dataset1[i](columns[i])
    # Return a tuple of (legal_entity, value)

    return value_dict


def parse_row_dataset2(row):
    # Split the row into columns
    columns = row.strip('\n').split(',')
    # Extract the legal_entity and value columns
    value_dict = {}
    for i, col_name in enumerate(col_names_dataset2):
        value_dict[col_name] = col_types_dataset2[i](columns[i])
    # Return a tuple of (legal_entity, value)

    return value_dict


def filter_matching_groups(element):
    pcol1, pcol2 = element[1]['pcol1'], element[1]['pcol2']
    if not pcol1 or not pcol2:
        return False
    return True


def extract_key_and_value(element, key_fields, value_field):
    return tuple(tuple(element[key] for key in key_fields), element[value_field])






def replace_linebreaks(input_filepath, output_filepath):
    with open(input_filepath, 'r') as f:
        # Read the file contents and replace Windows line breaks with Unix line breaks
        file_contents = f.read().replace('\r\n', '\n')
    with open(output_filepath, 'w', newline='\n') as f:
        # Write the file contents with Unix line breaks
        f.write(file_contents)


if __name__ == '__main__':
    with beam.Pipeline() as pipeline:
        # convert line breaks from windows to unix
        replace_linebreaks('dataset1.csv', 'dataset1_patched.csv')
        replace_linebreaks('dataset2.csv', 'dataset2_patched.csv')

        # Read the input files
        rows_ds1 = pipeline | 'Read CSV 1' >> beam.io.ReadFromText('dataset1_patched.csv', skip_header_lines=1)
        rows_ds2 = pipeline | 'Read CSV 2' >> beam.io.ReadFromText('dataset2_patched.csv', skip_header_lines=1)

        # Parse the filtered rows and create PCollection
        parsed_rows_dataset1 = rows_ds1 | 'Parse Rows 1' >> beam.Map(parse_row_dataset1)

        parsed_rows_dataset2 = rows_ds2 | 'Parse Rows 2' >> beam.Map(parse_row_dataset2)

        # extract the key to group on
        key_ds1 = parsed_rows_dataset1 | 'Extract Key Columns 1' >> beam.Map(lambda x: (x['counter_party'], x))
        key_ds2 = parsed_rows_dataset2 | 'Extract Key Columns 2' >> beam.Map(lambda x: (x['counter_party'], x))

        joined = outer_join(parsed_rows_dataset1, parsed_rows_dataset2, ['counter_party'], ['counter_party'], 'joining by counter_party')

        # filter the rows by value = "ARAP" or "ACCR"
        filtered_rows_arap = joined | 'Filter Rows ARAP' >> beam.Filter(lambda row: row['status'] == "ARAP")
        filtered_rows_accr = joined | 'Filter Rows ACCR' >> beam.Filter(lambda row: row['status'] == "ACCR")

        agg_rows_arap = aggregate_by_key(['legal_entity', 'counter_party', 'tier'],
                                         {'value': lambda x, y: x+y},
                                         filtered_rows_arap,
                                         txt_suffix='_arap')
        arap_agg_res = rename_column(agg_rows_arap, 'value', 'value_arap', txt_suffix='_arap')
        agg_rows_accr = aggregate_by_key(['legal_entity', 'counter_party', 'tier'],
                                         {'value': lambda x, y: x+y},
                                         filtered_rows_accr,
                                         txt_suffix='_accr')
        accr_agg_res = rename_column(agg_rows_accr, 'value', 'value_accr', txt_suffix='_accr')

        joined_rows_arap_accr = outer_join(arap_agg_res,
                                           accr_agg_res,
                                           ['legal_entity', 'counter_party', 'tier'],
                                           ['legal_entity', 'counter_party', 'tier'],
                                           ['value_arap'],
                                           ['value_accr'],
                                           txt_suffix='_arap_accr_joining')

        joined_rows = fill_na(joined_rows_arap_accr, 'value_arap', 0)
        joined_rows = fill_na(joined_rows_arap_accr, 'value_accr', 0)

        # calculate max rating
        max_rating = aggregate_by_key(['legal_entity', 'counter_party', 'tier'],
                                      {'rating': lambda x, y: max(x, y)},
                                      joined,
                                      txt_suffix='_rating')
        max_rating = rename_column(max_rating, 'rating', 'max_rating', txt_suffix='_rating2')

        aggregated_data = outer_join(joined_rows_arap_accr,
                                     max_rating,
                                     ['legal_entity', 'counter_party', 'tier'],
                                     ['legal_entity', 'counter_party', 'tier'],
                                     ['value_arap', 'value_accr'],
                                     ['max_rating'],
                                     txt_suffix='_combined')

        # aggregate by all combinations of dimensions
        dimensions_to_aggregate_by = ['legal_entity', 'counter_party', 'tier']

        column_pairs_comb = list(itertools.combinations(dimensions_to_aggregate_by, 2))
        all_single_dims = [[dim] for dim in dimensions_to_aggregate_by]
        all_dims_combinations = all_single_dims + column_pairs_comb + [dimensions_to_aggregate_by]

        combined_data = []
        for combination in all_dims_combinations:
            # aggregate by the combination of dimensions
            def pair_sum(x, y):
                return x+y

            def pair_max(x, y):
                return max(x, y)

            data_sub_agg = aggregate_by_key(list(combination),
                                            {'value_arap': pair_sum, 'value_accr': pair_sum, 'max_rating': pair_max},
                                            aggregated_data,
                                            txt_suffix=str(combination))
            # add remaining dimensions to the table
            remaining_dims = set(dimensions_to_aggregate_by) - set(combination)
            for dim in remaining_dims:
                data_sub_agg = add_column(data_sub_agg, dim, 'Total', txt_suffix=str(combination))

            combined_data.append(data_sub_agg)

        combined_collections = (
                [*combined_data]
                | 'Combine all aggregations' >> beam.Flatten()
        )
        # rename columns to match the output
        renaming_dictionary = {'max_rating': "max(rating by counterparty)",
                               "value_arap": "sum(value where status=ARAP)",
                               "value_accr": "sum(value where status=ACCR)"}
        for key, value in renaming_dictionary.items():
            combined_collections = rename_column(combined_collections, key, value, txt_suffix='_renaming' + key)

        # transform to csv
        output_columns = ['legal_entity',
                          'counter_party',
                          'tier',
                          'max(rating by counterparty)',
                          'sum(value where status=ARAP)',
                          'sum(value where status=ACCR)']

        header = ','.join(output_columns)

        header_collection = pipeline | "Create header collection" >> beam.Create([header])
        csv_collection = combined_collections | "convert to csv" >> beam.Map(lambda x: ','.join([str(x[col]) for col in output_columns]))
        combined_header_and_csv = (
                [header_collection, csv_collection]
                | 'Adding header' >> beam.Flatten()
        )
        # write to file
        combined_header_and_csv | 'Write to file' >> beam.io.WriteToText('output_beam.csv')


