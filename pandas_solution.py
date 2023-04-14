import pandas as pd
import itertools


def main():
    df1 = pd.read_csv('dataset1.csv')
    df2 = pd.read_csv('dataset2.csv')

    df = pd.merge(df1, df2, on='counter_party', how='inner')
    df = df[["legal_entity",
                           "counter_party",
                           "tier",
                           'invoice_id',
                           "rating",
                           'value',
                           'status']]
    df_filtered_arap = df[df.status == 'ARAP']
    df_filtered_accr = df[df.status == 'ACCR']
    df_arap_grouped = df_filtered_arap.groupby(['legal_entity', 'counter_party', 'tier']).agg({'value': 'sum'}).reset_index()
    df_accr_grouped = df_filtered_accr.groupby(['legal_entity', 'counter_party', 'tier']).agg({'value': 'sum'}).reset_index()
    df_max_rating = df.groupby(['legal_entity', 'counter_party', 'tier']).agg({'rating': 'max'}).reset_index()

    # join 3 dataframes together by legal_entity, counter_party and tier
    df_aggregated1 = pd.merge(df_arap_grouped, df_accr_grouped, on=['legal_entity', 'counter_party', 'tier'], how='outer')
    df_aggregated1['value_x'].fillna(0, inplace=True)
    df_aggregated1['value_y'].fillna(0, inplace=True)
    df_aggregated1.rename(columns={'value_x': 'sum_value_status_arap', 'value_y': 'sum_value_status_accr'}, inplace=True)
    df_aggregated2 = pd.merge(df_aggregated1, df_max_rating, on=['legal_entity', 'counter_party', 'tier'], how='inner')
    df_aggregated2.rename(columns={'rating': 'max_rating'}, inplace=True)
    dimensions_to_aggregate_by = ['legal_entity', 'counter_party', 'tier']

    combined_data = []
    column_pairs_comb = list(itertools.combinations(dimensions_to_aggregate_by, 2))
    all_single_dims = [[dim] for dim in dimensions_to_aggregate_by]
    for combination in column_pairs_comb + all_single_dims + [dimensions_to_aggregate_by]:
        df_sub_agg = df_aggregated2.groupby(list(combination)).agg({'sum_value_status_arap': 'sum',
                                                                    'sum_value_status_accr': 'sum',
                                                                    'max_rating': 'max'}).reset_index()
        remaining_dims = set(dimensions_to_aggregate_by) - set(combination)
        for dim in remaining_dims:
            df_sub_agg = df_sub_agg.assign(**{dim: "Total"})
        combined_data.append(df_sub_agg)
    combined_df = pd.concat(combined_data)
    combined_df = combined_df.reindex(columns=['legal_entity', 'counter_party', 'tier', 'max_rating', 'sum_value_status_arap', 'sum_value_status_accr'])

    #  rename columns
    combined_df.rename(columns={'max_rating': 'max(rating by counterparty)',
                                              'sum_value_status_arap': 'sum(value where status=ARAP)',
                                              'sum_value_status_accr': 'sum(value where status=ACCR)'},
                       inplace=True)
    combined_df.to_csv('output.csv', index=False)


if __name__ == '__main__':
    main()
