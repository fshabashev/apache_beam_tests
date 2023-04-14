import pandas as pd



df1 = pd.read_csv('output.csv')
df2 = pd.read_csv('output_beam.csv-00000-of-00001')
df1.sort_values(by=['legal_entity', 'counter_party', 'tier'], inplace=True)
df2.sort_values(by=['legal_entity', 'counter_party', 'tier'], inplace=True)
df1.reset_index(drop=True, inplace=True)
df2.reset_index(drop=True, inplace=True)
d1 = df1.to_dict(orient='records')
d2 = df2.to_dict(orient='records')
d1 = [tuple(elem.values()) for elem in d1]
d2 = [tuple(elem.values()) for elem in d2]
for elem in d1:
    if elem not in d2:
        print(elem)
for elem in d2:
    if elem not in d1:
        print(elem)
print(len(d1))
print(len(d2))