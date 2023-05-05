import os
import pandas as pd

def dataframe(files):
    dataframe = pd.DataFrame([])
    for f in files:
        df = pd.read_csv(f'{f}', converters={'prefix': str, 'rate': float})
        df['carrier'] = '#' + os.path.splitext(f)[0]
        dataframe = pd.concat([dataframe, df])
    return dataframe

at_df = dataframe(['Avid.csv', 'Touchtone.csv'])
cn_df = dataframe(['CCI.csv', 'Novatel.csv'])

# Filter rates below 0.004 for cci & novatel
filtered_under_df = cn_df[cn_df['rate'] < 0.004]

# Find cheapest carriers for each prefix in filtered_under_df
cheapest_under_df = filtered_under_df.sort_values(['rate']).groupby('prefix')['carrier'].apply(', '.join).to_frame()

# Filter rates above 0.004 for cci & novatel
filtered_over_df = cn_df[cn_df['rate'] >= 0.004]

# Merge filtered_over_df and at_df
merged_over_df = pd.concat([filtered_over_df, at_df])

# Sort by prefix and rate
#merged_over_df = merged_over_df.sort_values(['prefix', 'rate'])

# Find cheapest carriers for each prefix
cheapest_over_df = merged_over_df.sort_values(['rate']).groupby('prefix')['carrier'].apply(', '.join).to_frame()

# Merge cheapest_under_df and cheapest_over_df
merged_df = pd.merge(cheapest_under_df, cheapest_over_df, on='prefix', how='outer', suffixes=('_under', '_over'))
merged_df['gwlist'] = merged_df['carrier_under'].fillna('') + ', ' + merged_df['carrier_over'].fillna('')
merged_df = merged_df.drop(['carrier_under', 'carrier_over'], axis=1)
merged_df.to_csv('merged.csv')

#merged_df = merged_df.append({'prefix': prefix, 'carrier_under': ','.join(cheapest_under), 'carrier_over': ','.join(cheapest_over), 'gwlist': carriers}, ignore_index=True)

merged_df['ruleid'] = range(1, 1 + len(merged_df))
merged_df['groupid'] = 3
merged_df['timerec'] = None
merged_df['priority'] = 0
merged_df['routeid'] = None
merged_df['sort_alg'] = 'N'
merged_df['sort_profile'] = None
merged_df['attrs'] = None
merged_df['description'] = 'LCR'
merged_df.rename(columns={'carrier': 'gwlist'}, inplace=True)
merged_df.to_csv('result.csv')