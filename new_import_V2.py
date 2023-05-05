import os
import pandas as pd

def dataframe(files):
    dataframe = pd.DataFrame([])
    for f in files:
        df = pd.read_csv(f'{f}', converters={ 'prefix': str, 'rate': float})
        df['carrier'] = '#' + os.path.splitext(f)[0]
        dataframe = pd.concat([dataframe, df])
    return dataframe

at_df = dataframe(['Avid.csv', 'Touchtone.csv'])
cn_df = dataframe(['CCI.csv', 'Novatel.csv'])

# filter rates below 0.004 (cci & novatel)
filtered_under_df = cn_df[cn_df['rate'] < 0.004]
# filter rates above or equal to 0.004 (cci & novatel)
filtered_over_df = cn_df[cn_df['rate'] >= 0.004]

# join filtered_under_df and at_df
merged1_df = pd.concat([filtered_under_df, at_df])
# sort merged1_df by rate and group by prefix and carrier
grouped1_df = merged1_df.sort_values(['rate']).groupby(['prefix', 'carrier'], sort=False)['rate'].min().reset_index()
# sort grouped1_df by rate and group by prefix
grouped1_df = grouped1_df.sort_values(['rate', 'carrier']).groupby('prefix', sort=False)['carrier'].apply(', '.join).to_frame()

# sort filtered_over_df by rate and group by prefix and carrier
grouped2_df = filtered_over_df.sort_values(['rate']).groupby(['prefix', 'carrier'], sort=False)['rate'].min().reset_index()
# sort grouped2_df by rate and group by prefix
grouped2_df = grouped2_df.sort_values(['rate', 'carrier']).groupby('prefix', sort=False)['carrier'].apply(', '.join).to_frame()

# concatenate grouped1_df and grouped2_df
result_df = pd.concat([grouped1_df, grouped2_df], sort=False)
result_df['ruleid'] = range(1, 1+len(result_df))
result_df['groupid'] = 3
result_df['timerec'] = None
result_df['priority'] = 0
result_df['routeid'] = None
result_df['sort_alg'] = 'N'
result_df['sort_profile'] = None
result_df['attrs'] = None
result_df['description'] = 'LCR'
result_df.rename(columns={'carrier': 'gwlist'}, inplace=True)
result_df.to_csv('result.csv', index=False)
