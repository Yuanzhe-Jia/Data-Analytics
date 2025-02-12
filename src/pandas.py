# step 1: get data from clickhouse
# import libraries
from clickhouse_connect import get_client
import pandas as pd

# set up a client
client = get_client(host='localhost', port='8123', username='default', password='', database='default')

# define sql query
sql = """
select
    client_id,
    event_time,
    event_name,
    platform,
    device_name,
    app_name,
    sensor_version,
    duration
from 
    event_table
where
    event_time between '2024-08-25 00:00:00' and '2024-08-25 12:00:00'
limit 1000
"""

# save the result to a dataframe
df_result = client.query_df(sql)

df_result.head() # show result
df_result.info() # show basic info 
df_result.isnull().sum() # check missing values

# step 2: data manipulation
# extract some of columns
df_result[['client_id', 'event_name', 'platform']].head()

# locate data
df_result.loc[2:4, ['app_name', 'sensor_version']]

# extraxct subsets by filters
# df_result[df_result.duration >= 100].head()
# df_result[(df_result.duration >= 100) & (df_result.duration <= 200)].head()
# df_result[(df_result.sensor_version >= 'js-0.6.5') | (df_result.sensor_version == 'js-0.6.6')].head()
df_result[df_result.sensor_version.isin(['js-0.6.5', 'js-0.6.6'])].head()

# apply mapping functions
# df_result.event_time.map(lambda x: str(x)[:7]).head()
df_result.duration.map(lambda x: 1 if x >= 100 else 0).head()

# add a new column
df_result['date_time'] = df_result.event_time.map(lambda x: str(x)[:7])
df_result[['client_id', 'event_time', 'date_time']].head()

# delete columns
df_result.drop('app_name', axis=1, inplace=True)

# delete rows
df_result.drop(0, axis=0, inplace=True)
df_result.head()

# aggregate data by dimensions
# df_result.groupby('platform').duration.mean()
df_result.groupby('platform').duration.agg(['count', 'mean', 'min', 'max', 'sum'])

# count values for dimensions
# df_result.platform.value_counts()
df_result.platform.value_counts(normalize=True)

# sort data by dimensions
df_result.sort_values("duration", ascending=False).head()

# manipulate string
df_result.date_time.str.replace('-', '/').head()

# random sampling
# df_result.sample(n=10, random_state=42).head()
df_result.sample(frac=0.1, random_state=42).head()

# discretise data
df_result['type'] = pd.cut(df_result.duration, bins=[-10, 1, 10, 10000], labels=['small', 'medium', 'large'])
df_result.sample(5)[['client_id', 'duration', 'type']].head()

# create cross table
pd.crosstab(df_result.platform, df_result.type)

# plot data
%matplotlib inline
df_result.set_index('event_time').plot(kind='line', figsize=(16, 6)) # line, bar, hist, pie...
