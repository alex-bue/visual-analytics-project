# Imports
import pandas as pd
import numpy as np

# defining which columns we need to use when loading the data to save some memory
req_cols = ['avg_d_kbps', 'avg_u_kbps', 'avg_lat_ms', 'tests', 'devices', 'quarter', 'category', 'name', 'geometry']

# Create empty dataframe that will be used for concatenating the different aggregated dataframes
df_world = pd.DataFrame()

# Read and aggregate data
for i in range(0,30):
    df = pd.read_csv(f'./data/preprocessed_files/whole_world/whole_world_{i}.csv', sep=';', usecols=req_cols)

    # rename country col
    df.rename(columns={'name':'country'}, inplace=True)

    # calculate average values per country-quarter-category combination

    # add additional column with ones and take sum in groupby - a row count is necessary later for weighted averaging
    df['row_count'] = 1

    # Aggregate
    df_agg = df.groupby(['country', 'quarter', 'category']).agg({'devices': sum,
                                                                 'tests': sum,
                                                                 'row_count': sum,
                                                                 'avg_d_kbps': np.average,
                                                                 'avg_u_kbps': np.average,
                                                                 'avg_lat_ms': np.average})

    # add cols for mbps
    df_agg['avg_d_mbps'] = df_agg['avg_d_kbps'] / 1000
    df_agg['avg_u_mbps'] = df_agg['avg_u_kbps'] / 1000

    # drop kbps cols
    df_agg.drop(columns=['avg_d_kbps', 'avg_u_kbps'], inplace=True)

    # Convert columns to int64 to save space
    df_agg['avg_d_mbps'] = df_agg['avg_d_mbps'].astype('int64')
    df_agg['avg_u_mbps'] = df_agg['avg_u_mbps'].astype('int64')
    df_agg['avg_lat_ms'] = df_agg['avg_lat_ms'].astype('int64')
    df_agg['tests'] = df_agg['tests'].astype('int64')
    df_agg['devices'] = df_agg['devices'].astype('int64')
    df_agg['row_count'] = df_agg['row_count'].astype('int64')

    # Concat together
    df_world = pd.concat([df_world, df_agg])

# save to csv
df_world.to_csv('./data/final_data/world_aggregated.csv', sep=';')
