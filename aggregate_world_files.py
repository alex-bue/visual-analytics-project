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

    # aggregate as weighted average
    # create helper columns for weighted averages
    df['product1'] = df['avg_d_kbps'] * df['devices']
    df['product2'] = df['avg_u_kbps'] * df['devices']
    df['product3'] = df['avg_lat_ms'] * df['devices']

    # add additional column with ones and take sum in groupby
    df['row_count'] = 1

    # Aggregate
    df_agg = df.groupby(['country', 'quarter', 'category']).agg({'product1': sum,
                                                              'product2': sum,
                                                              'product3': sum,
                                                              'devices': sum,
                                                              'tests': sum,
                                                              'row_count': sum})
    
    # retrieve actual values from product columns
    df_agg['avg_d_kbps'] = df_agg['product1'] / df_agg['devices']
    df_agg['avg_u_kbps'] = df_agg['product2'] / df_agg['devices']
    df_agg['avg_lat_ms'] = df_agg['product3'] / df_agg['devices']

    # drop helper columns
    df_agg = df_agg.drop(columns=['product1', 'product2', 'product3'])

    # add cols for mbps
    df_agg['avg_d_mbps'] = df_agg['avg_d_kbps'] / 1000
    df_agg['avg_u_mbps'] = df_agg['avg_u_kbps'] / 1000

    # drop kbps cols
    df_agg.drop(columns=['avg_d_kbps', 'avg_u_kbps'], inplace=True)

    # Convert columns to int16 to save space
    df_agg['avg_d_mbps'] = df_agg['avg_d_mbps'].astype('int64')
    df_agg['avg_u_mbps'] = df_agg['avg_u_mbps'].astype('int64')
    df_agg['avg_lat_ms'] = df_agg['avg_lat_ms'].astype('int64')
    df_agg['tests'] = df_agg['tests'].astype('int64')
    df_agg['devices'] = df_agg['devices'].astype('int64')
    df_agg['row_count'] = df_agg['row_count'].astype('int64')
    # df_agg['country'] = df_agg['country'].astype('string')
    # df_agg['category'] = df_agg['category'].astype('category')

    # Concat together
    df_world = pd.concat([df_world, df_agg])

# save to csv
df_world.to_csv('./data/final_data/world_aggregated_final.csv', sep=';')

df_world.to_excel('./data/final_data/world_aggregated_final.xlsx', sheet_name='world_aggregated_final')