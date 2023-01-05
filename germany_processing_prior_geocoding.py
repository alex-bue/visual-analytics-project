# Imports
import pandas as pd
import numpy as np

# Germany
df_germany = pd.read_csv('./data/final_data/germany_combined.csv', sep=';')

# Cleaning and adjustments
df_germany.drop(columns=['geometry', 'iso3', 'continent', 'region', 'iso_3166_1_'], inplace=True)

# rename country column
df_germany.rename(columns={'name':'country'}, inplace=True)

# Convert to mpbs
df_germany['avg_d_mbps'] = df_germany['avg_d_kbps'] / 1000
df_germany['avg_u_mbps'] = df_germany['avg_u_kbps'] / 1000

# Round mbps and convert to int
df_germany['avg_d_mbps'] = df_germany['avg_d_mbps'].round(0).astype(int)
df_germany['avg_u_mbps'] = df_germany['avg_u_mbps'].round(0).astype(int)

# Drop kbps columns
df_germany.drop(columns=['avg_d_kbps', 'avg_u_kbps'], inplace=True)

# Convert columns to int64 to save space
df_germany['avg_d_mbps'] = df_germany['avg_d_mbps'].astype('int64')
df_germany['avg_u_mbps'] = df_germany['avg_u_mbps'].astype('int64')
df_germany['avg_lat_ms'] = df_germany['avg_lat_ms'].astype('int64')
df_germany['tests'] = df_germany['tests'].astype('int64')
df_germany['devices'] = df_germany['devices'].astype('int64')
df_germany['country'] = df_germany['country'].astype('string')
df_germany['category'] = df_germany['category'].astype('category')

# save as comma separated csv
df_germany.to_csv('./data/final_data/germany_pre_geocoding.csv', sep=';', index=False)