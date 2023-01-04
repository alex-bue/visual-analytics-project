# import packages
import pandas as pd

# read to dataframe
df_germany = pd.read_csv("./data/final_data/germany_final.csv", sep=";")

# drop na (maybe add do at the end of germamy_reverse_geocoding script before export)
df_germany = df_germany.dropna()

# create duplicates of columns - these will be used by the visuals that need wide format (and no long format)
df_germany['avg_d_mbps_copy'] = df_germany['avg_d_mbps']
df_germany['avg_u_mbps_copy'] = df_germany['avg_u_mbps']
df_germany['avg_lat_ms_copy'] = df_germany['avg_lat_ms']

# Rename value vars to look better for dashboard
df_germany.rename(columns={'avg_d_mbps': 'Download Speed', 'avg_u_mbps': 'Upload Speed', 'avg_lat_ms': 'Latency'},
                  inplace=True)

# Convert DataFrame from wide to long format
id_vars_list = ['federal_state', 'county', 'municipality', 'category', 'quarter',
                'avg_d_mbps_copy', 'avg_u_mbps_copy', 'avg_lat_ms_copy']
value_vars_list = ['Download Speed', 'Upload Speed', 'Latency']
df_germany_long = pd.melt(df_germany, id_vars=id_vars_list, value_vars=value_vars_list,
                          var_name='measure', value_name='value')
# Write to csv
df_germany_long.to_csv("./data/final_data/germany_final_long.csv", sep=";", index=False)