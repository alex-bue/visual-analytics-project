# import packages
import pandas as pd

# read to dataframe
df_germany = pd.read_csv("./data/final_data/germany_final.csv", sep=";")

# drop na (maybe add do at the end of germamy_reverse_geocoding script before export)
df_germany = df_germany.dropna()

# Convert DataFrame from wide to long format
# id vars: federal_state, county, municipality, category, tests, devices, quarter
# value vars: avg_d_mbps, avg_u_mbps, avg_lat_ms

df_germany_long = pd.melt(df_germany, id_vars=['federal_state', 'county', 'municipality', 'devices', 'tests', 'category', 'quarter'], value_vars=['avg_d_mbps', 'avg_u_mbps', 'avg_lat_ms'], var_name='measure', value_name='value')

df_germany_long.to_csv("./data/final_data/germany_final_long.csv", sep=";", index=False)