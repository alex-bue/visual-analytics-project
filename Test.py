import pandas as pd
import numpy as np
import geopandas as gpd
from shapely import Point, Polygon, MultiPolygon
# from geopy.geocoders import Nominatim
# from geopy.point import Point
import datetime as dt

# Extracting Path and Building DataFrame

# read in all single files and concatenate them
path = './data/aws_data/performance/'
df_complete = pd.DataFrame()
for i in [["type=fixed/", "fixed"], ["type=mobile/", "mobile"]]:
    path_i = path + i[0]
    category = i[1]
    for j in [['year=2019/', 2019], ['year=2020/', 2020], ['year=2021/', 2021], ['year=2022/', 2022]]:
        path_j = path_i + j[0]
        year = j[1]
        quarter_list = []
        if year != 2022:
            quarter_list = [['quarter=1/', '01'], ['quarter=2/',
                                                   '04'], ['quarter=3/', '07'], ['quarter=4/', '10']]
        elif year == 2022:
            quarter_list = [['quarter=1/', '01'],
                            ['quarter=2/', '04'], ['quarter=3/', '07']]
        for k in quarter_list:
            path_k = path_j + k[0]
            month = k[1]
            path_k = path_k + str(year)+"-"+month + \
                "-01_performance_"+str(category)+"_tiles.parquet"
            df = pd.read_parquet(path_k, engine='pyarrow')
            # add year, month and category information to the dataframe -
            # those are only in file names, not in the files itself
            df['quarter'] = dt.date(year, int(month), 1)
            df['category'] = category

            # retrieve lat-long data from tile column
            df['tile'] = df['tile'].apply(lambda x: x.replace("POLYGON", ""))
            df['tile'] = df['tile'].apply(lambda x: x.replace("(", ""))
            df['tile'] = df['tile'].apply(lambda x: x.replace(")", ""))
            df['tile'] = df['tile'].apply(lambda x: x.replace(",", ""))
            df[['long', 'lat', 'rest']] = df['tile'].str.split(
                pat=" ", n=2, expand=True)
            df = df.drop(columns=['rest', 'tile', 'quadkey'])

#%%
# Shape and Head of DataFrame
print(df.shape)
df.head()
#%%
# Save to CSV as intemediary version
df.to_csv('./data/preprocessed_files/dataset-without-regions.csv', sep=',')
#%%
# Convert to GeoDataFrame
gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.long, df.lat))
gdf.head()
#%%
# Reading shapefile with country boundaries
gdf_shape = gpd.GeoDataFrame.from_file('./data/shapefiles/world-administrative-boundaries.shp')
#%%
# Merge DataFrames
pointInPolys = gpd.sjoin(gdf, gdf_shape, how='left')

# Print head
pointInPolys.head()

#%%
# Drop columns we don't need
pointInPolys = pointInPolys.drop(columns=['french_shor', 'status', 'index_right', 'color_code', 'status', 'continent'])
#%%
# Print "final" DF
pointInPolys.head()
#%%
# Check NaNs
print(pointInPolys['iso3'].isna().sum())

pointInPolys[pointInPolys.isnull().any(axis=1)]
#%%