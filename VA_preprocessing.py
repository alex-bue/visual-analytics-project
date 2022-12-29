import pandas as pd
import numpy as np
import geopandas as gpd
from shapely import Point
# from geopy.geocoders import Nominatim
# from geopy.point import Point
import datetime as dt


def conduct_reverse_geocoding(df: pd.DataFrame, gdf_shape):
    """
    enriches the dataframe with the countries the long-lat data points are located in
    :param df: the dataframe with the long-lat data
    :param gdf_shape: shapefile with the country borders of the entire world
    :return: df: original dataframe with additional country column
    """
    # convert df to GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.long, df.lat))

    # Merge DataFrames
    pointInPolys = gpd.sjoin(gdf, gdf_shape, how='left')

    # Drop columns we don't need
    pointInPolys = pointInPolys.drop(
        columns=['french_shor', 'status', 'index_right', 'color_code', 'status', 'continent'])
    return pointInPolys


start = dt.datetime.now()
i = 0  # count iterations for runtime checks

# Read in shapefile with country boundaries
gdf_shape = gpd.GeoDataFrame.from_file('../../Documents/Master/Semester1/Visual_Analytics/Dashboard_Files/'
                                       'world-administrative-boundaries/world-administrative-boundaries.shp')

# read in country list of Europe
europe = pd.read_csv('../../Documents/Master/Semester1/Visual_Analytics/Europe_2.csv')

# the following nested for loops read in all single files and perform the necessary data preprocessing on them
# for memory limitation reasons, the datasets will be combined to a shared dataset later
path = '../../Documents/Master/Semester1/Visual_Analytics/'
path2 = 'Dashboard_Files/'
# path = './data/performance/'

for i in [["type=fixed/", "fixed"], ["type=mobile/", "mobile"]]:
    path_i = path + path2 + i[0]
    category = i[1]
    for j in [['year=2019/', 2019], ['year=2020/', 2020], ['year=2021/', 2021], ['year=2022/', 2022]]:
        path_j = path_i + j[0]
        year = j[1]
        quarter_list = []
        if year != 2022:
            quarter_list = [['quarter=1/', '01'], ['quarter=2/', '04'], ['quarter=3/', '07'], ['quarter=4/', '10']]
        elif year == 2022:
            quarter_list = [['quarter=1/', '01'], ['quarter=2/', '04'], ['quarter=3/', '07']]
        for k in quarter_list:
            path_k = path_j + k[0]
            month = k[1]
            path_k = path_k + str(year)+"-"+month+"-01_performance_"+str(category)+"_tiles.parquet"
            df = pd.read_parquet(path_k, engine='pyarrow')
            df = df.head()

            # add year, month and category information to the dataframe -
            # this information is only in file names, not in the files itself
            df['quarter'] = dt.date(year, int(month), 1)
            df['category'] = category

            # clean the location column, so that one latitude and one longitude value remain
            df['tile'] = df['tile'].apply(lambda x: x.replace("POLYGON", ""))
            df['tile'] = df['tile'].apply(lambda x: x.replace("(", ""))
            df['tile'] = df['tile'].apply(lambda x: x.replace(")", ""))
            df['tile'] = df['tile'].apply(lambda x: x.replace(",", ""))
            df[['long', 'lat', 'rest']] = df['tile'].str.split(pat=" ", n=2, expand=True)
            df = df.drop(columns=['rest', 'tile', 'quadkey'])

            # retrieve country from lat-long data (reverse geocoding)
            df = conduct_reverse_geocoding(df, gdf_shape)

            # save final dataframe as csv
            df.to_csv(path + f'preprocessed_files/whole_world/whole_world_{i}.csv', sep=';')

            # filter for Europe:
            europe = europe['Name'][:50].tolist()

            # ToDo: adjust the column name for country and check which countries aren't written the same way

            unique_countries = df['country'].unique()
            unique_countries = pd.Series(unique_countries)
            unique_countries.to_csv(path + 'unique_country_list.csv')

            df_europe = df[df['country'].isin(europe)]
            df_europe.to_csv(path + f'preprocessed_files/europe/europe_{i}.csv', sep=';')

            print("europe to csv after " + str(dt.datetime.now() - start))

            # filter for Germany
            df_germany = df_europe[df_europe['country'] == 'Germany']
            df_germany.to_csv(path + f'preprocessed_files/germany/germany_{i}.csv', sep=';')

            i += 1
            print(str(i) + " Datasets processed in " + str(dt.datetime.now()-start))
print("Successful execution in " + str(dt.datetime.now()-start))
