import pandas as pd
import numpy as np
import geopandas as gpd
from shapely import Point
# from geopy.geocoders import Nominatim
# from geopy.point import Point
import datetime as dt


def retrieve_address(x):
    try:
        return x.raw['address']
    except AttributeError:
        return np.nan


def get_country(x):
    try:
        return x.get('country', np.nan)
    except AttributeError:
        return np.nan


start = dt.datetime.now()
i = 0  # count iterations for runtime checks
# read in all single files and concatenate them
path = './data/performance/'
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
            df = df.head()
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

            # filter lat-long of Europe
            # df['lat'] = df['lat'].astype('float64')
            # df['long'] = df['long'].astype('float64')
            # df = df[(df['lat'] > 28) & (df['lat'] < 75) &
            #         (df['long'] > -25) & (df['long'] < 70)]

            # retrieve country from lat-long data
            # geolocator = Nominatim(user_agent="geoapiExercises")
            # ToDo: use more efficient function
            # df['location'] = df.apply(lambda x: geolocator.reverse(Point(x['lat'], x['long'])), axis=1)
            # df['country'] = df['location'].apply(lambda x: retrieve_address(x))
            # df['country'] = df['country'].apply(lambda x: get_country(x))
            # df = df.drop(columns=['location'])

            # concat the prepared dataset to the other prepared datasets
            # # df_complete = pd.concat([df_complete, df])
            # # i += 1
            # # print(str(i) + " Datasets concatenated in " +
            #       str(dt.datetime.now()-start))
# print("data read in completed in " + str(dt.datetime.now()-start))

# df_raw = pd.read_parquet('./data/performance/type=fixed/'
#                          'year=2019/quarter=1/2019-01-01_performance_fixed_tiles.parquet', engine = 'pyarrow')
# df = df_raw.head()


# save the whole dataset as csv:
# df_complete.to_csv('./data/performance/df_whole_world.csv', sep=",")

# print("whole world to csv after " + str(dt.datetime.now()-start))

# get a list of all countries in geopy:
# unique_countries = df['country'].unique()
# unique_countries = np.sort(unique_countries)
# np.savetxt('./data/performance/country_list.csv', unique_countries, delimiter=",")

# filter for Europe:
# europe = pd.read_csv('./data/performance/Europe_2.csv')
# europe = europe['Name'][:50].tolist()

# df_europe = df_complete[df_complete['country'].isin(europe)]
# df_europe.to_csv('./data/performance/df_europe.csv', sep=",")

# print("europe to csv after " + str(dt.datetime.now()-start))

# # filter for Germany
# df_germany = df_europe[df_europe['country'] == 'Germany']
# df_germany.to_csv('./data/performance/df_germany.csv', sep=",")

# print("Successful execution in " + str(dt.datetime.now()-start))

print(df)
