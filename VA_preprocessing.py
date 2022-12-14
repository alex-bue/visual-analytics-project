import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from geopy.point import Point
import datetime as dt

start = dt.datetime.now()
# read in all single files and concatenate them
path = '../../Documents/Master/Semester1/Visual_Analytics/Dashboard_Files/'
df = pd.DataFrame()
for i in [["type=fixed/", "fixed"], ["type=mobile/", "mobile"]]:
    path_i = path + i[0]
    category = i[1]
    for j in [['year=2019/', 2019], ['year=2020/', 2020], ['year=2021/', 2021], ['year=2022/', 2022]]:
        path_j = path_i + j[0]
        year = j[1]
        if year != 2022:
            for k in [['quarter=1/', '01'], ['quarter=2/', '04'], ['quarter=3/', '07'], ['quarter=4/', '10']]:
                path_k = path_j + k[0]
                month = k[1]
                path_k = path_k + str(year)+"-"+month+"-01_performance_"+str(category)+"_tiles.parquet"
                test_raw = pd.read_parquet(path_k, engine='pyarrow')
                test_short = test_raw.head().copy()
                # add year, month and category information to the dataframe - those are
                test_short['quarter'] = dt.date(year, int(month), 1)
                test_short['category'] = category
                df = pd.concat([df, test_short])
        elif year == 2022:
            for k in [['quarter=1/', '01'], ['quarter=2/', '04'], ['quarter=3/', '07']]:
                path_k = path_j + k[0]
                month = k[1]
                path_k = path_k + str(year) + "-" + month + "-01_performance_" + str(category) + "_tiles.parquet"
                test_raw = pd.read_parquet(path_k, engine='pyarrow')
                test_short = test_raw.head().copy()
                # add year, month and category information to the dataframe - those are
                test_short['quarter'] = dt.date(year, int(month), 1)
                test_short['category'] = category
                df = pd.concat([df, test_short])
print("data read in completed in " + str(dt.datetime.now()-start))

# df_raw = pd.read_parquet('../../Documents/Master/Semester1/Visual_Analytics/Dashboard_Files/type=fixed/'
#                          'year=2019/quarter=1/2019-01-01_performance_fixed_tiles.parquet', engine = 'pyarrow')
# df = df_raw.head()
# retrieve lat-long data from tile column
df['tile'] = df['tile'].apply(lambda x: x.replace("POLYGON", ""))
df['tile'] = df['tile'].apply(lambda x: x.replace("(", ""))
df['tile'] = df['tile'].apply(lambda x: x.replace(")", ""))
df['tile'] = df['tile'].apply(lambda x: x.replace(",", ""))
df[['long', 'lat', 'rest']] = df['tile'].str.split(pat=" ", n=2, expand=True)
df = df.drop(columns=['rest', 'tile', 'quadkey'])

print("extraction of lat-long data ready after " + str(dt.datetime.now()-start))

# retrieve country from lat-long data
geolocator = Nominatim(user_agent="geoapiExercises")
# ToDo: use np.vectorize
df['location'] = df.apply(lambda x: geolocator.reverse(Point(x['lat'], x['long'])), axis=1)

print("geolocator.reverse function ready after " + str(dt.datetime.now()-start))

df['country'] = df['location'].apply(lambda x: x.raw['address'])
df['country'] = df['country'].apply(lambda x: x.get('country', np.nan))

print("data preparation ready after " + str(dt.datetime.now()-start))


# save the whole dataset as csv:
df.to_csv('../../Documents/Master/Semester1/Visual_Analytics/df_whole_world.csv', sep=",")

print("whole world to csv after " + str(dt.datetime.now()-start))

# get a list of all countries in geopy:
#unique_countries = df['country'].unique()
#unique_countries = np.sort(unique_countries)
# np.savetxt('../../Documents/Master/Semester1/Visual_Analytics/country_list.csv', unique_countries, delimiter=",")

# filter for Europe:
europe = pd.read_csv('../../Documents/Master/Semester1/Visual_Analytics/Europe_2.csv')
europe = europe['Name'][:50].tolist()

df_europe = df[df['country'].isin(europe)]
df_europe.to_csv('../../Documents/Master/Semester1/Visual_Analytics/df_europe.csv', sep=",")

print("europe to csv after " + str(dt.datetime.now()-start))

# filter for Germany
df_germany = df_europe[df_europe['country'] == 'Germany']
df_germany.to_csv('../../Documents/Master/Semester1/Visual_Analytics/df_germany.csv', sep=",")

print("Successful execution in " + str(dt.datetime.now()-start))


