import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from geopy.point import Point

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
        # ToDO: Date format
            for k in [['quarter=1/', '01'], ['quarter=2/', '04'], ['quarter=3/', '07'], ['quarter=4/', '10']]:
                path_k = path_j + k[0]
                month = k[1]
                path_k = path_k + str(year)+"-"+month+"-01_performance_"+str(category)+"_tiles.parquet"
                test_raw = pd.read_parquet(path_k, engine='pyarrow')
                test_short = test_raw.head().copy()
                # add year, month and category information to the dataframe - those are
                test_short['year'] = year
                test_short['quarter_month'] = month
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
                test_short['year'] = year
                test_short['quarter_month'] = month
                test_short['category'] = category
                df = pd.concat([df, test_short])

# retrieve lat-long data from tile column
df['tile'] = df['tile'].apply(lambda x: x.replace("POLYGON", ""))
df['tile'] = df['tile'].apply(lambda x: x.replace("(", ""))
df['tile'] = df['tile'].apply(lambda x: x.replace(")", ""))
df['tile'] = df['tile'].apply(lambda x: x.replace(",", ""))
df[['long', 'lat', 'rest']] = df['tile'].str.split(pat=" ", n=2, expand=True)
df = df.drop(columns=['rest', 'tile', 'quadkey'])

# retrieve country from lat-long data
geolocator = Nominatim(user_agent="geoapiExercises")
# ToDo: use np.vectorize
df['location'] = df.apply(lambda x: geolocator.reverse(Point(x['lat'], x['long'])), axis=1)
# df['location'] = df.apply(lambda x: str(x['lat'])+','+str(x['long']), axis=1)
df['country'] = df['location'].apply(lambda x: x.raw['address'])
df['country'] = df['country'].apply(lambda x: x.get('country', np.nan))
# ToDo: add country filtering
print()
