# import packages
import pandas as pd
import geopandas as gpd

# function to perform reverse geocoding
def conduct_reverse_geocoding(df: pd.DataFrame, gdf_shape):
    """
    enriches the dataframe with the federal states, counties and municipalities the long-lat data points are located in
    :param df: the germany dataframe with the long-lat data
    :param gdf_shape: shapefile with the federal state, county and municipality borders for Germany
    :return: df: original dataframe with additional country column
    """
    # convert df to GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.long, df.lat))

    # Merge DataFrames (checking if points are in polygons of shapefile)
    pointInPolys = gpd.sjoin(gdf, gdf_shape, how='left')

    return pointInPolys

# Read shapefiles as geodataframe
sf_federal_states = gpd.GeoDataFrame.from_file("./data/shapefiles/germany/federal_states/B-2020-AI001-2-5--AI0106--2023-01-03.shp")

sf_counties = gpd.GeoDataFrame.from_file("./data/shapefiles/germany/counties/K-2020-AI001-2-5--AI0106--2023-01-03.shp")

sf_municipalities = gpd.GeoDataFrame.from_file("./data/shapefiles/germany/municipalities/G-2020-AI001-2-5--AI0106--2023-01-03.shp")

# load germany data as df
df_germany = pd.read_csv("./data/final_data/germany_pre_geocoding.csv", sep=";")

# convert CRS of shapefiles to fit CRS of CRS in Germany file (EPSG)
sf_federal_states = sf_federal_states.to_crs(4326)
sf_counties = sf_counties.to_crs(4326)
sf_municipalities = sf_municipalities.to_crs(4326)

# Perform geocoding
df_germany = conduct_reverse_geocoding(df_germany, sf_federal_states)
df_germany.drop(columns=["index_right"], inplace=True)

df_germany = conduct_reverse_geocoding(df_germany, sf_counties)
df_germany.drop(columns=["index_right"], inplace=True)

df_germany = conduct_reverse_geocoding(df_germany, sf_municipalities)


# clean columsn from joining
df_germany.drop(columns=
["id_left",         
"schluessel_left",
"jahr_left",
"ai0106_left",
"Shape_Leng_left",
"Shape_Area_left",
"id_right", 
"schluessel_right",
"id_right",
"jahr_right", 
"Shape_Leng_right",
"Shape_Area_right",
"index_right",
"id",
"jahr", 
"Shape_Leng",
"Shape_Area",
"ai0106_right",
"schluessel",
"ai0106"
], inplace=True)

# clean other unecessary columns
df_germany.drop(columns=["long", "lat", "geometry"], inplace=True)

# rename newly added features for better readability
df_germany.rename(columns={"gen_left":"federal_state", "gen_right":"county", "gen":"municipality"}, inplace=True)

# save as csv
df_germany.to_csv("./data/final_data/germany_final.csv", sep=";", index=False)

