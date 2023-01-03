import pandas as pd
import geopandas as gpd


def conduct_reverse_geocoding(df: pd.DataFrame, gdf_shape):
    """
    enriches the dataframe with the federal states, counties and municipalities the long-lat data points are located in
    :param df: the germany dataframe with the long-lat data
    :param gdf_shape: shapefile with the federal state, county and municipality borders for Germany
    :return: df: original dataframe with additional country column
    """
    # convert df to GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.long, df.lat))

    # Merge DataFrames
    pointInPolys = gpd.sjoin(gdf, gdf_shape, how='left')

    # Drop columns we don't need
    # pointInPolys = pointInPolys.drop(
    #     columns=['french_shor', 'status', 'index_right', 'color_code', 'status'])
    return pointInPolys


shapefile = gpd.GeoDataFrame.from_file("../../Documents/Master/Semester1/Visual_Analytics/"
                                       "G-2020-AI001-2-5--AI0109--2023-01-03/G-2020-AI001-2-5--AI0109--2023-01-03.shp")

df_germany = pd.read_csv("../../Documents/Master/Semester1/Visual_Analytics/Data_Input_final/germany_final_2.csv")

df_germany = conduct_reverse_geocoding(df_germany, shapefile)
