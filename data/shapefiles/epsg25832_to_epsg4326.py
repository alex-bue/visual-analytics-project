import geopandas as gpd

# Read shapefiles to geodataframes
sf_federal_states = gpd.read_file("data/shapefiles/germany/federal_states/B-2020-AI001-2-5--AI0106--2023-01-03.shp")

sf_counties = gpd.read_file("data/shapefiles/germany/counties/K-2020-AI001-2-5--AI0106--2023-01-03.shp")

sf_municipalities = gpd.read_file("data/shapefiles/germany/municipalities/G-2020-AI001-2-5--AI0106--2023-01-03.shp")

# Convert to EPSG 4326
sf_federal_states = sf_federal_states.to_crs('4326')
sf_counties = sf_counties.to_crs('4326')
sf_municipalities = sf_municipalities.to_crs('4326')

# Write new shapefiles
sf_federal_states.to_file("data/shapefiles/germany/converted_shapefiles/federal_states.shp")
sf_counties.to_file("data/shapefiles/germany/converted_shapefiles/counties.shp")
sf_municipalities.to_file("data/shapefiles/germany/converted_shapefiles/municipalities.shp")