import geopandas as gpd 

nta_code_geometry = gpd.read_file('nynta.geojson')
# Drop unused columns
nta_code_geometry = nta_code_geometry.drop(['county_fips', 'ntaname', 'shape_leng', 'shape_area', 'boro_name', 'boro_code'], axis=1)
nta_code_geometry = nta_code_geometry.rename(columns={'ntacode':'NTACode'})

nta_code_geometry.to_file('nta_processed.geojson', driver='GeoJSON')
