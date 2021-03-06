'''
CODE DESCRIPTION: Extract relevant columns from the NYC NTA geojson data to increase read speeds
Neighborhood Tabulation data were retrieved from: https://www1.nyc.gov/site/planning/data-maps/open-data/dwn-nynta.page
'''

# Import libraries
import geopandas as gpd 

nta_code_geometry = gpd.read_file('nynta.geojson')
# Drop unused columns to increase load speed
nta_code_geometry = nta_code_geometry.drop(['county_fips', 'ntaname', 'shape_leng', 'shape_area', 'boro_name', 'boro_code'], axis=1)
nta_code_geometry = nta_code_geometry.rename(columns={'ntacode':'NTACode'})
nta_code_geometry.to_file('nta_processed.geojson', driver='GeoJSON')
