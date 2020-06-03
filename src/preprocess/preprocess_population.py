import pandas as pd  
import numpy as np 

# Read population data and extract specific columns
pop_fields = ['Borough', 'Year', 'NTA Code', 'NTA Name', 'Population']
population_data = pd.read_csv('New_York_City_Population_By_Neighborhood_Tabulation_Areas.csv', usecols=pop_fields)

# Rename columns and reorganize the table
population_data = population_data.rename(columns={"Population": "Population - 2010", "NTA Code": "NTACode"})
population_data = population_data.drop(['Year'], axis = 1)

# Add annual growth rate to the table
# Growth rates were sourced on: 
growth = {'Bronx': 0.045, 'Brooklyn': 0.037, 'Manhattan': 0.033, 'Queens': 0.036, 'Staten Island': 0.039}
population_data['Growth Factor'] = population_data['Borough'].map(growth)

# Estimate population growth yearly from 2011 - 2020 for each NTA Area using the estimate borough growth rate
years = np.arange(2011, 2021)
for year in years:
    n = year - 2010
    col_name = "Population - " + str(year)
    population_data[col_name] = round(population_data['Population - 2010'] * (1+population_data['Growth Factor'])**n)
population_data = population_data.reindex(sorted(population_data.columns), axis=1)
population_data = population_data.set_index('NTACode')
population_data.to_csv('population_processed.csv', index=True)


