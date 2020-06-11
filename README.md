# CityWatch - Creating Smarter Cities Using the 311

## Table of Contents

## Introduction
With increasing urban growth in large metropolitan areas, maintaining city infrastructure becomes a main concern for local municipal governments. Broken and wearing infrastructure costs everyone time and money. Fortunately, these infrastructure or social concerns are reported through 311 calls. In NYC alone, there have been 22 million 311 calls (from 2010 to June 2020), ranging from different complaint types. 

The main objective of this project aims to create a database API and platform that provides aggregated 311 data summaries and trends based on spatial and temporal parameters including the specific 311 complaint type. The potential use case would be to allow local government agencies and urban planners a way to quickly see 311 trends based on a common complaint type for a given neighborhood over a period of time. Hence, these stakeholders can use this information to properly allocate municipal resources in real-time. 

## Data Set
The data used for this project was retrieved from NYC Open Data. Historical 311 calls from 2010 to June 2020 were retrieved as a 13 GB CSV file from the NYC Open Data website. Daily new 311 calls were retrieved using Socrata API and were stored directly into S3. 

A summarized view of the raw 311 data is shown below. 

<image1 - Raw Data 311>

In addition to the 311 Data set, the coordinates for each 311 data call is mapped to a specific neighborhood using NTA geometries. An NTA geometry represents a neighborhood in NYC indicating the neighborhood's code, name, and geometric shape. An example view of NYC split into NTA geometries are shown below. 

<image2 - NTA geometry> 

Population data was also used to determine the number of 311 calls per capita. This was retrieved from census data. Using a forecasted growth rate for each borough, population from 2010 onwards was estimated for 2011-2020.  

<image3 - Population> 



## Data Pipeline
The ETL pipeline used to analyze this data is shown below. AWS EC2 was used to host the entire data pipeline consisting of a Spark Cluster, PostgreSQL database, Flask API instance, and a Front-end instance. 

<image4 - Pipeline> 

### Data Ingestion Layer (S3, EC2, Airflow)
- Historical 311 raw data was first preprocessed to extract the columns of interest and was directly uploaded to S3 where it will be accessed during batch processing. 
- Daily 311 data was retrieved using a daily scheduled workflow through Airflow which retrieves 311 day every midnight and saves the data onto S3. 

### Data Computation (Spark, Airflow)
- Spark was used to perform a series of data transformation on both the historical and daily 311 data. These include the following:
	- Cleaning the data
		- Ensuring borough names are consistent (remove human error from data entry)
		- Categorize complaint types based on common complaints (noise-residential, noise-commerical => Noise)
	- Map coordinates of 311 calls into NTA Codes
		- Using geopandas, the coordinates for each 311 call was mapped into an NTA code representing the neighborhood that the 311 call occurred or is referenced to
	- Group and sort based on date, NTA Code, complaint type and perform aggregated count.
		- Aggregated based on date, NTA Code, and then created a complaint map representing a dictionary containing the specific complaint type and its respective count. 
- A sample output of the spark computation is shown in the image below. 

<image5 - Spark Output>

