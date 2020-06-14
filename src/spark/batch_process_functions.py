'''
CODE DESCRIPTION: Contains functions and UDFs that are used in batch_process.py
'''

# Import PySpark Libraries
from __future__ import print_function
from pyspark.sql import *
from pyspark.sql.types import StringType, IntegerType, MapType, StructType, StructField, DoubleType
from pyspark.sql.functions import udf, broadcast, col
from pyspark.sql import functions as F

# Import Additional Libraries
import geopandas as gpd 
from shapely.geometry import Point, Polygon, shape
import shapely.speedups
import datetime as dt
import json

# Define Functions and PySpark UDFs 
def defineSchemaHistoric():
    '''
    Function to define schema to avoid messy row header data for analyzing historic data
    INPUT: None
    OUTPUT: schema => pyspark.sql StructType
    '''
    schema = StructType([StructField('unique_key', StringType(),True), 
        StructField('created_date', StringType(),True), 
        StructField('closed_date', StringType(),True), 
        StructField('agency', StringType(),True), 
        StructField('agency_name', StringType(),True), 
        StructField('complaint_type', StringType(),True),
        StructField('descriptor', StringType(),True),
        StructField('location_type', StringType(),True),
        StructField('incident_zip', StringType(),True),
        StructField('incident_address', StringType(),True),
        StructField('street_name', StringType(),True),
        StructField('cross_street1', StringType(),True), 
        StructField('cross_street2', StringType(),True), 
        StructField('intersection_street1', StringType(),True), 
        StructField('intersection_street2', StringType(),True), 
        StructField('address_type', StringType(),True),
        StructField('city', StringType(),True),
        StructField('landmark', StringType(),True),
        StructField('facility_type', StringType(),True),
        StructField('status', StringType(),True),
        StructField('due_date', StringType(),True),
        StructField('resolved description', StringType(),True), 
        StructField('resolved', StringType(),True), 
        StructField('community_board', StringType(),True), 
        StructField('BBL', StringType(),True), 
        StructField('borough', StringType(),True), 
        StructField('x', StringType(),True), 
        StructField('y', StringType(),True), 
        StructField('OpenChannel', StringType(), True),
        StructField('park_facility_name', StringType(),True),
        StructField('park_borough', StringType(),True),
        StructField('vehicle_type', StringType(),True),
        StructField('taxi_borough', StringType(),True),
        StructField('taxi_pickup', StringType(),True),
        StructField('bridge_highway_name', StringType(),True),
        StructField('bridge_highway_direction', StringType(),True), 
        StructField('road_ramp', StringType(),True),
        StructField('bridge_highway_segment', StringType(),True), 
        StructField('latitude', StringType(),True),
        StructField('longitude', StringType(),True), 
        StructField('location', StringType(),True)])
    return schema

def defineSchemaDaily():
    '''
    Function to define schema to avoid messy row header data for daily 311 data intake
    INPUT: None
    OUTPUT: schema => pyspark.sql StructType
    '''
    schema = StructType([StructField('created_date', StringType(),True), 
        StructField('complaint_type', StringType(),True), 
        StructField('resolved', StringType(),True), 
        StructField('borough', StringType(),True), 
        StructField('latitude', StringType(),True), 
        StructField('longitude', StringType(),True)])

    return schema

def categorizeComplaints(complaint_type):
    '''
    Function to categorize many complaints into buckets based on relevance
    INPUT: complaint_type => String
    OUTPUT: String
    '''
    try:
        complaint = complaint_type.lower()
    except:
        return "others"

    utilities = ["heat/hot water", "heating", "plumbing", "water", "water system", "electricity", "electric", "elevator", "appliance"]
    street_condition = ["street condition", "street light condition", "traffic signal condition", "sidewalk condition", "graffiti", "snow", "street sign - damaged", "curb condition", "traffic", "derelict vehicles", "water leak"]
    parking = ["parking - illegal parking", "blocked driveway"]
    construction = ["paint - plaster", "paint/plaster", "flooring/stairs", "flooring", "stairs", "lead"]
    unsanitary = ["unsanitary condition", "sewer, dirty conditions", "sanitary conditions", "rodent", "air quality", "standing water"]
    safety = ["safety", "food poisoning", "poisoning", "gun", "gunshot", "fireworks"]

    if "noise" in complaint:
        return "noise"
    elif complaint in utilities or "collection" in complaint:
        return "utilities"
    elif complaint in street_condition:
        return "street condition"
    elif complaint in parking or "parking" in complaint or "vehicle" in complaint:
        return "parking"
    elif "tree" in complaint:
        return "tree report"
    elif "homeless" in complaint:
        return "homeless report"
    elif complaint in construction or "construction" in complaint:
        return "construction services"
    elif "drug" in complaint:
        return "drug activity"
    elif complaint in unsanitary or "sanitary" in complaint:
        return "unsanitary conditions"
    elif complaint in safety or "emergency" in complaint:
        return "safety emergency"
    else:
        return "others"

def checkBoroughName(borough):
    '''
    Function to ensure borough names are consistent and short to increase read/write speed
    INPUT: borough => String
    OUTPUT: String
    '''
    try:
        borough = borough.lower()
    except:
        return "UN"

    if "queens" in borough:
        return "QN"
    elif "brooklyn" in borough:
        return "BK"
    elif "manhattan" in borough:
        return "MN"
    elif "bronx" in borough:
        return "BX"
    elif "staten island" in borough:
        return "SI"
    else:
        return "UN"

def createTimeStampHistoric(timestamp):
    '''
    Function that creates synthetic timestamp to easily perform groupby
    INPUT: timestamp => String
    OUTPUT: String 
    '''
    try:
        processed_timestamp = dt.datetime.strptime(timestamp, '%m/%d/%Y %I:%M:%S %p')
        year = str(processed_timestamp.year)
        month = str(processed_timestamp.month)
        day = str(processed_timestamp.day)
    
    except:
        year = '0'
        month = '0'
        day = '0'

    return year + '-' + month + '-' + day

def createTimeStampDaily(timestamp):
    '''
    Function that creates synthetic timestamp to easily perform groupby for daily 311 calls
    INPUT: timestamp => String
    OUTPUT: String comprising of year, month, day
    '''
    try:
        processed_timestamp = dt.datetime.strptime(timestamp[:-13], '%Y-%m-%d')
        year = str(processed_timestamp.year)
        month = str(processed_timestamp.month)
        day = str(processed_timestamp.day)
    
    except:
        year = '0'
        month = '0'
        day = '0'

    return year + '-' + month + '-' + day

def createKey(date, borough, ntacode):
    '''
    Function creates a unique key using timestamp and ntacode information
    INPUT: timestmap => String, borough => String, NTACode => String
    OUTPUT: String
    '''
    return date + '-' + borough + '-' + ntacode

def extractDateParams(date):
    '''
    Function that splits up year, month, and date
    INPUT: date => String
    OUTPUT: Row
    '''
    split_date = date.split('-')
    year = split_date[0]
    month = split_date[1]
    day = split_date[2]
    
    return Row('year', 'month', 'day') (year, month, day)

def convertMaptoJSON(data):
    '''
    Function converts data to a json string
    INPUT: data => MapType() or Dictionary
    OUTPUT: String
    '''
    return json.dumps(data)

# Declare UDFs
categorizeComplaints_udf = udf(categorizeComplaints, StringType())
checkBoroughName_udf = udf(checkBoroughName, StringType())
createTimeStampHistoric_udf = udf(createTimeStampHistoric, StringType())
createTimeStampDaily_udf = udf(createTimeStampDaily, StringType())
createKey_udf = udf(createKey, StringType())
extractDateParams_udf = udf(extractDateParams, StructType([
        StructField("year", StringType(), True), 
        StructField("month", StringType(), True), 
        StructField("day", StringType(), True)]))
convertMaptoJSON_udf = udf(convertMaptoJSON, StringType())

