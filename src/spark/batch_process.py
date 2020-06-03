from __future__ import print_function
from pyspark.sql import *
from pyspark.sql.types import StringType, IntegerType, MapType, StructType, StructField, DoubleType
from pyspark.sql.functions import udf, broadcast, col
from pyspark.sql import functions as F

import geopandas as gpd 
from shapely.geometry import Point, Polygon, shape
import shapely.speedups
import datetime as dt
import json

# Define Access Keys
AWS_ACCESS_KEY_ID=""
AWS_SECRET_ACCESS_KEY=""
POSTGRESQL_URL = ""
POSTGRESQL_TABLE = ""
POSTGRESQL_USER = ""
POSTGRESQL_PASSWORD = ""
S3_FILE = ""

# Define Functions and PySpark UDFs 

def defineSchema():
    '''
    Function to define schema to avoid messy row header data
    INPUT: 
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
        StructField('resolution_date', StringType(),True), 
        StructField('community_board', StringType(),True), 
        StructField('borough', StringType(),True), 
        StructField('x', StringType(),True), 
        StructField('y', StringType(),True), 
        StructField('park_facility_name', StringType(),True),
        StructField('park_borough', StringType(),True),
        StructField('vehicle_type', StringType(),True),
        StructField('taxi_borough', StringType(),True),
        StructField('taxi_pickup', StringType(),True),
        StructField('bridge_highway_name', StringType(),True),
        StructField('bridge_highway_direction', StringType(),True), 
        StructField('road_ramp', StringType(),True),
        StructField('bridge_highway_segment', StringType(),True), 
        StructField('latitude', DoubleType(),True),
        StructField('longitude', DoubleType(),True), 
        StructField('location', StringType(),True)])
    return schema

def categorizeComplaints(complaint_type):
    '''
    Function to categorize many complaints into buckets based on relevance
    INPUT: complaint_type => String
    OUTPUT: complaint => String
    '''
    complaint = complaint_type.lower()
    
    utilities = ["heat/hot water", "heating", "plumbing", "water", "water system", "electricity", "electric"]
    street_condition = ["street condition", "street light condition", "traffic signal condition", "sidewalk condition", "graffiti", "snow", "street sign - damaged", "curb condition", "traffic", "derelict vehicles", "water leak"]
    parking = ["parking - illegal parking", "blocked driveway"]
    construction = ["paint - plaster", "paint/plaster"]
    unsanitary = ["unsanitary condition", "sewer, dirty conditions", "sanitary conditions", "rodent", "air quality"]
    
    if "noise" in complaint:
        return "noise"
    elif complaint in utilities:
        return "utilities"
    elif complaint in street_condition:
        return "street condition"
    elif complaint in parking or "parking" in complaint:
        return "parking"
    elif "tree" in complaint:
        return "tree report"
    elif "homeless" in complaint:
        return "homeless report"
    elif complaint in construction or "construction" in complaint:
        return "construction services"
    elif "drug" in complaint:
        return "drug activity"
    elif complaint in unsanitary:
        return "unsanitary conditions"
    else:
        return "miscellaneous concern"

def checkBoroughName(borough):
    borough = borough.lower()
    if "queens" in borough:
        return "queens"
    elif "brooklyn" in borough:
        return "brooklyn"
    elif "manhattan" in borough:
        return "manhattan"
    elif "bronx" in borough:
        return "bronx"
    elif "staten island" in borough:
        return "staten island"
    else:
        return "unspecified"
    
def checkResolvedIn7Days(created_date, resolved_date):
    '''
    Function which checks if the issue was resolved in less than or equal to 7 days
    INPUT: created_date => String, resolved_date => String
    OUTPUT: True/False => Int
    '''
    date_1 = dt.datetime.strptime(created_date, '%m/%d/%Y %I:%M:%S %p')
    try:
        date_2 = dt.datetime.strptime(resolved_date, '%m/%d/%Y %I:%M:%S %p')
        if (date_2 - date_1).days <= 7:
            return 1
        else:
            return 0
    except:
        return 0
    
def extractYear(created_date):
    '''
    Function extracts year from datetime string
    INPUT: created_date => String
    OUTPUT: year => String
    '''
    year = str(dt.datetime.strptime(created_date, '%m/%d/%Y %I:%M:%S %p').year)
    return year

def extractMonth(created_date):
    '''
    Function extracts month from datetime string
    INPUT: created_date => String
    OUTPUT: month => String
    '''
    month = str(dt.datetime.strptime(created_date, '%m/%d/%Y %I:%M:%S %p').month)
    return month

def extractDay(created_date):
    '''
    Function extracts day from datetime string
    INPUT: created_date => String
    OUTPUT: day => String
    '''
    day = str(dt.datetime.strptime(created_date, '%m/%d/%Y %I:%M:%S %p').day)
    return day

def extractHour(created_date):
    '''
    Function extracts hour from datetime string
    INPUT: created_date => String
    OUTPUT: hour => String
    '''
    hour = str(dt.datetime.strptime(created_date, '%m/%d/%Y %I:%M:%S %p').hour)
    return hour


def findNTA(longitude, latitude):
    '''
    Function determines the NTACode given the complaint's coordinates
    INPUT: longitude => Double, latitude => Double
    OUTPUT: NTACode => String
    '''

    if longitude is None or latitude is None:
        return "unspecified"
    else:
        point = Point(longitude, latitude)

        # Iterate through each geometry
        for _,row in nta_codes.iterrows():
            boundary = row['geometry']
            if point.within(boundary):
                return row["NTACode"]
        # If nothing found
        return "unspecified" 

def createKey(year, month, day, NTACode):
    return year + '_' + month + '_' + day + '_' + NTACode


def convertMaptoJSON(mapDataType):
    return json.dumps(mapDataType)



if __name__ == "__main__":

    # Define Spark Session and Spark Context
    spark = SparkSession \
            .builder\
            .appName("BatchTest")\
            .enableHiveSupport()\
            .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)

    # Define Schema for CSV (Raw schema contains weird entities)
    desired_schema = defineSchema()
    raw_df = spark.read.csv(S3_FILE, header=True, schema=desired_schema)

    # Select columns of interest
    select_df = raw_df.select("created_date", "complaint_type", "resolution_date", "borough", "latitude", "longitude")
    
    # Categorize ComplaintType into complaint buckets and overwrite the existing ComplaintType column and check borough names
    categorizeComplaints_udf = udf(categorizeComplaints, StringType())
    checkBoroughName_udf = udf(checkBoroughName, StringType())
    # categorized_df = select_df.withColumn("complaint_type", categorizeComplaints_udf(col("complaint_type")))
    categorized_df = select_df.withColumn("complaint_type", categorizeComplaints_udf(col("complaint_type"))).withColumn("borough", checkBoroughName_udf(col("borough")))

    # Determine if the issue was resolved in 7 days and add as a new column
    checkResolvedIn7Days_udf = udf(checkResolvedIn7Days, IntegerType())
    resolved_df = categorized_df.withColumn("resolved_7", checkResolvedIn7Days_udf(col("created_date"), col("resolution_date")))

    # Extract year, month, day, hour from the CreatedDate timestamp
    extractYear_udf = udf(extractYear, StringType())
    extractMonth_udf = udf(extractMonth, StringType())
    extractDay_udf = udf(extractDay, StringType())
    extractHour_udf = udf(extractHour, StringType())

    time_df = resolved_df.withColumn("year", extractYear_udf(col("created_date"))) \
        .withColumn("month", extractMonth_udf(col("created_date"))) \
        .withColumn("day", extractDay_udf(col("created_date"))) \
        .withColumn("hour", extractHour_udf(col("created_date"))) 

    time_df = time_df.drop(*['created_date', 'resolution_date'])
    
    # Read nta code and broadcast
    shapely.speedups.enable()
    nta_codes = gpd.read_file('nta_processed.geojson')
    sc.broadcast(nta_codes)

    findNTA_udf = udf(findNTA, StringType())
    NTA_df = time_df.withColumn("ntacode", findNTA_udf(col("longitude"), col("latitude")))
    
    # Drop lon and lat
    processed_df = NTA_df.drop(*["longitude", "latitude"])

    ##### TRANSFORM
    # Group based on time parameters, NTACode, and complaint type and aggregate counts
    complaint_count_df = processed_df.groupby("year", "month", "day", "hour", "ntacode", "borough","complaint_type") \
        .agg(F.count("complaint_type").alias("complaint_count"), \
            F.sum("resolved_7").alias("resolved_7_count")) \
        .select("year", "month", "day", "hour", "ntacode", "borough", "complaint_type", "complaint_count", "resolved_7_count")

    # Create a map between the complaint type and its respective count and resolved count
    map_complaint_count_df = complaint_count_df.groupby("year", "month", "day", "hour", "ntacode", "borough", "complaint_type", "complaint_count", "resolved_7_count") \
        .agg(F.create_map("complaint_type", "complaint_count").alias("complaint_map"), 
            F.create_map("complaint_type", "resolved_7_count").alias("resolved_7_map"))

    # Define UDF to combine array of maps into single map in pyspark dataframe
    combineMap = udf(lambda maps: {key:f[key] for f in maps for key in f},
                   MapType(StringType(),StringType()))

    # Group based on time parameters and NTA Code while combining maps with similar timestamp and NTACode
    group_complaint_df = map_complaint_count_df.groupby("year", "month", "day", "hour", "ntacode", "borough") \
        .agg(F.collect_list('complaint_map').alias("complaint_map"), \
            F.collect_list('resolved_7_map').alias("resolved_7_map")) \
        .select("year", "month", "day", "hour", "ntacode", "borough", combineMap('complaint_map').alias("complaint_map"), combineMap('resolved_7_map').alias("resolved_7_map"))

    # Create unique key by appending the year, month, day and NTACode
    createKey_udf = udf(createKey, StringType())
    key_df = group_complaint_df.withColumn("key", createKey_udf(col("year"), col("month"), col("day"),col("ntacode")))
    
    # Convert mapped complaint count to JSON string
    convertMaptoJSON_udf = udf(convertMaptoJSON, StringType())
    complaints_json_df = key_df.withColumn('complaints', convertMaptoJSON_udf(col("complaint_map"))).withColumn('resolved', convertMaptoJSON_udf(col("resolved_7_map")))
    complaints_json_df = complaints_json_df.drop(*['complaint_map', 'resolved_7_map'])

    # Map hours to complaints
    map_hour_complaint_df = complaints_json_df.groupby("key", "year", "month", "day", "hour", "ntacode", "borough", "complaints","resolved") \
    .agg(F.create_map('hour', 'complaints').alias("hourly_complaint_map")) \
    .select("key", "year", "month", "day", "hour", "ntacode", "borough", "hourly_complaint_map",'resolved')

    # Group based on the date and nta code
    group_hour_complaint_df = map_hour_complaint_df.groupby("key","year", "month", "day", "ntacode", "borough") \
    .agg(F.collect_list('hourly_complaint_map').alias("hourly_complaint"), F.collect_list('resolved').alias('resolved')) \
    .select("key","year", "month", "day", "ntacode", "borough", combineMap('hourly_complaint').alias("hourly_complaint"), 'resolved')

    # Convert the grouped by hour-complaint map to JSON string for each storing
    daily_complaint_df = group_hour_complaint_df.withColumn('hourly_complaint', convertMaptoJSON_udf(col("hourly_complaint"))).withColumn('resolved', convertMaptoJSON_udf(col("hourly_complaint")))

    # Print to check functionality
    print(daily_complaint_df.show())
    daily_complaint_df.printSchema()


    ##### STORE
    daily_complaint_df.write \
        .format("jdbc") \
        .option("url", POSTGRESQL_URL) \
        .option("dbtable", POSTGRESQL_TABLE) \
        .option("user", POSTGRESQL_USER) \
        .option("password", POSTGRESQL_PASSWORD) \
        .option("truncate", "true") \
        .mode("overwrite") \
        .save()

    print("End of Script")
   
    spark.stop()