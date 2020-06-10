# Import library containing all needed libraries and user defined functions
from batch_process_functions import *

# Define Access Keys
AWS_ACCESS_KEY_ID=""
AWS_SECRET_ACCESS_KEY=""
POSTGRESQL_URL = ""
POSTGRESQL_TABLE = ""
POSTGRESQL_USER = ""
POSTGRESQL_PASSWORD = ""
S3_FILE = ""

def findNTA(longitude, latitude):
    '''
    Function determines the NTACode based on coordinates
    Outside of batch_process_functions.py due to module availability issues on worker nodes 
    INPUT: longitude => Double, latitude => Double
    OUTPUT: NTACode => String
    '''
    if longitude is None or latitude is None:
        # If nothing found, return unspecified
        return "USFD"
    else:
        # create a point object and check if the point is in the geometry
        longitude = float(longitude)
        latitude = float(latitude)
        point = Point(longitude, latitude)

        # Iterate through each geometry
        for _,row in nta_codes.iterrows():
            boundary = row['geometry']
            if point.within(boundary):
                return row["NTACode"]
        # If nothing found, return unspecified
        return "USFD" 


if __name__ == "__main__":

    # Define Spark Session and Spark Context
    spark = SparkSession \
            .builder\
            .appName("BatchTest")\
            .enableHiveSupport()\
            .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sc.addPyFile('batch_process_functions.py')

    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)

    # Define Schema for CSV to remove weird entities in the raw data
    desired_schema = defineSchemaHistoric()
    raw_df = spark.read.csv(S3_FILE, header=True, schema=desired_schema)

    # Select columns of interest
    select_df = raw_df.select("created_date", "complaint_type", "resolved", "borough", "latitude", "longitude")

    # Categorize ComplaintType into complaint buckets and overwrite the existing ComplaintType column and check borough names
    categorized_df = select_df.withColumn("complaint_type", categorizeComplaints_udf(col("complaint_type"))) \
                    .withColumn("borough", checkBoroughName_udf(col("borough")))

    # Create own timestamp to accelerate dataframe groupby calls
    timestamp_df = categorized_df.withColumn("created_date", createTimeStampHistoric_udf(col("created_date")))

    # Drop rows where created_date is blank
    timestamp_df = timestamp_df.filter(timestamp_df.created_date != "0-0-0")

    # Read nta code and broadcast
    shapely.speedups.enable()
    nta_codes = gpd.read_file('nta_processed.geojson')
    sc.broadcast(nta_codes)
    findNTA_udf = udf(findNTA, StringType())
    NTA_df = timestamp_df.withColumn("ntacode", findNTA_udf(col("longitude"), col("latitude")))
    processed_df = NTA_df.drop(*["longitude", "latitude"])

    # Group based on time parameters, NTACode, and complaint type and aggregate counts
    complaint_count_df = processed_df.groupby("created_date", "borough", "ntacode", "complaint_type") \
        .agg(F.count("complaint_type").alias("complaint_count")) \
        .select("created_date", "borough", "ntacode", "complaint_type", "complaint_count")

    # Create a map between the complaint type and its respective count and resolved count
    map_complaint_count_df = complaint_count_df.groupby("created_date", "borough", "ntacode", "complaint_type", "complaint_count") \
        .agg(F.create_map("complaint_type", "complaint_count").alias("complaint_map"))  \
        .select("created_date", "borough", "ntacode", "complaint_map")

    # Define UDF to combine array of maps into single map in pyspark dataframe
    combineMap = udf(lambda maps: {key:f[key] for f in maps for key in f},
                   MapType(StringType(),StringType()))

    # Group based on time parameters and NTA Code while combining maps with similar timestamp and NTACode
    group_complaint_df = map_complaint_count_df.groupby("created_date", "borough", "ntacode") \
        .agg(F.collect_list('complaint_map').alias("complaint_map")) \
        .select("created_date", "borough", "ntacode", combineMap('complaint_map').alias("complaint_map"))

    # Create unique key by appending the year, month, day and NTACode
    key_df = group_complaint_df.withColumn("created_date", createKey_udf(col("created_date"), col("borough"), col("ntacode"))) \
        .select(col("created_date").alias("key"), "borough", "ntacode", "complaint_map")

    # Extract year, month, day from timestamp
    time_df = key_df.withColumn("output", extractDateParams_udf(col("key")))\
        .select("key",'output.*', "borough", "ntacode", "complaint_map")

    # Convert mapped complaint count to JSON string
    complaints_json_df = time_df.withColumn('complaint_map', convertMaptoJSON_udf(col("complaint_map")))\
        .select("key", "year", "month", "borough", "ntacode", "complaint_map")

    # print(complaints_json_df.show())
    # complaints_json_df.printSchema()

    ### STORE
    complaints_json_df.write \
        .format("jdbc") \
        .option("url", POSTGRESQL_URL) \
        .option("dbtable", POSTGRESQL_TABLE) \
        .option("user", POSTGRESQL_USER) \
        .option("password", POSTGRESQL_PASSWORD) \
        .option("truncate", "true") \
        .mode("append") \
        .save()

   
    print("End of Script")
    spark.stop()