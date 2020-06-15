'''
CODE DESCRIPTION: Retrieve new daily 311 calls and store it into S3 to be used
for daily batch processing
'''

# Import libraries to retrieve daily 311 calls
import datetime as dt
import pandas as pd
from sodapy import Socrata

# Import libraries to store files into S3
from io import StringIO # python3; python2: BytesIO 
import boto3

# Define S3 token and Socrata App Token
bucket = ""
SOCRATA_TOKEN = ""
csv_buffer = StringIO()

if __name__ == "__main__":
	# Get data between the current day and yesterday
	today_date = dt.date.today()
	yesterday_date = today_date - dt.timedelta(days=1)
	today_string = today_date.strftime("%Y-%m-%d")
	yesterday_string = yesterday_date.strftime("%Y-%m-%d")

	# Make Socrata API call
	client = Socrata("data.cityofnewyork.us", SOCRATA_TOKEN)
	client.timeout=60
	query_req = "created_date between '" + yesterday_string + "' and '" + today_string + "'"
	results = client.get("erm2-nwe9", select="created_date, complaint_type, resolution_action_updated_date, borough, latitude, longitude",where=query_req, limit=10000)
	results_df = pd.DataFrame(results)

	# print(results_df.head(1))
	
	# Save onto S3 as csv using the same name "daily311.csv"
	results_df.to_csv(csv_buffer, index=False)
	print("Saving to S3")

	s3_resource = boto3.resource('s3')
	s3_resource.Object(bucket, 'daily311.csv').put(Body=csv_buffer.getvalue())

	print("End of Script")