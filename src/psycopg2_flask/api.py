# Import libraries 
import psycopg2
import json
import flask
from flask import request, jsonify
import pandas as pd

'''
	NOTES ABOUT THE DATABASE:
	('2014_6_4_BK17', '2014', '6', '4', 'BK17', 'brooklyn', '{"1": "{\\"unsanitary conditions\\": 1}"}', '"{\\"1\\": \\"{\\\\\\"unsanitary conditions\\\\\\": 1}\\"}"')
	   	key = entry[0]
	    year = entry[1]
	    month = entry[2]
	    day = entry[3]
	    ntacode = entry[4]
	    borough = entry[5]
	    complaints = entry[6]
	    resolved=entry[7]

	NOTES ABOUT COMPLAINT BUCKET:
		noise - noise
		utilities - utilities
		street_condition - street condition
		parking - parking
		tree_report - tree 
		homeless - homeless report
		construction_services = construction services 
		drug - drug activity 
		unsanitary_condition - unsanitary condition
		miscellaneous - miscellaneous concern
'''

POSTGRES_DB = ""
POSTGRES_USER = ""
POSTGRES_PASSWORD = ""
POSTGRES_HOST = ""
POSTGRES_PORT = ""

# Define helper functions for API calls
def unpackComplaints(complaints):
    '''
    Function to unpack nested json string from database
    INPUT: complaints => String
    OUTPUT: complaints => Dictionary (nested)
    '''
    complaints = json.loads(complaints)
    for key in complaints.keys():
        complaints[key] = json.loads(complaints[key])
    return complaints

def sumComplaints(query_result):
    '''
    Function to sum all complaints counts based on complaint type
    INPUT: query_result => JSON
    OUTPUT: total_complaints_dict => Dictionary
    OUTPUT SAMPLE: {'construction services': 1, 'unsanitary conditions': 3, 'utilities': 1, 'miscellaneous concern': 1}
    '''

    total_complaints_dict = {}
    for row in query_result:
        raw_complaints = row[6]

        # Unpack nested JSON strings
        complaints = unpackComplaints(raw_complaints)

        # print(complaints)
        # Retrieve dictionary from hour objects
        complaint_values_dict = list(complaints.values())[0]

        if total_complaints_dict == {}:
        # if the dictionary is empty, then save the values 
            total_complaints_dict = complaint_values_dict
        else:
        # if the dictionary is not empty, then sum the counter_dict and the current values
            for k in set(total_complaints_dict) | set(complaint_values_dict):
                # Find union of keys in both dictionaries and define new dictionary that uses key from union and adds the values
                total_complaints_dict[k] = total_complaints_dict.get(k, 0) + complaint_values_dict.get(k, 0)
			
    return total_complaints_dict

def sumNTAComplaints(query_result):
    '''
    Function to sum all complaints counts based on a first level of NTACode and then a second level of complaint type
    INPUT: query_result => JSON
    OUTPUT: total_NTA_complaints_dict => Dictionary (nested)
    OUTPUT SAMPLE: {BX01: {'construction services': 1, 'unsanitary conditions': 3}, BX02: 'construction services': 1, 'unsanitary conditions': 3}
    '''

    total_NTA_complaints_dict = {}
    for row in query_result:
        nta_code = row[4]
        raw_complaints = row[6]

        # Unpack nested JSON strings 
        complaints = unpackComplaints(raw_complaints)

        # Retrieve dictionary from hour objects
        complaint_values_dict = list(complaints.values())[0]

        if nta_code not in total_NTA_complaints_dict.keys():
            # if the nta code is not in the dictionary, then add the entire complaints dict 
            total_NTA_complaints_dict[nta_code] = complaint_values_dict 
        else:
			# if the nta code is already in the dictionary, then sum the contents of the dictionary
            for k in set(total_NTA_complaints_dict[nta_code]) | set(complaint_values_dict):
                total_NTA_complaints_dict[nta_code][k] = total_NTA_complaints_dict[nta_code].get(k, 0) + complaint_values_dict.get(k, 0)

	# go through all keys in total_NTA_complaints_dict and check if they are there compared to the entire nta code list
    for nta_code in full_nta_codes:
        if nta_code not in total_NTA_complaints_dict.keys():
            total_NTA_complaints_dict[nta_code] = {}

    return total_NTA_complaints_dict


def sumNTAComplaintsCount(query_result):
    '''
    Function to count total complaints regardless of complaint type for each NTA Code
    INPUT: query_result => JSON
    OUTPUT: agg_NTA_complaints_dict => Dictionary 
    OUTPUT SAMPLE: {BX01: 4, BX02: 2}
    '''

	# Count total of complaints regardless of complaint type for each NTA Code
    agg_NTA_complaints_dict = {}
    for row in query_result:
        nta_code = row[4]
        raw_complaints = row[6]

        # Unpack nested JSON strings 
        complaints = unpackComplaints(raw_complaints)

        # Remove dependency on hour
        complaint_values_dict = list(complaints.values())[0]
        complaint_count_list = list(complaint_values_dict.values())
        sum_complaint_count = sum(complaint_count_list)
        # print(sum_complaint_count)

        if nta_code not in agg_NTA_complaints_dict.keys():
            # if the nta code is not in the dictionary, set its value to the current sum_complaint_count
            agg_NTA_complaints_dict[nta_code] = sum_complaint_count
        else:
            agg_NTA_complaints_dict[nta_code] += sum_complaint_count

    # go through all keys in agg_NTA_complaints_dict and check if they are there compared to the entire nta code list
    for nta_code in full_nta_codes:
        if nta_code not in agg_NTA_complaints_dict.keys():
            agg_NTA_complaints_dict[nta_code] = 0

    return agg_NTA_complaints_dict


def sumBoroughComplaints(query_result):
    '''
    Function to count complaints based on a first level of borough and the second level of complaint type
    INPUT: query_result => JSON
    OUTPUT: total_borough_complaints_dict => Dictionary (nested)
    OUTPUT SAMPLE: {'brooklyn': {'noise': 1}, 'queens': {'street condition'}: 2}
    '''

    total_borough_complaints_dict = {}
    for row in query_result:
        borough = row[5]
        raw_complaints = row[6]

        # Unpack nested JSON strings 
        complaints = unpackComplaints(raw_complaints)

		# Remove dependency on hour
        complaint_values_dict = list(complaints.values())[0]

        if borough not in total_borough_complaints_dict.keys():
            total_borough_complaints_dict[borough] = complaint_values_dict
        else:
            for k in set(total_borough_complaints_dict[borough]) | set(complaint_values_dict):
                total_borough_complaints_dict[borough][k] = total_borough_complaints_dict[borough].get(k, 0) + complaint_values_dict.get(k, 0)
    
    return total_borough_complaints_dict


############################################################################################################################################################

# Start flask app
app = flask.Flask(__name__)
app.config["DEBUG"] = True

# Define HOME PAGE (SANITY CHECK)
@app.route('/', methods=['GET'])
def home():
    return "<h1>Home Page Test for API Calls.</p>"


@app.route('/data/complaintType/all', methods=['GET'])
def getAllComplaints():
	'''
	API CALL to retrieve all complaint types for the entire city regardless of borough and time
    HELPER: sumComplaints()
    CALL SAMPLE: /data/complaintType/all?year=2014&month=6&day=1
    OUTPUT: JSON
    OUTPUT SAMPLE: {
      "construction services": 5, 
      "miscellaneous concern": 15, 
      "street condition": 6, 
      "unsanitary conditions": 18, 
      "utilities": 21
    }
	'''

	query_parameters = request.args 
	year = query_parameters.get('year')
	month = query_parameters.get('month')
	day = query_parameters.get('day')
	borough = query_parameters.get('borough')

	query = "SELECT * FROM test311 WHERE "

	if year:
		query += "year='" + year + "' AND " 
	if month:
		query += "month='" + month + "' AND " 
	if day:
		query += "day='" + day + "' AND "  
	if borough:
		query += "borough='" + borough + "' AND "  

	# delete trailing AND 
	query = query[:-5]
	cur.execute(query)
	rows = cur.fetchall()
	results = sumComplaints(rows)
	return jsonify(results)


@app.route('/data/complaintType/borough', methods=['GET'])
def getBoroughComplaints():
    '''
    API CALL to retrieve all complaint types for the entire city grouped by Borough and complaint type
    HELPER: sumBoroughComplaints()
    CALL SAMPLE: /data/complaintType/borough?year=2014&month=6&day=1
    OUTPUT: JSON
    OUTPUT SAMPLE: {
      "bronx": {
        "construction services": 1, 
        "miscellaneous concern": 3, 
        "street condition": 2, 
        "unsanitary conditions": 5, 
        "utilities": 4
      }, 
      "brooklyn": {
        "construction services": 4, 
        "miscellaneous concern": 6, 
        "street condition": 1, 
        "unsanitary conditions": 7, 
        "utilities": 9
      }, 
      "manhattan": {
        "miscellaneous concern": 2, 
        "unsanitary conditions": 3, 
        "utilities": 5
      }, 
      "queens": {
        "miscellaneous concern": 3, 
        "street condition": 3, 
        "unsanitary conditions": 3, 
        "utilities": 2
      }, 
      "staten island": {
        "miscellaneous concern": 1, 
        "utilities": 1
      }
    }
    '''

    query_parameters = request.args

    year = query_parameters.get('year')
    month = query_parameters.get('month')
    day = query_parameters.get('day')
    borough = query_parameters.get('borough')

    query = "SELECT * FROM test311 WHERE "

    if year:
        query += "year='" + year + "' AND " 
    if month:
        query += "month='" + month + "' AND " 
    if day:
        query += "day='" + day + "' AND "  
    if borough:
        query += "borough='" + borough + "' AND "  

    # delete trailing AND 
    query = query[:-5]
    cur.execute(query)
    rows = cur.fetchall()
    results = sumBoroughComplaints(rows)
    return jsonify(results)


@app.route('/data/complaintType/nta', methods=['GET'])
def getNTAComplaints():
    '''
    API CALL to retrieve all complaint types for the entire city grouped by NTACode and complaint type
    NOTE: NTACode without any complaint will be empty
    HELPER: sumNTAComplaints()
    CALL SAMPLE: /data/complaintType/nta?year=2014&month=6&day=1
    OUTPUT: JSON
    OUTPUT SAMPLE: {
      "BX01": {
        "construction services": 1, 
        "miscellaneous concern": 3, 
        "street condition": 2, 
        "unsanitary conditions": 5, 
        "utilities": 4
      }
    '''
    query_parameters = request.args

    year = query_parameters.get('year')
    month = query_parameters.get('month')
    day = query_parameters.get('day')
    borough = query_parameters.get('borough')

    query = "SELECT * FROM test311 WHERE "

    if year:
        query += "year='" + year + "' AND " 
    if month:
        query += "month='" + month + "' AND " 
    if day:
        query += "day='" + day + "' AND "  
    if borough:
        query += "borough='" + borough + "' AND "  

	# delete trailing AND 
    query = query[:-5]
    cur.execute(query)
    rows = cur.fetchall()
    results = sumNTAComplaints(rows)
    return jsonify(results)

# /data/ntaComplaintsAggregate?year=2014&month=6&day=1
@app.route('/data/complaintCount/nta', methods=['GET'])
def getNTAComplaintsCount():
    '''
    API CALL to retrieve all complaint counts for the entire city grouped by NTACode 
    NOTE: NTACode without any complaint will be set to 0
    HELPER: sumNTAComplaintsCount()
    CALL SAMPLE: /data/complaintCount/nta?year=2014&month=6&day=1
    OUTPUT: JSON
    OUTPUT SAMPLE: {
      "BK09": 1, 
      "BK17": 2, 
      "BK19": 0, 
      "BK21": 0, 
      "BK23": 0, ...
    }
    '''
    query_parameters = request.args

    year = query_parameters.get('year')
    month = query_parameters.get('month')
    day = query_parameters.get('day')
    borough = query_parameters.get('borough')

    query = "SELECT * FROM test311 WHERE "

    if year:
        query += "year='" + year + "' AND " 
    if month:
        query += "month='" + month + "' AND " 
    if day:
        query += "day='" + day + "' AND "  
    if borough:
        query += "borough='" + borough + "' AND "  

	# delete trailing AND 
    query = query[:-5]
    cur.execute(query)
    rows = cur.fetchall()
    results = sumNTAComplaintsCount(rows)
    return jsonify(results)


if __name__ == "__main__":

	# Load nta_codes 
	full_nta_codes = list(pd.read_csv('population_processed.csv')['NTACode'])

	# Connect to the database
	con = psycopg2.connect(
	    database=POSTGRES_DB, # database name
	    user=POSTGRES_USER, # user
	    password=POSTGRES_PASSWORD, # password
	    host=POSTGRES_HOST, # endpoint
	    port=POSTGRES_PORT)

	cur = con.cursor()

	##### TESTING FUNCTIONS
	# execute query 
	# cur.execute("SELECT * FROM test311 WHERE year='2014' AND month='5' LIMIT 5")
	# cur.execute("SELECT * FROM test311 LIMIT 5")
	# rows = cur.fetchall()


	# test = sumComplaints(rows)
	# print(test)

	# ntatest = sumNTAComplaints(rows)
	# print(ntatest)

	# boroughtest = sumBoroughComplaints(rows)
	# print(boroughtest)

	# nta_test_total = sumNTAComplaintsCount(rows)
	# print(nta_test_total)


    # Define the flask app to run on machine's IP Address
	app.run(host='0.0.0.0', debug=True)

	print("End of Script")


