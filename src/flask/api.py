'''
CODE DESCRIPTION: Defines all the database API endpoints to query the database. 
'''

# Import libraries 
import psycopg2
import json
import flask
from flask import request, jsonify, abort
import pandas as pd

# Import helper python file
from api_helper import *

# Define PostgreSQL information
POSTGRESQL_DB = ""
POSTGRESQL_USER = ""
POSTGRESQL_PASSWORD = ""
POSTGRESQL_HOST = ""
POSTGRESQL_PORT = ""
POSTGRESQL_311_TABLE = ""
POSTGRESQL_POPULATION_TABLE = ""

# Start flask app
app = flask.Flask(__name__)
app.config["DEBUG"] = True

# Define API home page
@app.route('/', methods=['GET'])
def home():
    return "<h1>Home Page Test for API Calls.</p>"

@app.route('/data/complaintType/all', methods=['GET'])
def getAllComplaints():
    '''
    API CALL to retrieve complaint type distribution for the entire city or borough (if specified) for a period of time
    HELPER: sumComplaints()
    CALL SAMPLE: /data/complaintType/all?year=2014&month=6
    OUTPUT: JSON
    OUTPUT SAMPLE: {
        "construction services": 5, 
        "miscellaneous concern": 15, ...}
    ''' 
    query_parameters = request.args 
    year = query_parameters.get('year')
    month = query_parameters.get('month')
    query = constructQuery(POSTGRESQL_311_TABLE, year, month)
    cur.execute(query)
    rows = cur.fetchall()
    results = sumComplaints(rows)
    return jsonify(results)

@app.route('/data/complaintType/borough', methods=['GET'])
def getBoroughComplaints():
    '''
    API CALL to retrieve complaint type distribution for the entire city grouped by borough for a period of time
    HELPER: sumBoroughComplaints()
    CALL SAMPLE: /data/complaintType/borough?year=2014&month=6
    OUTPUT: JSON
    OUTPUT SAMPLE: {
      "BX": {
        "construction services": 1, 
        "miscellaneous concern": 3, ...
      }, 
    }
    '''
    query_parameters = request.args
    year = query_parameters.get('year')
    month = query_parameters.get('month')
    query = constructQuery(POSTGRESQL_311_TABLE, year, month)
    cur.execute(query)
    rows = cur.fetchall()
    results = sumBoroughComplaints(rows)
    return jsonify(results)

@app.route('/data/complaintType/nta', methods=['GET'])
def getNTAComplaints():
    '''
    API CALL to retrieve complaint type distribution grouped by NTACode for a period of time
    NOTE: NTACode without any complaint will be empty
    HELPER: sumNTAComplaints()
    CALL SAMPLE: /data/complaintType/nta?year=2014&month=6
    OUTPUT: JSON
    OUTPUT SAMPLE: {
      "BX01": {
        "construction services": 1, 
        "miscellaneous concern": 3, ...
      }
    '''
    query_parameters = request.args
    year = query_parameters.get('year')
    month = query_parameters.get('month')
    query = constructQuery(POSTGRESQL_311_TABLE, year, month)
    cur.execute(query)
    rows = cur.fetchall()
    results = sumNTAComplaints(rows)
    return jsonify(results)

@app.route('/data/complaintCount/borough', methods=['GET'])
def getBoroughComplaintsCount():
    '''
    API CALL to retrieve all complaint counts for the entire city grouped by borough for a period of time
    HELPER: sumBoroughComplaintsCount()
    CALL SAMPLE: /data/complaintCount/borough?year=2014&month=6
    OUTPUT: JSON
    OUTPUT SAMPLE: {
      "BK": 1, 
      "QN": 2, 
      "MN": 0, 
    }
    '''
    query_parameters = request.args
    year = query_parameters.get('year')
    month = query_parameters.get('month')
    query = constructQuery(POSTGRESQL_311_TABLE, year, month)
    cur.execute(query)
    rows = cur.fetchall()
    results = sumBoroughComplaintsCount(rows)
    return jsonify(results)

@app.route('/data/complaintCount/nta', methods=['GET'])
def getNTAComplaintsCount():
    '''
    API CALL to retrieve all complaint counts for the entire city grouped by NTACode 
    HELPER: sumNTAComplaintsCount()
    CALL SAMPLE: /data/complaintCount/nta?year=2014&month=6
    OUTPUT: JSON
    OUTPUT SAMPLE: {
      "BK09": 1, 
      "BK17": 2, ...
    }
    '''
    query_parameters = request.args
    year = query_parameters.get('year')
    month = query_parameters.get('month')
    query = constructQuery(POSTGRESQL_311_TABLE, year, month)
    cur.execute(query)
    rows = cur.fetchall()
    results = sumNTAComplaintsCount(rows)
    return jsonify(results)

@app.route('/data/complaintCapitaRatio/nta', methods=['GET'])
def getNTAComplaintsCapitaRatio():
    '''
    API CALL to retrieve normalized complaint per capita ratio for the entire city grouped by NTACode for a period of time
    HELPER: complaintsPerCapitaNTA()
    CALL SAMPLE: /data/complaintCapitaRatio/nta?year=2014
    OUTPUT: JSON
    OUTPUT SAMPLE: {
      "BK09": 0.1, 
      "BK17": 0.3, ...
    }
    '''
    query_parameters = request.args
    year = query_parameters.get('year')
    query_nta = "SELECT * FROM " + POSTGRESQL_311_TABLE + " WHERE year='" + year + "'"
    query_population = "SELECT ntacode, population" + year + " FROM " + POSTGRESQL_POPULATION_TABLE
    cur.execute(query_nta)
    rows_nta = cur.fetchall()
    cur.execute(query_population)
    rows_population = cur.fetchall()
    results = complaintsPerCapitaNTA(rows_nta, rows_population)
    return jsonify(results)

if __name__ == "__main__":
    # Connect to the database
    con = psycopg2.connect(database=POSTGRESQL_DB, user=POSTGRESQL_USER, password=POSTGRESQL_PASSWORD, host=POSTGRESQL_HOST, port=POSTGRESQL_PORT)
    cur = con.cursor()

    # Deploy the flask app to run on machine's IP Address
    app.run(host='0.0.0.0', debug=True)

    print("End of Script")
