import psycopg2
import json
import flask
from flask import request, jsonify, abort
import pandas as pd

# Load Full NTA Code list
full_nta_codes = list(pd.read_csv('population_processed.csv')['NTACode'])

# Define helper functions for API calls
def unpackComplaints(complaints):
    '''
    Function to unpack nested json string from database
    INPUT: complaints => String
    OUTPUT: complaints => Dictionary
    '''
    complaints = json.loads(complaints)
    return complaints

def padNTACode(complaints_dict, padded_value={}):
    '''
    Function that adds missing nta codes to complaints_dict to help in plotting
    INPUT: complaints_dict => Dictionary
    OUTPUT: complaints_dict => Dictionary
    '''
    for nta_code in full_nta_codes:
        if nta_code not in complaints_dict.keys():
            complaints_dict[nta_code] = padded_value
    return complaints_dict

def sumComplaints(query_result):
    '''
    Function to sum all complaints counts based on complaint type
    INPUT: query_result => List of tuples
    OUTPUT: total_complaints_dict => Dictionary
    OUTPUT SAMPLE: {'construction services': 1, 'unsanitary conditions': 3,...}
    '''
    total_complaints_dict = {}
    for row in query_result:
        raw_complaints = row[5]
        complaints = unpackComplaints(raw_complaints)

        if total_complaints_dict == {}:
            # if the dictionary is empty, then save the values
            total_complaints_dict = complaints
        else:
            # if the dictionary is not empty, then sum the dictionaries
            for k in set(total_complaints_dict) | set(complaints):
                # Find union of keys in both dictionaries and define new dictionary that uses key from union and adds the values
                total_complaints_dict[k] = total_complaints_dict.get(k, 0) + complaints.get(k, 0)
    return total_complaints_dict

def sumNTAComplaints(query_result):
    '''
    Function to sum all complaints counts based on a first level of NTACode and then a second level of complaint type
    INPUT: query_result => List of tuples
    OUTPUT: total_NTA_complaints_dict => Dictionary (nested)
    OUTPUT SAMPLE: {BX01: {'construction services': 1}, BX02: {'construction services': 1, 'unsanitary conditions': 3}}
    '''
    total_NTA_complaints_dict = {}
    for row in query_result:
        nta_code = row[4]
        raw_complaints = row[5]
        complaints = unpackComplaints(raw_complaints)

        if nta_code not in total_NTA_complaints_dict.keys():
            total_NTA_complaints_dict[nta_code] = complaints
        else:
            for k in set(total_NTA_complaints_dict[nta_code]) | set(complaints):
                total_NTA_complaints_dict[nta_code][k] = total_NTA_complaints_dict[nta_code].get(k, 0) + complaints.get(k, 0)
    total_NTA_complaints_dict = padNTACode(total_NTA_complaints_dict)

    return total_NTA_complaints_dict

def sumNTAComplaintsCount(query_result):
    '''
    Function to count total complaints regardless of complaint type for each NTA Code
    INPUT: query_result => List of tuples
    OUTPUT: agg_NTA_complaints_dict => Dictionary 
    OUTPUT SAMPLE: {BX01: 4, BX02: 2}
    '''
    agg_NTA_complaints_dict = {}
    for row in query_result:
        nta_code = row[4]
        raw_complaints = row[5]
        complaints = unpackComplaints(raw_complaints)
        complaint_count_list = list(complaints.values())
        sum_complaint_count = sum(complaint_count_list)
        
        if nta_code not in agg_NTA_complaints_dict.keys():
            agg_NTA_complaints_dict[nta_code] = sum_complaint_count
        else:
            agg_NTA_complaints_dict[nta_code] += sum_complaint_count
    agg_NTA_complaints_dict = padNTACode(agg_NTA_complaints_dict, 0)

    return agg_NTA_complaints_dict

def sumBoroughComplaints(query_result):
    '''
    Function to count complaints based on a first level of borough and the second level of complaint type
    INPUT: query_result => List of tuples
    OUTPUT: total_borough_complaints_dict => Dictionary (nested)
    OUTPUT SAMPLE: {'BK': {'noise': 1}, 'QN': {'street condition'}: 2}
    '''
    total_borough_complaints_dict = {}
    for row in query_result:
        borough = row[3]
        raw_complaints = row[5]
        complaints = unpackComplaints(raw_complaints)

        if borough not in total_borough_complaints_dict.keys():
            total_borough_complaints_dict[borough] = complaints
        else:
            for k in set(total_borough_complaints_dict[borough]) | set(complaints):
                total_borough_complaints_dict[borough][k] = total_borough_complaints_dict[borough].get(k, 0) + complaints.get(k, 0)
    
    return total_borough_complaints_dict

def sumBoroughComplaintsCount(query_result):
    '''
    Function to count total complaints regardless of complaint type for each borough
    INPUT: query_result => List of tuples
    OUTPUT: agg_NTA_complaints_dict => Dictionary 
    OUTPUT SAMPLE: {BX: 4, BK: 2}
    '''
    agg_borough_complaints_dict = {}
    for row in query_result:
        borough = row[3]
        raw_complaints = row[5]
        complaints = unpackComplaints(raw_complaints)
        complaint_count_list = list(complaints.values())
        sum_complaint_count = sum(complaint_count_list)
        
        if borough not in agg_borough_complaints_dict.keys():
            # if the nta code is not in the dictionary, set its value to the current sum_complaint_count
            agg_borough_complaints_dict[borough] = sum_complaint_count
        else:
            agg_borough_complaints_dict[borough] += sum_complaint_count

    return agg_borough_complaints_dict    

def complaintsPerCapitaNTA(query_result_NTA, query_result_population, year):
    '''
    Function to return normalized complaints per capita for each NTA code based on the max ratio.
    This can be used to visualize areas that receive the most complaints for the smallest population. 
    INPUT: query_result_NTA => List of tuples, query_result_population => List of tuples, year => string
    OUTPUT: complaints_pop_ratio_NTA => Dictionary 
    OUTPUT SAMPLE: {BX01: 0.4, BX02: 0.2}
    '''
    complaints_pop_ratio_NTA = {}
    year = int(year)

    NTA_count_dict = sumNTAComplaintsCount(query_result_NTA)
    for row in query_result_population:
        nta_code = row[0]
        population = float(row[1])
        if population == 0:
            complaints_pop_ratio_NTA[nta_code] = 0
        else:
            complaints_pop_ratio_NTA[nta_code] = NTA_count_dict[nta_code] / population

    # Normalize complaints based on the highest number of complaints
    if max(complaints_pop_ratio_NTA.values()) == 0:
        # Edge case where there are no complaints
        factor = 0
    else:
        factor = 1.0/max(complaints_pop_ratio_NTA.values())
    
    for nta_code in complaints_pop_ratio_NTA:
        complaints_pop_ratio_NTA[nta_code] = complaints_pop_ratio_NTA[nta_code] * factor        
    complaints_pop_ratio_NTA = padNTACode(complaints_pop_ratio_NTA, 0)

    return complaints_pop_ratio_NTA