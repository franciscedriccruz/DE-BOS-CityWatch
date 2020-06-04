# Import libraries
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import pandas as pd
import json
import requests
import geopandas as gpd 
import plotly.express as px

# Define global variables
# LOCAL ENDPOINT
ENDPOINT_IP = "http://0.0.0.0:5000"

# AWS ENDPOINT
# ENDPOINT_IP = ""

empty_graph =  {
                    "layout": {
                        "xaxis": {
                            "visible": False
                        },
                        "yaxis": {
                            "visible": False
                        },
                        "annotations": [
                            {
                                "text": "No matching data found",
                                "xref": "paper",
                                "yref": "paper",
                                "showarrow": False,
                                "font": {
                                    "size": 28
                                }
                            }
                        ]
                    }
                }
# Load geojson and population files 
nta_code_geometry = gpd.read_file('nta_processed.geojson')
nta_geojson = json.loads(nta_code_geometry.to_json())
nta_population = pd.read_csv('population_processed.csv')
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']


def displayBoroughDataBar(json_result):
    '''
    Function to return graph in JSON form to plot complaint types per borough in Bar chart form
    INPUT: json_result => JSON
    OUTPUT: data_list => JSON
    '''

    # Create dataframe based on json result to easily index nested objects
    df = pd.DataFrame(json_result)
    df = df.fillna(0)
    complaint_types = list(df.index)

    data_list = []
    columns = list(df.columns)
    for complaint_type in complaint_types:
        complaint_dict = {}
        complaint_dict['x'] = columns
        complaint_dict['y'] = list(df.loc[complaint_type, :])
        complaint_dict['type'] = 'bar'
        complaint_dict['name'] = complaint_type
        data_list.append(complaint_dict)
    return data_list

def displayComplaintTypeBar(json_result):
    '''
    Function to return graph in JSON form to plot total complaint count per complaint type in Bar chart form for the city
    INPUT: json_result => JSON
    OUTPUT: data_list => JSON
    '''

    complaint_types = list(json_result.keys())

    data_list = []
    for complaint_type in complaint_types:
        complaint_dict = {}
        complaint_dict['x'] = [complaint_type]
        complaint_dict['y'] = [json_result[complaint_type]]
        complaint_dict['type'] = 'bar'
        data_list.append(complaint_dict)
    return data_list

def createAggNTACountDF(json_result):
    '''
    Function to return dataframe to plot total complaint count based on NTACode
    INPUT: json_result => JSON
    OUTPUT: df => pandas.DataFrame
    '''

    nta_codes = list(json_result.keys())
    agg_complaint_count = list(json_result.values())
    nta_agg_dict = {'NTACode':nta_codes,'Count':agg_complaint_count}
    df = pd.DataFrame(nta_agg_dict)
    return df

# def createComplaintTypeDF(json_result):
#     df = pd.DataFrame(json_result)
#     df = df.transpose()
#     df = df.reset_index()
#     df = df.rename(columns={'index': 'NTACode'})
#     df = df.fillna(0)
#     df['Total'] = df.sum(axis=1)
#     df = pd.merge(df, nta_population, on='NTACode', how='left')
#     return df
    
# Begin Dash App
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# Define layout
app.layout = html.Div(children=[

    # Header 
    html.H1(children='CityWatch Demo'),

    # Tagline
    html.Div(children='''Creating Smarter Cities Using the 311'''),

    # Dropdown to indicate year for the bar graphs 
    html.Label('Year'),
    dcc.Dropdown(
        id='year-bar',
        options=[
            {'label': 'All', 'value': 'All'},
            {'label': '2013', 'value': '2013'},
            {'label': '2014', 'value': '2014'},
            {'label': '2015', 'value': '2015'}, 
        ],
        value='All'
    ),
    
    # Dropdown to indicate month for the bar graphs
    html.Label('Month'),
    dcc.Dropdown(
        id='month-bar',
        options=[
            {'label': 'All', 'value': "All"},
            {'label': 'January', 'value': '1'},
            {'label': 'February', 'value': '2'},
            {'label': 'March', 'value': '3'},
            {'label': 'April', 'value': '4'},
            {'label': 'May', 'value': '5'},
            {'label': 'June', 'value': '6'},
            {'label': 'July', 'value': '7'},
            {'label': 'August', 'value': '8'},
            {'label': 'September', 'value': '9'},
            {'label': 'October', 'value': '10'},
            {'label': 'November', 'value': '11'},
            {'label': 'December', 'value': '12'},
        ],
        value='All'
    ),

    dcc.Graph(id='borough-graph'),

    dcc.Graph(id='total-graph'), 

    html.H1(children='''Geospatial Count'''),

    html.Label('Year'),
    dcc.Dropdown(
        id='year-spatial',
        options=[
            {'label': 'All', 'value': 'All'},
            {'label': '2013', 'value': '2013'},
            {'label': '2014', 'value': '2014'},
            {'label': '2015', 'value': '2015'}, 

        ],
        value='2014'
    ),
    
    html.Label('Month'),
    dcc.Dropdown(
        id='month-spatial',
        options=[
            {'label': 'All', 'value': "All"},
            {'label': 'January', 'value': '1'},
            {'label': 'February', 'value': '2'},
            {'label': 'March', 'value': '3'},
            {'label': 'April', 'value': '4'},
            {'label': 'May', 'value': '5'},
            {'label': 'June', 'value': '6'},
            {'label': 'July', 'value': '7'},
            {'label': 'August', 'value': '8'},
            {'label': 'September', 'value': '9'},
            {'label': 'October', 'value': '10'},
            {'label': 'November', 'value': '11'},
            {'label': 'December', 'value': '12'},
        ],
        value='All'
    ),

    html.Label('Complaint Type'),
    dcc.Dropdown(
        id='complaint-spatial',
        options=[
            {'label': 'All Complaints', 'value': "Total"},
            {'label': 'Noise', 'value': "noise"},
            {'label': 'Utilities', 'value': 'utilities'},
            {'label': 'Street Condition', 'value': 'street condition'},
            {'label': 'Parking', 'value': 'parking'},
            {'label': 'Tree Issue', 'value': 'tree report'},
            {'label': 'Homeless Reported', 'value': 'homeless report'},
            {'label': 'Construction Services', 'value': 'construction services'},
            {'label': 'Drug Activity', 'value': 'drug activity'},
            {'label': 'Unsanitary Condition', 'value': 'unsanitary conditions'},
            {'label': 'Other', 'value': 'others'},
        ],
        value='Total'
    ),
    
    dcc.Graph(id='geospatial-graph'), 

])

# Declare callback for the bar graphs
@app.callback(
    [Output(component_id='borough-graph', component_property='figure'),
     Output(component_id='total-graph', component_property='figure')],
    [Input(component_id='year-bar', component_property='value'), 
     Input(component_id='month-bar', component_property='value')]
)
def updateBarGraphs(year, month):
    '''
    Callback to update the bar graphs based on year and month dropdown inputs
    INPUT: year => value (String), month => value (String)
    OUTPUT: figure_borough_data => JSON, figure_total_data => JSON
    '''

    # Call API query based on inputs
    borough_query = ENDPOINT_IP + "/data/complaintType/borough?"
    if year == "All":
        year_parameter = ""
    else:
        year_parameter = "year=" + str(year) + '&'

    if month == "All":
        month_parameter = ""
    else:
        month_parameter = "month=" + str(month)

    full_borough_query = borough_query + year_parameter + month_parameter
    resp_borough = requests.get(full_borough_query)
    json_borough_data = json.loads(resp_borough.text)

    # Create JSON object where the data contains a JSON containing the x,y data
    figure_borough_data = {
                        'data': displayBoroughDataBar(json_borough_data),
                        'layout': {'title': '311 Data by Borough'}
        }


    total_query = ENDPOINT_IP + "/data/complaintType/all?"
    full_total_query = total_query + year_parameter + month_parameter
    resp_total = requests.get(full_total_query)
    json_total_data = json.loads(resp_total.text)

    # Create JSON object where the data contains a JSON containing the x,y data
    figure_total_data = {
            'data': displayComplaintTypeBar(json_total_data),
            'layout': {'title': 'Total City Complaint Distribution', 'showlegend': False}
        }

    if not figure_borough_data['data']:
        figure_borough_data = empty_graph
        figure_total_data = empty_graph

    return figure_borough_data, figure_total_data

@app.callback(
    Output(component_id='geospatial-graph', component_property='figure'),
    [Input(component_id='year-spatial', component_property='value'), 
     Input(component_id='month-spatial', component_property='value'),
     Input(component_id='complaint-spatial', component_property='value')]
)
def updateChoroplethGraph(year, month, complaint_type):
    '''
    Callback to update the choropleth graph based on year, month, and complaint type dropdown inputs
    INPUT: year => value (String), month => value (String)
    OUTPUT: figure_borough_data => JSON, figure_total_data => JSON
    '''

    # Call API query based on inputs
    nta_agg_query = ENDPOINT_IP + "/data/complaintCount/nta?"
    if year == "All":
        year_parameter = ""
    else:
        year_parameter = "year=" + str(year) + '&'

    if month == "All":
        month_parameter = ""
    else:
        month_parameter = "month=" + str(month)

    # if complaint == "All":
    #     complaint_parameter = ""
    # else:
    #     complaint_parameter = "month=" + str(month)    

    full_nta_agg_query = nta_agg_query + year_parameter + month_parameter

    resp_nta_agg = requests.get(full_nta_agg_query)
    json_nta_agg_data = json.loads(resp_nta_agg.text)

    # Update graph
    fig = px.choropleth_mapbox(createAggNTACountDF(json_nta_agg_data), geojson=nta_geojson, color="Count",
                               locations="NTACode", featureidkey="properties.NTACode",
                               center={"lat": 40.7128, "lon": -74.0060},
                               mapbox_style="carto-positron", zoom=9)
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, title="Plot test")

    return fig



if __name__ == '__main__':
    app.run_server(debug=True)



