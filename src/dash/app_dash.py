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
# AWS PRIVATE FLASK IP
ENDPOINT_IP = ""

# Define website entities
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

year_dropdown = [{'label': '2010', 'value': '2010'},
                {'label': '2011', 'value': '2011'},
                {'label': '2012', 'value': '2012'}, 
                {'label': '2013', 'value': '2013'},
                {'label': '2014', 'value': '2014'},
                {'label': '2015', 'value': '2015'},
                {'label': '2016', 'value': '2016'},
                {'label': '2017', 'value': '2017'},
                {'label': '2018', 'value': '2018'},
                {'label': '2019', 'value': '2019'},
                {'label': '2020', 'value': '2020'}]

month_dropdown = [{'label': 'All Months', 'value': 'All'},
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
            {'label': 'December', 'value': '12'}]

complaints_dropdown = [
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
            {'label': 'Safety / Emergency', 'value': 'safety emergency'},
            {'label': 'Others', 'value': 'others'}]

# Load geojson and population files 
nta_code_geometry = gpd.read_file('nta_processed.geojson')
nta_geojson = json.loads(nta_code_geometry.to_json())
nta_population = pd.read_csv('population_processed.csv')
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

def displayBoroughDataBar(json_result):
    '''
    Function to return graph in JSON form to plot complaint types per borough in Bar chart form
    INPUT: json_result => JSON
    OUTPUT: data_list => LIST containing JSON bar graph contents (i.e. x, y, type, name)
    '''
    # Create dataframe based on json result to easily index nested objects
    df = pd.DataFrame(json_result)
    df = df.fillna(0)
    complaint_types = list(df.index)

    # Rename x-axis for better visualization
    data_list = []
    borough_dict = {'BK': 'Brooklyn', 'QN': 'Queens', 'SI': 'Staten Island', 'BX': 'Bronx', 'MN': 'Manhattan'}

    columns = list(df.columns.map(borough_dict))
    for complaint_type in complaint_types:
        complaint_dict = {}
        complaint_dict['x'] = columns
        complaint_dict['y'] = list(df.loc[complaint_type, :])
        complaint_dict['type'] = 'bar'
        complaint_dict['name'] = complaint_type
        if complaint_type == 'others':
            complaint_dict['visible'] = 'legendonly'
        data_list.append(complaint_dict)
    return data_list

def displayComplaintTypeBar(json_result):
    '''
    Function to return graph in JSON form to plot total complaint count per complaint type in Bar chart form for the city
    INPUT: json_result => JSON
    OUTPUT: data_list => LIST containing JSON bar graph (i.e. x, y, type, name)
    '''
    complaint_types = list(json_result.keys())
    data_list = []
    for complaint_type in complaint_types:
        complaint_dict = {}
        complaint_dict['x'] = [complaint_type]
        complaint_dict['y'] = [json_result[complaint_type]]
        complaint_dict['type'] = 'bar'
        complaint_dict['name'] = complaint_type
        if complaint_type == 'others':
            complaint_dict['visible'] = 'legendonly'
        data_list.append(complaint_dict)
    return data_list

def createComplaintTypeDF(json_result):
    '''
    Function to create dataframe to plot the choropleth graph containing the ntacode information
    INPUT: json_result => JSON
    OUTPUT: df => pandas.DataFrame containing NTACode, ComplaintTypeCount, TotalCount, Borough, NTAName
    '''
    df = pd.DataFrame(json_result)
    df = df.transpose()
    df = df.reset_index()
    df = df.rename(columns={'index': 'NTACode'})
    df = df.fillna(0)
    df['Total'] = df.sum(axis=1)
    df = pd.merge(df, nta_population[['NTACode', 'Borough', 'NTA Name']], on='NTACode', how='left')
    df = df.drop_duplicates()
    return df

def createComplaintCapitaDF(json_result):
    '''
    Function to create dataframe to plot the choropleth graph containing complaints per capita for each ntacode
    INPUT: json_result => JSON
    OUTPUT: df => pandas.DataFrame containing NTACode, Ratio, Borough, NTAName
    '''
    borough_code = list(json_result.keys())
    ratio = list(json_result.values())
    complaint_capita_dict = {'NTACode': borough_code, 'Ratio': ratio}
    df = pd.DataFrame(complaint_capita_dict)
    df = pd.merge(df, nta_population[['NTACode', 'Borough', 'NTA Name']], on='NTACode', how='left')
    df = df.drop_duplicates()
    return df

    
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
        options=year_dropdown,
        value='2010'
    ),
    
    # Dropdown to indicate month for the bar graphs
    html.Label('Month'),
    dcc.Dropdown(
        id='month-bar',
        options=month_dropdown,
        value='All'
    ),

    dcc.Graph(id='borough-graph'),

    dcc.Graph(id='total-graph'), 

    html.H1(children='''Geospatial Count'''),
    html.Div(children='''See 311 Calls Based on Area Code and Neighborhoods'''),

    html.Label('Year'),
    dcc.Dropdown(
        id='year-spatial',
        options=year_dropdown,
        value='2010'
    ),
    
    html.Label('Month'),
    dcc.Dropdown(
        id='month-spatial',
        options=month_dropdown,
        value='All'
    ),

    html.Label('Complaint Type'),
    dcc.Dropdown(
        id='complaint-spatial',
        options=complaints_dropdown,
        value='Total'
    ),
    
    dcc.Graph(id='geospatial-graph'), 


    html.H1(children='''Normalized 311 Calls per Capita'''), 
    html.Div(children='''Find out where most 311 calls happen based on population'''), 

    html.Label('Year'),
    dcc.Dropdown(
        id='year-capita',
        options=year_dropdown,
        value='2010'
    ),
    
    html.Label('Month'),
    dcc.Dropdown(
        id='month-capita',
        options=month_dropdown,
        value='All'
    ),

    dcc.Graph(id='geospatial-capita-graph'), 

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
        month_parameter = "month=" + str(month) + '&'

    full_borough_query = borough_query + year_parameter + month_parameter
    full_borough_query = full_borough_query[:-1]
    resp_borough = requests.get(full_borough_query)
    json_borough_data = json.loads(resp_borough.text)

    # Create JSON object where the data contains a JSON containing the x,y data
    figure_borough_data = {
                        'data': displayBoroughDataBar(json_borough_data),
                        'layout': {'title': '311 Data by Borough'},  
        }

    total_query = ENDPOINT_IP + "/data/complaintType/all?"
    full_total_query = total_query + year_parameter + month_parameter
    full_total_query = full_total_query[:-1]
    resp_total = requests.get(full_total_query)
    json_total_data = json.loads(resp_total.text)

    # Create JSON object where the data contains a JSON containing the x,y data
    figure_total_data = {
            'data': displayComplaintTypeBar(json_total_data),
            'layout': {'title': 'Total City Complaint Distribution', 'showlegend': False}
        }

    # If there are no available data, then displa empty graph template
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
    nta_agg_query = ENDPOINT_IP + "/data/complaintType/nta?"
    if year == "All":
        year_parameter = ""
    else:
        year_parameter = "year=" + str(year) + '&'

    if month == "All":
        month_parameter = ""
    else:
        month_parameter = "month=" + str(month) + '&'

    full_nta_agg_query = nta_agg_query + year_parameter + month_parameter
    full_nta_agg_query = full_nta_agg_query[:-1]
    resp_nta_agg = requests.get(full_nta_agg_query)
    json_nta_agg_data = json.loads(resp_nta_agg.text)

    # Update graph based on specified year, month, complaintType filter
    df = createComplaintTypeDF(json_nta_agg_data)
    if complaint_type in df.columns:
        fig = px.choropleth_mapbox(df, geojson=nta_geojson, color=complaint_type,
                                       locations="NTACode", featureidkey="properties.NTACode",
                                       center={"lat": 40.7128, "lon": -74.0060},
                                       mapbox_style="carto-positron", zoom=9, 
                                       hover_data=["NTACode", "NTA Name", "Borough"])

        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, title="Geospatial Graph")
    else:
        # If no data available, then display empty graph
        fig = empty_graph
    return fig


@app.callback(
    Output(component_id='geospatial-capita-graph', component_property='figure'),
    [Input(component_id='year-capita', component_property='value'), 
     Input(component_id='month-capita', component_property='value')]
)
def updateChoroplethGraphPerCapita(year, month):
    '''
    Callback to update the choropleth graph based on year, month, and complaint type dropdown inputs
    INPUT: year => value (String), month => value (String)
    OUTPUT: figure_borough_data => JSON, figure_total_data => JSON
    '''

    # Call API query based on inputs
    nta_capita_query = ENDPOINT_IP + "/data/complaintCapitaRatio/nta?"
    if year == "All":
        year_parameter = ""
    else:
        year_parameter = "year=" + str(year) + '&'

    if month == "All":
        month_parameter = ""
    else:
        month_parameter = "month=" + str(month) + '&'

    full_nta_capita_query = nta_capita_query + year_parameter + month_parameter
    full_nta_capita_query = full_nta_capita_query[:-1]
    resp_nta_capita = requests.get(full_nta_capita_query)
    json_nta_capita_data = json.loads(resp_nta_capita.text)

    # Update graph
    df = createComplaintCapitaDF(json_nta_capita_data)
    fig = px.choropleth_mapbox(df, geojson=nta_geojson, color="Ratio",
                                           locations="NTACode", featureidkey="properties.NTACode",
                                           center={"lat": 40.7128, "lon": -74.0060},
                                           mapbox_style="carto-positron", zoom=9, 
                                           hover_data=["NTACode", "NTA Name", "Borough"])

    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, title="Capita Graph")
    return fig


if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0')

