from math import ceil
from re import A
from dash import dcc, html, Input, Output, State, no_update, DiskcacheManager, CeleryManager, exceptions
from dash.dash import PreventUpdate
from dash_enterprise_libraries import EnterpriseDash
import plotly
import dash_design_kit as ddk
import dash_ag_grid as dag
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import redis
import os
import json
import db
import numpy as np
import datetime
from io import StringIO

import constants

import diskcache

from celery import Celery

import colorcet as cc

dataset_start = '2020-01-01'
dataset_end = '2024-12-31'
dataset_future = '2075-12-31'
plot_bg = 'rgba(1.0, 1.0, 1.0 ,1.0)'
map_height = 520
center = {'lon': 0.0, 'lat': 0.0}
zoom = 1.4
marker_size = 9
trace_size = 12
row_height = 450

def cc_color_set(index, palette):
    rgb = px.colors.convert_to_RGB_255(palette[index])
    hexi = '#%02x%02x%02x' % rgb
    return hexi

platform_color = {
    'ARGO' : cc_color_set(0, cc.glasbey_bw_minc_20),
    'TAGGED ANIMAL': cc_color_set(1, cc.glasbey_bw_minc_20),
    'C-MAN WEATHER STATIONS': cc_color_set(2, cc.glasbey_bw_minc_20), 
    'CLIMATE REFERENCE MOORED BUOYS': cc_color_set(3, cc.glasbey_bw_minc_20), 
    'DRIFTING BUOYS (GENERIC)': cc_color_set(4, cc.glasbey_bw_minc_20),
    'DRIFTING BUOYS': cc_color_set(4, cc.glasbey_bw_minc_20), 
    'PROFILING FLOATS AND GLIDERS': cc_color_set(5, cc.glasbey_bw_minc_20),
    'PROFILING FLOATS AND GLIDERS (GENERIC)': cc_color_set(5, cc.glasbey_bw_minc_20),
    'GLIDERS': cc_color_set(5, cc.glasbey_bw_minc_20), 
    'ICE BUOYS': cc_color_set(6, cc.glasbey_bw_minc_20),
    'MOORED BUOYS (GENERIC)': cc_color_set(7, cc.glasbey_bw_minc_20),
    'MOORED BUOYS': cc_color_set(7, cc.glasbey_bw_minc_20),
    'RESEARCH': cc_color_set(8, cc.glasbey_bw_minc_20),
    'SHIPS (GENERIC)': cc_color_set(9, cc.glasbey_bw_minc_20),
    'SHIPS': cc_color_set(9, cc.glasbey_bw_minc_20),
    'SHORE AND BOTTOM STATIONS (GENERIC)': cc_color_set(10, cc.glasbey_bw_minc_20),
    'TIDE GAUGE STATIONS (GENERIC)': cc_color_set(11, cc.glasbey_bw_minc_20),
    'TROPICAL MOORED BUOYS': cc_color_set(12, cc.glasbey_bw_minc_20),
    'TSUNAMI WARNING STATIONS': cc_color_set(21, cc.glasbey_bw_minc_20),
    'UNKNOWN': cc_color_set(14, cc.glasbey_bw_minc_20),
    'UNCREWED SURFACE VEHICLE': cc_color_set(15, cc.glasbey_bw_minc_20),
    'VOLUNTEER OBSERVING SHIPS (GENERIC)': cc_color_set(16, cc.glasbey_bw_minc_20),
    'VOLUNTEER OBSERVING SHIPS': cc_color_set(16, cc.glasbey_bw_minc_20),
    'VOSCLIM': cc_color_set(17, cc.glasbey_bw_minc_20),
    'WEATHER AND OCEAN OBS': cc_color_set(18, cc.glasbey_bw_minc_20),
    'WEATHER BUOYS': cc_color_set(19, cc.glasbey_bw_minc_20),
    'WEATHER OBS': cc_color_set(20, cc.glasbey_bw_minc_20),
    'UNDERWAY CARBON SHIPS (GENERIC)': '#0d3b66',
    'OCEAN TRANSPORT STATIONS (GENERIC)': '#faf0ca',
    'GLOSS': '#f4d35e'
}

platforms = [
    'WEATHER AND OCEAN OBS',
    'SHIPS',
    'ICE BUOYS',
    'DRIFTING BUOYS (GENERIC)',
    'RESEARCH',
    'GLIDERS',
    'SHORE AND BOTTOM STATIONS (GENERIC)',
    'UNDERWAY CARBON SHIPS (GENERIC)',
    'VOLUNTEER OBSERVING SHIPS',
    'TROPICAL MOORED BUOYS',
    'VOLUNTEER OBSERVING SHIPS (GENERIC)',
    'MOORED BUOYS (GENERIC)',
    'TSUNAMI WARNING STATIONS',
    'PROFILING FLOATS AND GLIDERS',
    'SHIPS (GENERIC)',
    'MOORED BUOYS',
    'WEATHER BUOYS',
    'OCEAN TRANSPORT STATIONS (GENERIC)',
    'CLIMATE REFERENCE MOORED BUOYS',
    'GLOSS',
    'VOSCLIM',
    'UNKNOWN',
    'WEATHER OBS',
    'UNCREWED SURFACE VEHICLE',
    'C-MAN WEATHER STATIONS',
    'PROFILING FLOATS AND GLIDERS (GENERIC)',
    'TAGGED ANIMAL',
    'DRIFTING BUOYS',
    'TIDE GAUGE STATIONS (GENERIC)'
]


celery_app = Celery(broker=os.environ.get("REDIS_URL", "redis://127.0.0.1:6379"), backend=os.environ.get("REDIS_URL", "redis://127.0.0.1:6379"))
if os.environ.get("DASH_ENTERPRISE_ENV") == "WORKSPACE":
    # For testing...
    # import diskcache
    cache = diskcache.Cache("./cache")
    background_callback_manager = DiskcacheManager(cache)
else:
    # For production...
    background_callback_manager = CeleryManager(celery_app)

app = EnterpriseDash(__name__, background_callback_manager=background_callback_manager, )
server = app.server  # expose server variable for Procfile
app.setup_shortcuts(
    # logo=app.get_asset_url('GOMO_Lockup_Outlines-2.svg'),
    title="Summary Information for the Historical OSMC", # Default: app.title
    size="normal" # Can also be "slim"
)

redis_instance = redis.StrictRedis.from_url(os.environ.get("REDIS_URL", "redis://127.0.0.1:6379"))
df, total = db.get_summary(dataset_start, '2020-01-31')
redis_instance.hset("cache", "summary", json.dumps(df.to_json()))
redis_instance.hset("cache", "totals", json.dumps(total.to_json()))

platforms = db.get_platforms(dataset_start, dataset_future)
code_options = []
for code in platforms['platform_code']:
    code_options.append({'label': code, 'value': code})


platform_options = [{'label': 'All', 'value': 'all'}]
platforms = df['platform_type'].unique()
for platform in sorted(platforms):
    platform_options.append({'label': platform, 'value': platform})

parameter_options = []

# Iterate all long names dictionary sort by value (the long name)
for key, value in sorted(constants.long_names.items(), key=lambda x: x[1].lower()):
    if key in constants.data_variables:
        parameter_options.append({'label': constants.long_names[key], 'value': key})


def get_blank(message):
    blank_graph = go.Figure(go.Scatter(x=[0, 1], y=[0, 1], showlegend=False))
    blank_graph.add_trace(go.Scatter(x=[0, 1], y=[0, 1], showlegend=False))
    blank_graph.update_traces(visible=False)
    blank_graph.update_layout(
        height=map_height,
        xaxis={"visible": False},
        yaxis={"visible": False},
        title=message,
        plot_bgcolor=plot_bg,
        annotations=[
            {
                "text": message,
                "xref": "paper",
                "yref": "paper",
                "showarrow": False,
                "font": {
                    "size": 14
                }
            },
        ]
    )
    return blank_graph
app.layout = ddk.App(show_editor=False, theme=constants.theme, children=[
    dcc.Store('data-change'),
    dcc.Store('platform-data'),
    dcc.Store('week-data'),
    dcc.Store('current-platform'),
    dcc.Tabs([
        dcc.Tab(label='Summary Map', children = [
            ddk.ControlCard(width=.25, children=[
                ddk.CardHeader(title='Summary Map Controls'),
                ddk.ControlItem(label="Platform Type", children=[
                    dcc.Dropdown(
                        id='platform-dropdown',
                        options=platform_options,
                        multi=True,
                        clearable=False,
                        value=['VOSCLIM']
                    ),
                ]),
                ddk.ControlCard(orientation='horizontal', children=[
                    ddk.CardHeader('Time Range'),
                    ddk.ControlItem(children=[
                        dcc.Input(id='start-date-picker', type="date", min=dataset_start, max=dataset_end, value=dataset_start),
                    ]),
                    ddk.ControlItem(children=[
                        dcc.Input(id='end-date-picker', type='date', min=dataset_start, max=dataset_end, value='2020-01-31')
                    ]),
                    ddk.ControlItem(children=[
                        html.Button(id='update', children='Update', style={'margin-left': '35%', 'margin-right': '20%'}),
                    ],),
                ]),
                ddk.ControlItem(children=[
                    dcc.Loading(html.Div(id='loader', style={'display': 'none'}))
                ])
            ]),
            ddk.Card(width=.75, id='one-graph-card', children=[
                ddk.CardHeader(id='graph-title', children=''),
                dcc.Loading(dcc.Graph(
                    id='update-graph', 
                    style={'height': '65vh'},
                    figure=get_blank('Select the date range of interest and click the "Update" button.')
                )),
            ]),    
        ]),  # summary map tab
        dcc.Tab(label='Observations per Week', children=[
            ddk.ControlCard(width='.25', children=[
                ddk.CardHeader(title='Percentage of weeks with:'),
                ddk.ControlItem(label='this number of observations:', children=[
                    dcc.Input(id='min-obs', placeholder='Enter minimum observations', type='number'),
                ]),
                ddk.ControlItem(label='for this parameter:', children=[
                    dcc.Dropdown(
                        id='week-parameter',
                        options=parameter_options,
                        value='sst',
                        searchable=True,
                        clearable=False,
                    ),
                ]),
                ddk.ControlItem(label='as observed by this platform:', children=[
                    dcc.Dropdown(
                        searchable=True,
                        id='week-platform',
                        options=platform_options,
                        clearable=True,
                        multi=True,
                        value=['VOSCLIM']
                    ),
                ]),
                ddk.ControlItem(label='in each grid cell of this size:', children=[
                    dcc.Dropdown(id='gridsize', options=[{'label': '5\u00B0 x 5\u00B0 Grid Cell', 'value': 5}], value=5),
                ]),
                ddk.ControlCard(orientation='horizontal', children=[
                    ddk.CardHeader(title='for this date range:'),
                    ddk.ControlItem(children=[
                        dcc.Input(id='week-start-date-picker', type="date", min=dataset_start, max=dataset_end, value=dataset_start),
                    ]),
                    ddk.ControlItem(children=[
                        dcc.Input(id='week-end-date-picker', type='date', min=dataset_start, max=dataset_end, value='2020-01-31')
                    ]),
                    ddk.ControlItem(children=[
                        html.Button(id='week-update', children='Update', style={'margin-left': '35%', 'margin-right': '20%'}),
                    ]),
                ]),
                ddk.ControlItem(children=[
                    dcc.Loading(html.Div(id='bigq-loader', style={'display': 'none'}))
                ])
            ]),
            ddk.Card(width=.75, children=[
            ddk.CardHeader(id='percent-map-title'),
                dcc.Loading(dcc.Graph(
                    id='percent-map', 
                    style={'height': '65vh'},
                    figure=get_blank('Fill out the form on the left with the minimum number of observations, the parameter, and the date range and click "Update".'))
                ),        
            ]),
        ]),
        dcc.Tab(label='Individual Platforms', children=[
            ddk.ControlCard(width=.25, children=[
                ddk.ControlItem(label='Select platform:', children=[
                    dcc.Dropdown(
                        id='platform-code',
                        options=code_options,
                        multi=True,
                        searchable=True,
                        clearable=True,
                    ),
                ]),
                ddk.ControlItem(label='Select parameter:', children=[
                    dcc.Dropdown(
                        id='parameter',
                        options=parameter_options,
                        value='sst',
                        searchable=True,
                        clearable=True,
                    ),
                ]),
            ]),
            ddk.Block(width=.75, children=[
                ddk.Card(width=.5, children=[
                    dcc.Loading(dcc.Graph(
                            id='platform-summary-map',
                            figure=get_blank('Type in or select a WMO ID for the desired platform.')
                        )
                    )
                ]),
                ddk.Card(width=.5, children=[
                    dcc.Loading(dcc.Graph(
                            id='platform-summary-bar',
                            figure=get_blank('')
                        )
                    )
                ]),
                ddk.Card(width=1, children=[
                    dcc.Loading(dcc.Graph(
                            id='data-plot',
                            figure=get_blank('')
                        )
                    )
                ])      
            ])           
        ]),
        dcc.Tab(label='Observing State by Storm', children = [
            ddk.ControlCard(width=.25, children=[
                ddk.ControlItem(label='Select a Year:', children=[
                    dcc.Dropdown(
                        id='storm-year',
                        multi=False,
                        searchable=True,
                        clearable=True,
                        options={
                            "2020": "2020",
                            "2021": "2021",
                            "2022": "2022",
                            "2023": "2023",
                            "2024": "2024"
                        }
                    ),
                ]),
                ddk.ControlItem(label='Select a Storm:', children=[
                    dcc.Dropdown(
                        id='storm',
                        multi=False,
                        searchable=True,
                        clearable=True,
                    ),
                ]),
                ddk.ControlItem(label='Storm Marker Size by:', children=[
                    dcc.RadioItems(id='marker-size', options=[
                        {'label': 'Wind Speed (knots)','value': 'USA_WIND'},
                        {'label': 'Minimum Sea Level Pressure (mb)', 'value': "USA_PRES"}
                    ], value='USA_WIND'),
                ]),
                ddk.ControlItem(label='Storm Marker Color by:', children=[
                    dcc.RadioItems(id='marker-color', options=[
                        {'label': 'Wind Speed (knots)','value': 'USA_WIND'},
                        {'label': 'Minimum Sea Level Pressure (mb)', 'value': "USA_PRES"}
                    ], value='USA_WIND'),
                ]),
                html.Div(
                    dcc.Loading(html.Div(id='map-loader', style={'height':47, 'visibility':'hidden'}))
                )
            ]),
            ddk.Card(width=.75, children=[
                dcc.Graph(
                    id='platforms-storms',
                    figure=get_blank('Choose a storm to view.')
                )
            ]),
            ddk.Card(width=1, children=[
                dcc.Loading(dcc.Graph(
                    id='storm-timeseries',
                    figure=get_blank('Choose a storm to view.')
                ))
            ]),
            ddk.Card(width=1, children=[
                dcc.Loading(dcc.Graph(
                    id='storm-profiles',
                    figure=get_blank('Choose a storm to view.')
                ))
            ])
        ])
        # new tab here
    ]), # All Tabs
            ddk.PageFooter(children=[                    
            html.Hr(),
            ddk.Block(children=[
                ddk.Block(width=.1),
                ddk.Block(width=.3, children=[
                    html.Div(children=[
                        dcc.Link('National Oceanic and Atmospheric Administration',
                                href='https://www.noaa.gov/', style={'font-size': '.8em'}),
                    ]),
                    html.Div(children=[
                        dcc.Link('Pacific Marine Environmental Laboratory',
                                href='https://www.pmel.noaa.gov/',style={'font-size': '.8em'}),
                    ]),
                    html.Div(children=[
                        dcc.Link('oar.pmel.webmaster@noaa.gov', href='mailto:oar.pmel.webmaster@noaa.gov', style={'font-size': '.8em'})
                    ]),
                    dcc.Link('DOC |', href='https://www.commerce.gov/', style={'font-size': '.8em'}),
                    dcc.Link(' NOAA |', href='https://www.noaa.gov/', style={'font-size': '.8em'}),
                    dcc.Link(' OAR |', href='https://www.research.noaa.gov/', style={'font-size': '.8em'}),
                    dcc.Link(' PMEL |', href='https://www.pmel.noaa.gov/', style={'font-size': '.8em'}),
                    dcc.Link(' Privacy Policy |', href='https://www.noaa.gov/disclaimer', style={'font-size': '.8em'}),
                    dcc.Link(' Disclaimer |', href='https://www.noaa.gov/disclaimer',style={'font-size': '.8em'}),
                    dcc.Link(' Accessibility', href='https://www.pmel.noaa.gov/accessibility',style={'font-size': '.8em'})
                ]),
                ddk.Block(width=.2,children=[html.Img(src=app.get_asset_url('logo-PMEL-lockup-light_noaaPMEL_horizontal_rgb_2024.png'), style={'height': '90px', 'padding':'14px'})]),
                ddk.Block(width=.35,children=[html.Img(src=app.get_asset_url('GOMO_Lockup_Outlines-2.svg'), style={'height': '90px', 'padding':'14px'})])

            ])
        ])
]) # App


@app.callback(
    [
        Output('storm', 'options'),
        Output('current-platform', 'data', allow_duplicate=True),
        Output('map-loader','children', allow_duplicate=True)
    ],
    [
        Input('storm-year', 'value')
    ], prevent_initial_call=True
)
def get_storms(year):
    options = []
    if year is not None and len(year) > 0:
        df = db.get_storms_by_year(year)
        redis_instance.hset("cache", "storm_data", json.dumps(df.to_json()))
        df['label'] = df['NAME'] + ' (' + df['MIN_ISO_TIME'].str.slice(0, 10) + ', ' + df['MAX_ISO_TIME'].str.slice(0, 10) + ')'
        df = df[['SID','label']]
        df.rename(columns={'SID':'value'}, inplace=True)
        df.set_index('value')
        options = df.to_dict(orient='records')
        return [options, None, '']
    else:
        return [no_update, None, '']


@app.callback(
    [
        Output('platforms-storms','figure'),
        Output('map-loader', 'children', allow_duplicate=True)
    ],
    [
        Input('storm','value'),
        Input('marker-color', 'value'),
        Input('marker-size', 'value'),
        Input('current-platform', 'data')
    ], prevent_initial_call=True
)
def make_storm_map(sid, storm_marker_color, storm_marker_size, in_current_platform):

    if sid is None or len(sid) <= 1:
        raise PreventUpdate 

    sdf = db.get_storm_track(sid)
    sdf = sdf.loc[sdf[storm_marker_size].notna()]
    sdf.loc[:,'millis'] = pd.to_datetime(sdf['ISO_TIME']).astype(np.int64)
    sdf.loc[:,'millis'] =  sdf.loc[:,'millis']/1000
    cmap = px.colors.sequential.Inferno
    if storm_marker_color == 'USA_PRES':
        cmap = px.colors.sequential.Inferno_r
    plot = px.scatter_map(
               sdf, 
               lat='LAT', 
               lon='LON', 
               color=storm_marker_color, 
               size=storm_marker_size, 
               hover_data=['NAME', 'LAT', 'LON', 'ISO_TIME', 'USA_WIND', 'USA_PRES'],
               color_continuous_scale=cmap
            )
    
    for i in range(0, sdf.shape[0] - 1):
        plot.add_trace(go.Scattermap(mode="lines",
                                    lon=[sdf['LON'].iloc[i],sdf['LON'].iloc[i+1]],
                                    lat=[sdf['LAT'].iloc[i],sdf['LAT'].iloc[i+1]],
                                    line_color='red', showlegend=False, hoverinfo='skip'))
    redis_instance.hset('cache', 'storm-start', str(sdf['ISO_TIME'].min()))
    redis_instance.hset('cache', 'storm-end', str(sdf['ISO_TIME'].max()))

    df = db.get_platform_locations(sdf['ISO_TIME'].min(), sdf['ISO_TIME'].max())
    df.loc[:,'trace_text'] = df['observation_date'].astype(str) + "<br>" + df['platform_type'] + "<br>" + df['country'] + "<br>" + df['platform_code']

    for ptype in sorted(platforms):  
        if ptype in platform_color:
            marker_color = platform_color[ptype]
        else:
            marker_color = '#FF69B4'
        df_plot = df.loc[df['platform_type']==ptype]
        plot.add_trace(
            go.Scattermap(
                mode='markers', 
                lon=df_plot['longitude'], 
                lat=df_plot['latitude'],
                marker=dict(
                    color=marker_color, 
                    size=marker_size,
                ), name=str(ptype),
                hovertext=df_plot['trace_text'],
                hoverlabel = {'namelength': 0,},
                customdata=df_plot['platform_code'],
                
            )
        )

    if in_current_platform is not None and len(in_current_platform) > 0:
        # Plot the platform trace
        trace_df = pd.read_json(StringIO(json.loads(redis_instance.hget("cache", "storm-platform-data").decode('utf-8'))))
        trace_df['observation_date'] = pd.to_datetime(trace_df['observation_date'], unit='ms')
        platform_trace = go.Scattermap(lat=trace_df["latitude"], lon=trace_df["longitude"], 
                                    hovertext=trace_df['trace_text'],
                                    hoverlabel = {'namelength': 0,},
                                    mode='markers',
                                    marker=dict(color=trace_df["millis"], colorscale='Greys', size=trace_size), name=str(in_current_platform),
                                    uid=9000)
        plot.add_trace(platform_trace)

        # Add colored dots to the storm location that match the color of the platform
        storm_platform_co_location = sdf.loc[sdf['millis']>=trace_df['millis'].min()]
        storm_platform_co_location = storm_platform_co_location.loc[storm_platform_co_location['millis']<=trace_df['millis'].max()]
        storm_platform_co_location.loc[:,'trace_text'] = storm_platform_co_location['ISO_TIME'].astype(str) + "<br>LAT=" + storm_platform_co_location['LAT'].astype(str) + "<br>LON=" + storm_platform_co_location['LON'].astype(str)
        storm_trace = go.Scattermap(lat=storm_platform_co_location["LAT"], lon=storm_platform_co_location["LON"], 
                                    hoverlabel = {'namelength': 0,},
                                    mode='markers',
                                    hovertext=storm_platform_co_location['trace_text'],
                                    marker=dict(
                                        color=storm_platform_co_location["millis"], 
                                        colorscale='Greys', size=trace_size, 
                                        cmin=trace_df['millis'].min(),
                                        cmax=trace_df['millis'].max(),
                                    ),
                                    name=str('Storm Location'),
                                    uid=9000)
        plot.add_trace(storm_trace)

    plot.update_layout(
        uirevision=str(sid),
        height=map_height,
        map_style="white-bg",
        map_layers=[
            {
                "below": 'traces',
                "sourcetype": "raster",
                "sourceattribution": "&nbsp;GEBCO &amp; NCEI&nbsp;",
                "source": [
                   'https://tiles.arcgis.com/tiles/C8EMgrsFcRFL6LrL/arcgis/rest/services/GEBCO_basemap_NCEI/MapServer/tile/{z}/{y}/{x}'
                    
                ]
            }
        ],
        map_zoom=zoom,
        map_center=center,
        map_pitch = 0,
        map_bearing = 0,
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        legend=dict(
            orientation="v",
            x=-.01,
        ),
        modebar_orientation='v',
    )
    return[plot,'done']


@app.callback(
    [
        Output('storm-timeseries', 'figure'),
        Output('storm-profiles', 'figure')
    ],
    [
        Input('current-platform', 'data')
    ], prevent_initial_call=True
)
def plot_timeseries(current_platform):
    if current_platform is None:
        return [get_blank('Select a reporting platform from the map.'), get_blank("Click on a reporting platform.")]
    else:
        out_platform_code = current_platform
        # hget data
        df = pd.read_json(StringIO(json.loads(redis_instance.hget("cache", "storm-platform-data").decode('utf-8'))))
        df['platform_code'] = df['platform_code'].astype(str)
        df['observation_date'] = pd.to_datetime(df['observation_date'], unit='ms')
        columns_remaining = df.columns
        surface_variables = list(set(constants.surface_variables) & set(columns_remaining))
        depth_variables = list(set(constants.depth_variables) & set(columns_remaining))
        sub_titles = []
        rows = ceil(len(surface_variables)/3)
        row = 1
        col = 1

        if df is not None and not df.empty:
            if len(surface_variables) > 0:
                df.loc[:,'trace_text'] = df['observation_date'].astype(str) + "<br>" + df['platform_type'] + "<br>" + df['platform_code'] + "<br>" + df['platform_code']
                for var in surface_variables:
                    sub_titles.append(constants.long_names[var])
                row_heights = [row_height]*rows
                sfigure = make_subplots(cols=3, rows=rows, row_heights=row_heights, subplot_titles=sub_titles, shared_xaxes='all',)
                for var in surface_variables:
                    df.loc[:,'trace_text'] = df['observation_date'].astype(str) + "<br>" + df['platform_type'] + "<br>" + df['platform_code'] + "<br>" + df[var].astype(str)
                    trace = go.Scatter(y=df[var], x=df['observation_date'], name=var, showlegend=False, text=df['trace_text'], hoverinfo='text')
                    sfigure.add_trace(trace, row=row, col=col)
                    if col == 3:
                        row = row + 1
                    col = col%3 + 1
                this_type = df['platform_type'].loc[df['platform_code']==out_platform_code].iloc[0]
                sfigure.update_layout(height=rows*row_height, margin={'t':180}, title='Data from '+ str(this_type) + ' ' + str(out_platform_code))
                for i in range(0, len(surface_variables)):
                    xax = 'xaxis'
                    if i > 0:
                        xax = xax + str(i+1)
                    sfigure['layout'][xax].update(showticklabels=True)
            else:
                sfigure = get_blank('No surface data found.')

            if len(depth_variables) > 1:
                cbarlocs=[.45, 1.0]
                dfigure = make_subplots(cols=2, rows=1, row_heights=[row_height], 
                                        subplot_titles=[constants.long_names[depth_variables[0]], constants.long_names[depth_variables[1]]],
                                        shared_xaxes='all')
                for vix, var in enumerate(depth_variables):
                    colorscale='Inferno'
                    if var == 'zsal':
                        colorscale='Viridis'
                    plot_df = df.dropna(subset=[var])
                    plot_df.loc[:,'trace_text'] = plot_df['observation_date'].astype(str) + "<br>" + plot_df['platform_type'] + "<br>" + plot_df['platform_code'] + "<br>" + plot_df[var].astype(str)
                    this_type = df['platform_type'].loc[df['platform_code']==out_platform_code].iloc[0]
                    trace = go.Scatter(y=plot_df['observation_depth'], x=plot_df['observation_date'],
                                    showlegend=False,
                                    marker=dict(
                                        symbol='square', 
                                        showscale=True, color=plot_df[var], 
                                        colorscale=colorscale,
                                        colorbar={'x':cbarlocs[vix], 'title':{'side':'right','text': var,}}
                                    ),
                                    mode='markers', name=str(var), hoverinfo='text', text=plot_df['trace_text'])
                    dfigure.add_trace(trace, row=1, col=vix+1)
                
                dfigure['layout']['yaxis']['autorange'] = "reversed"
                dfigure['layout']['yaxis2']['autorange'] = "reversed"
                dfigure.update_layout(height=row_height, margin={'t':90}, title='Data from '+ str(this_type) + ' ' + str(out_platform_code))
            else:
                dfigure = get_blank("No sub-surface variables found.")
            return [sfigure, dfigure]
        else:
            return [get_blank("No surface data found for this platform."), get_blank("No sub-surface data found for this platform.") ]


@app.callback(
    [
        Output('current-platform', 'data', allow_duplicate=True),
    ],
    [
        Input('platforms-storms', 'clickData')
    ], prevent_initial_call=True
)
def set_platform_code_from_map(state_in_click):
    out_platform_code = None
    if state_in_click is not None:
        fst_point = state_in_click['points'][0]
        if 'customdata' in fst_point:
            out_platform_code = fst_point['customdata']
    if out_platform_code is not None:
        # Get and cache the platform data.
        start_date = redis_instance.hget('cache', 'storm-start').decode('utf-8')
        end_date = redis_instance.hget('cache', 'storm-end').decode('utf-8')
        df = db.get_data_from_bq(out_platform_code, start_date, end_date)
        df.dropna(axis=1, how='all', inplace=True)
        redis_instance.hset('cache','storm-platform-data', json.dumps(df.to_json()))
    return [out_platform_code]
    



@app.callback(
    [
        Output('platform-data', 'data'),
    ],
    [
        Input('platform-code', 'value')
    ], background=True
)
def update_platform_data(in_platform_code):
    if in_platform_code is None or len(in_platform_code) == 0:
        return no_update
    else:
        df = db.get_platform_data(in_platform_code)
        if df is None:
            print('get_platform_data returned None for', in_platform_code)
            return no_update
        redis_instance.hset("cache", "platform_data", json.dumps(df.to_json()))
        return ['data']


@app.callback(
    [
        Output('week-data', 'data'),
        Output('bigq-loader', 'children')
    ],
    [
        Input('week-update', 'n_clicks')
    ],
    [
        State('min-obs', 'value'),
        State('week-start-date-picker', 'value'),
        State('week-end-date-picker', 'value'),
        State('week-parameter', 'value')
    ], prevent_initial_call=True, background=True
)
def week_update_data(week_click, min_nobs, in_week_start, in_week_end, in_week_var):
    d1 = datetime.datetime.strptime(in_week_start, '%Y-%m-%d')
    d2 = datetime.datetime.strptime(in_week_end, '%Y-%m-%d')

    # If not on a Sunday, move back to previous Sunday
    weekday1 = d1.weekday()
    weekday2 = d2.weekday()
    if weekday1 < 6:
        sunday1 = (d1 - datetime.timedelta(days=weekday1)) - datetime.timedelta(days=1)
    else:
        sunday1 = d1
    if weekday2 < 6:
        sunday2 = (d2 - datetime.timedelta(days=weekday2)) - datetime.timedelta(days=1)
    else:
        sunday2 = d2

    
    if min_nobs is None:
        return exceptions.PreventUpdate
    else:
        min_nobs = int(min_nobs)
    df = db.counts_by_week(sunday1.strftime('%Y-%m-%d'), sunday2.strftime('%Y-%m-%d'), in_week_var)
    redis_instance.hset("cache", "week_data", json.dumps(df.to_json()))
    return ['data', '']


@app.callback(
    [
        Output('percent-map', 'figure'),
        Output('percent-map-title', 'children')
    ],
    [
        Input('week-data', 'data'),
        Input('week-platform', 'value')
    ],
    [
        State('week-start-date-picker', 'value'),
        State('week-end-date-picker', 'value'),
        State('min-obs', 'value'),
        State('week-parameter', 'value')
    ], prevent_initial_call=True
)
def make_week_map(new_data, in_plat, week_start, week_end, in_min_nobs, in_var):

    if in_min_nobs is None:
        return [get_blank('Fill out the form on the left with the minimum number of observations, the parameter, and the date range and click "Update".'), '']
    else:
        in_min_nobs = int(in_min_nobs)
    jstr = json.loads(redis_instance.hget("cache", "week_data"))
    df = pd.read_json(StringIO(jstr))
    
    d1 = datetime.datetime.strptime(week_start, '%Y-%m-%d')
    d2 = datetime.datetime.strptime(week_end, '%Y-%m-%d')

    # If not on a Sunday, move back to previous Sunday
    weekday1 = d1.weekday()
    weekday2 = d2.weekday()
    if weekday1 < 6:
        sunday1 = (d1 - datetime.timedelta(days=weekday1)) - datetime.timedelta(days=1)
    else:
        sunday1 = d1
    if weekday2 < 6:
        sunday2 = (d2 - datetime.timedelta(days=weekday2)) - datetime.timedelta(days=1)
    else:
        sunday2 = d2

    s1f = sunday1.strftime('%Y-%m-%d')
    s2f = sunday2.strftime('%Y-%m-%d')    
    weeks = (sunday2 - sunday1).days / 7

    # Get specificed platforms
    if 'all' not in in_plat:
        df = df.loc[df['platform_type'].isin(in_plat)]
    # count
    df = df.groupby(['gid', 'latitude', 'longitude', 'cell', 'platform_type', 'week'], as_index=False).sum(['obs'])
    df = df.loc[df['obs'] > int(in_min_nobs)].groupby(['gid', 'latitude', 'longitude', 'cell', 'platform_type', 'week'], as_index=False).count()
    
    df['percent'] = (df['week']/weeks)*100.0
    df['percent'] = df['percent'].astype(int)

    title = f'Percent of weeks (from Sunday, {s1f} to Sunday, {s2f}) in each 5\u00B0 x 5\u00B0 cell with at least {in_min_nobs} {in_plat} observations of {in_var}.'
    figure = px.scatter_geo(df, lat="latitude", lon="longitude", color='percent', color_continuous_scale=px.colors.sequential.YlOrRd, 
                                hover_data={'latitude': True, 'longitude': True, 'percent': True}, range_color=[0,100])
    figure.update_traces(marker=dict(size=8))
    figure.update_layout(margin={'t':45, 'b':25, 'l':0, 'r':0},)
    figure.update_coloraxes(colorbar={'orientation':'h', 'thickness':20, 'y': -.175, 'title': None,})
    figure.update_geos(showland=True, landcolor='lightgrey', showocean=True, oceancolor="#9bedff", showlakes=True, lakecolor="#9bedff", coastlinecolor='black', coastlinewidth=1, resolution=50,
                    lataxis={'dtick':5, 'gridcolor': '#eee', "showgrid": True}, lonaxis={'dtick':5, 'gridcolor': '#eee', "showgrid": True},)
    return [figure, title]
@app.callback(
    [
        Output('platform-summary-bar', 'figure'),
        Output('parameter', 'options'),
        Output('parameter', 'value')
    ],
    [
        Input('platform-data', 'data')
    ], prevent_initial_call=True
)
def update_bar_chart(data_trigger):
    jstr = json.loads(redis_instance.hget("cache", "platform_data"))
    df = pd.read_json(StringIO(jstr))
    df['platform_code'] = df['platform_code'].astype(str)
    df = df.groupby('platform_code', as_index=False).count()
    df = df[constants.data_variables + ['platform_code']]
    df = df.melt(id_vars=['platform_code'], var_name='parameter', value_name='count')
    df = df.loc[df['count'] != 0]
    if df.shape[0] == 0:
        raise exceptions.PreventUpdate
    params = df['parameter'].unique()
    if len(params) == 0:
        raise exceptions.PreventUpdate
    parameter_opts = []
    for var in params:
        parameter_opts.append({'label': constants.long_names[var], 'value': var})
    ordered_codes = list(df['platform_code'].unique())
    ordered_codes.sort()
    figure = px.bar(df, x='parameter', y='count', color='platform_code', barmode='group', category_orders={'platform_code': ordered_codes})
    return [figure, parameter_options, params[0]]   


@app.callback(
    [
        Output('data-plot', 'figure'),
    ],
    [
        Input('platform-data', 'data'),
        Input('parameter', 'value')
    ], prevent_initial_call=True
)
def update_data_plot(data_trigger, in_parameter):
    jstr = json.loads(redis_instance.hget("cache", "platform_data"))
    df = pd.read_json(StringIO(jstr))
    df['observation_date'] = pd.to_datetime(df['observation_date'], unit='ms')
    df = df.sort_values(['observation_date', 'platform_code'])
    if in_parameter == 'ztmp' or in_parameter == 'zsal':
        color_choice = 'Viridis'
        if in_parameter == 'ztmp':
            color_choice = 'Inferno'
        figure = px.scatter(df, y='observation_depth', x='observation_date', color=in_parameter, color_continuous_scale=color_choice, 
                            hover_data=['platform_code', 'longitude', 'latitude', 'observation_depth', 'observation_date', in_parameter])
        figure.update_yaxes(autorange='reversed')
    else:        
        figure = go.Figure()
        codes = list(df['platform_code'].unique())
        codes.sort()
        for idx, code in enumerate(codes):
            pdf = df.loc[df['platform_code']==code]
            trace = px.scatter(pdf, x='observation_date', y=in_parameter,)
            trace.update_traces(marker=dict(color=plotly.colors.qualitative.Dark24[idx]), name=str(code), showlegend=True)
            figure.add_traces(list(trace.select_traces()))
        # figure.update_traces(mode='lines')
        figure.update_layout(showlegend=True, title=constants.long_names[in_parameter], margin={'t':60, 'r':40})
    return [figure]   


@app.callback(
    [
        Output('platform-summary-map', 'figure'),
    ],
    [
        Input('platform-code', 'value')
    ]
)
def update_platform_summary_map(summary_platform_code):
    if summary_platform_code is None or len(summary_platform_code) == 0:
        return no_update
    else:
        df = db.get_summary_for_platform('2020-01-01', '2075-01-01', summary_platform_code)
        sdf = df.groupby(['gid', 'latitude', 'longitude'], as_index=False).sum()
        sdf['colorby'] = np.log10(sdf['obs'])
        # pdf[:'obs'] = pdf.loc[pdf['mask']] = np.nan
        # title = f'Count of observations of {platforms} from {in_start_date} to {in_end_date} in each 5\u00B0 x 5\u00B0 grid cell.'
        title = f'Count of observations in each 5\u00B0 x 5\u00B0 grid cell.'
        figure = px.scatter_geo(sdf, lat="latitude", lon="longitude", color='colorby', color_continuous_scale=px.colors.sequential.YlOrRd, 
                                    hover_data={'colorby': False, 'latitude': True, 'longitude': True, 'obs': True})
        figure.update_traces(marker=dict(size=8))
        figure.update_layout(margin={'t':45, 'b':25, 'l':0, 'r':0}, 
                            coloraxis_colorbar={'tickvals':[1,2,3,4,5,6,7], 'ticktext':['10', '100', '1K', '10K', '100K', '1000K', '10000K']},
                            title = title)
        figure.update_coloraxes(colorbar={'orientation':'h', 'thickness':20, 'y': -.175, 'title': None})
        figure.update_geos(showland=True, landcolor='lightgrey', showocean=True, oceancolor="#9bedff", showlakes=True, lakecolor="#9bedff", coastlinecolor='black', coastlinewidth=1, resolution=50,
                        lataxis={'dtick':5, 'gridcolor': '#eee', "showgrid": True}, lonaxis={'dtick':5, 'gridcolor': '#eee', "showgrid": True}, fitbounds='locations')
        return [figure]


@app.callback(
    [
        Output('update-graph', 'figure'),
        Output('graph-title', 'children'),
        Output('loader', 'children')
    ],
    [
        Input('platform-dropdown', 'value'),
        Input('data-change', 'data')
    ],
    [
        State('start-date-picker', 'value'),
        State('end-date-picker', 'value')
    ]
)
def update_graph(platform_type, data_change, in_start_date, in_end_date):
    if len(platform_type) == 0:
        return go.Figure(), 'No platform selected.', 'nothing'
    if 'all' in platform_type:
        pdf = pd.read_json(StringIO(json.loads(redis_instance.hget("cache", "totals").decode('utf-8'))))
        platforms = 'all platforms'
    else:
        df = pd.read_json(StringIO(json.loads(redis_instance.hget("cache", "summary").decode('utf-8'))))
        collection = []
        for platform in platform_type:
            cdf = df.loc[df['platform_type']==platform]
            collection.append(cdf)
        pdf = pd.concat(collection)
        pdf.groupby(['gid']).sum()
        platforms = ",".join(platform_type)
    # There are no zeros because of the query pdf['mask'] = pdf.loc[:,pdf['obs'] == 0]
    pdf['colorby'] = np.log10(pdf['obs'])
    # pdf[:'obs'] = pdf.loc[pdf['mask']] = np.nan
    title = f'Count of observations of {platforms} from {in_start_date} to {in_end_date} in each 5\u00B0 x 5\u00B0 grid cell.'
    figure = px.scatter_geo(pdf, lat="latitude", lon="longitude", color='colorby', color_continuous_scale=px.colors.sequential.YlOrRd, 
                                 hover_data={'colorby': False, 'latitude': True, 'longitude': True, 'obs': True})
    figure.update_traces(marker=dict(size=12))
    figure.update_layout(margin={'t':25, 'b':25, 'l':0, 'r':0}, 
                         coloraxis_colorbar={'tickvals':[1,2,3,4,5,6,7], 'ticktext':['10', '100', '1K', '10K', '100K', '1000K', '10000K']})
    figure.update_coloraxes(colorbar={'orientation':'h', 'thickness':20, 'y': -.175, 'title': None})
    figure.update_geos(showland=True, landcolor='lightgrey', showocean=True, oceancolor="#9bedff", showlakes=True, lakecolor="#9bedff", coastlinecolor='black', coastlinewidth=1, resolution=50,
                    lataxis={'dtick':5, 'gridcolor': '#eee', "showgrid": True}, lonaxis={'dtick':5, 'gridcolor': '#eee', "showgrid": True})
    return [figure, title, 'data']



@app.callback(
    [
        Output('data-change', 'data'),
        Output('loader', 'children', allow_duplicate=True),
        Output('platform-dropdown', 'options'),
        Output('platform-dropdown', 'value')
    ],
    [
        Input('update', 'n_clicks')
    ],
    [
        State('start-date-picker', 'value'),
        State('end-date-picker', 'value'),
        State('platform-dropdown', 'value')
    ], prevent_initial_call=True, background=True
)
def update_cache(click, start_date, end_date, values):
    df, total = db.get_summary(start_date, end_date)
    redis_instance.hset("cache", "summary", json.dumps(df.to_json()))
    redis_instance.hset("cache", "totals", json.dumps(total.to_json()))
    platform_options = [{'label': 'All', 'value': 'all'}]
    platforms = df['platform_type'].unique()
    for platform in sorted(platforms):
        platform_options.append({'label': platform, 'value': platform})
    return ['yes', 'graph', platform_options, values]


if __name__ == '__main__':
    app.run_server(debug=True, dev_tools_props_check=False)