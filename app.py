from dash import Dash, dcc, html, Input, Output, exceptions
import dash_design_kit as ddk
import plotly.express as px
import plotly.graph_objects as go
import os
import numpy as np

import db
import constants
import colorcet as cc
from plotly.subplots import make_subplots

import time
import datetime
from dateutil.relativedelta import relativedelta
import dateutil.parser

app = Dash(__name__)
server = app.server  # expose server variable for Procfile

center = {'lon': 0.0, 'lat': 0.0}
zoom = 1.4
marker_size = 9

time_series_variables = ['latitude', 'longitude', 'precip', 'sss', 'wvht', 'waterlevel', 'uo', 'vo', 'wo', 'rainfall_rate', 'hur', 'sea_water_elec_conductivity', 'sea_water_pressure', 'rlds', 'rsds', 'waterlevel_met_res', 'waterlevel_wrt_lcd', 'water_col_ht', 'wind_to_direction', 'sst', 'atmp', 'slp', 'windspd', 'winddir', 'clouds', 'dewpoint']
profile_variables = ['zsal', 'ztmp']

app.layout = ddk.App([
    ddk.Header([
        ddk.Logo(src=app.get_asset_url('OSMC_logo.png')),
        ddk.Title('Historical GTS Data'),
    ]),

    ddk.Row(children=[
        ddk.ControlCard(width=30, children=[
            ddk.ControlItem(label="Date Range:", children=[
                dcc.Dropdown(id='months', options=[
                {'label': 'Jan 2020 - Feb 2020', 'value': '2020-01-01T00:00:00Z'},
                {'label': 'Mar 2020 - Apr 2020', 'value': '2020-03-01T00:00:00Z'},
                {'label': 'May 2020 - Jun 2020', 'value': '2020-05-01T00:00:00Z'},
                ], value='2020-01-01T00:00:00Z', clearable=False, multi=False)
            ]),
            ddk.ControlItem(label="Platform:", children=[
                dcc.Dropdown(id='platform', clearable=True, multi=False)
            ]),
            dcc.Loading(
                ddk.DataTable(id="metadata", editable=False,
                    style_cell={
                        'overflow': 'hidden',
                        'textOverflow': 'ellipsis',
                        'maxWidth': 0
                    }
                ),
            )
        ]),
        ddk.Card(width=80, children=[ddk.CardHeader(id='map-header'), dcc.Loading(ddk.Graph(id='map'))])
    ]),
    ddk.Row(children=[
        ddk.Card(width=100, children=[ddk.CardHeader(id='plot-card', style={'margin-bottom':'20px'}), dcc.Loading(ddk.Graph(id='graphs'))])
    ]),
        ddk.Row(children=[
        ddk.Card(width=50, children=[ddk.CardHeader(id='ztmp-card', style={'margin-bottom':'20px'}), dcc.Loading(ddk.Graph(id='ztmp'))]),
        ddk.Card(width=50, children=[ddk.CardHeader(id='zsal-card', style={'margin-bottom':'20px'}), dcc.Loading(ddk.Graph(id='zsal'))])
    ])
])
    
@app.callback(
    [
        Output('map', 'figure'),
        Output('map-header', 'title'),
        Output('platform', 'options')
    ],
    [
        Input('months', 'value')
    ]
)
def show_locations(in_months):

    start_dt = dateutil.parser.isoparse(in_months)
    end_dt = start_dt + relativedelta(months=+2)

    map_title = 'Last reported location for all platform during the period ' + start_dt.strftime('%Y-%m') + ' to ' + end_dt.strftime('%Y-%m')

    location_df = db.get_time_range_locations_from_bq(start_dt.isoformat(), end_dt.isoformat())
    platform_color = {}
    p_types = sorted(location_df['platform_type'].unique())
    for index, p_type in enumerate(p_types):
        platform_color[p_type] = px.colors.qualitative.Dark24[index]
    platform_dots = px.scatter_mapbox(location_df, lat="latitude", lon="longitude", color='platform_type', color_discrete_map=platform_color,
                                        hover_data=['text_time','platform_type','country','platform_code'],
                                        # hoverlabel = {'namelength': 0,},
                                        custom_data='platform_code',
                                        )


    c_title = str(location_df.shape[0]) + ' Platform locations loaded from BiqQuery'
    platform_dots.update_traces(marker_size=marker_size)
    platform_dots.update_layout(
            mapbox_style="white-bg",
            mapbox_layers=[
                {
                    "below": 'traces',
                    "sourcetype": "raster",
                    "sourceattribution": "Powered by Esri",
                    "source": [
                        "https://ibasemaps-api.arcgis.com/arcgis/rest/services/Ocean/World_Ocean_Base/MapServer/tile/{z}/{y}/{x}?token=" + constants.ESRI_API_KEY
                    ]
                }
            ],
            mapbox_zoom=zoom,
            mapbox_center=center,
            margin={"r": 0, "t": 0, "l": 0, "b": 0},
            legend=dict(
                orientation="v",
                x=-.01,
            ),
            modebar_orientation='v',
    )
    options = []
    for platform in sorted(location_df['platform_code'].unique()):
        options.append({'label': platform, 'value': platform})
    return [platform_dots, map_title, options]

@app.callback(
    [
        Output('platform', 'value')
    ],
    [
        Input('map', 'clickData')
    ], prevent_initial_call = True
)
def set_platform_from_map(in_click):
    if in_click is not None:
        fst_point = in_click['points'][0]
        out_platform_code = fst_point['customdata']
    else:
        raise exceptions.PreventUpdate
    v = out_platform_code[0]
    return[v]



@app.callback(
    [
        Output('graphs', 'figure'),
        Output('plot-card', 'title'),
        Output('ztmp', 'figure'),
        Output('ztmp-card', 'title'),
        Output('zsal', 'figure'),
        Output('zsal-card', 'title'),
        Output('metadata', 'columns'),
        Output('metadata', 'data'),
    ],
    [
        Input('platform', 'value'),
        Input('months', 'value')
    ], prevent_initial_call=True
)
def make_plots(in_platform, in_months):
    if in_platform is None or len(in_platform) == 0:
        raise exceptions.PreventUpdate

    start_dt = dateutil.parser.isoparse(in_months)
    end_dt = start_dt + relativedelta(months=+2)

    print(in_platform)
    df = db.get_data_from_bq(in_platform, start_dt.isoformat(), end_dt.isoformat())
    t0 = time.time()
    counts = df.count()
    available_variables = []
    for column in time_series_variables:
        if counts[column] > 0:
            available_variables.append(column)
    row_size = int((len(available_variables)/3))

    if row_size*3 < len(available_variables):
        row_size = row_size + 1
    figure = make_subplots(rows=row_size, cols=3, shared_xaxes='all', row_heights=np.full(fill_value=400, shape=row_size).tolist(), subplot_titles=available_variables)
    row = 0
    t1 = time.time()
    print(available_variables)
    for ci, column in enumerate(available_variables):
        trace = px.scatter(df, x='time', y=column, height=400)
        col = ci%3 + 1
        if col == 1:
            row = row+1
        figure.add_trace(list(trace.select_traces())[0],row,col)
        figure.update_yaxes(title=column, row=row, col=col)
        figure.update_xaxes(title='Time', row=row, col=col)
    figure.update_layout(height=(410*row_size)+45, margin={'t':45})
    ts_title = 'Time series of data from ' + str(in_platform)
    t2 = time.time()
    available_variables = []
    pro_title = 'Profile plots from ' + in_platform
    for column in profile_variables:
        if counts[column] > 0:
            available_variables.append(column) 
    ztmp = go.Figure()
    ztmp_title = ''
    if 'ztmp' in available_variables:
        ztmp = px.scatter(df, x='time', y='observation_depth', color='ztmp', color_continuous_scale='Inferno', height=400)
        ztmp.update_yaxes(title='Depth', autorange='reversed')
        ztmp.update_xaxes(title='Time')  
        ztmp_title = 'ztmp from ' + str(in_platform)
    t3 = time.time()
    zsal = go.Figure()
    zsal_title = ''
    if 'zsal' in available_variables:
        zsal = px.scatter(df, x='time', y='observation_depth', color='zsal', color_continuous_scale='Viridis', height=400)
        zsal.update_yaxes(title='Depth', autorange='reversed')
        zsal.update_xaxes(title='Time')  
        zsal_title = 'zsal from ' + str(in_platform)
    t4 = time.time()

    extra_info = db.get_platform_info(in_platform)
    dt_cols = [{'name': 'Parameter', 'id': 'Parameter'}, {'name': 'Value', 'id': 'Value'}]
    print('Set up %4f | Plotting timeseries %4f | Plotting ztmp %4f | Plotting zsal %4f' % (t1-t0, t2-t1, t3-t2, t4-t3))
    return figure, ts_title, ztmp, ztmp_title, zsal, zsal_title, dt_cols, extra_info.to_dict('records')



if __name__ == '__main__':
    app.run_server(debug=True)
