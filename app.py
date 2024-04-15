from dash import Dash, dcc, html, Input, Output, State, no_update, DiskcacheManager, CeleryManager, exceptions
import plotly
import dash_mantine_components as dmc
import dash_design_kit as ddk
import dash_ag_grid as dag
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import redis
import os
import json
import db
import numpy as np
import datetime

import constants

import diskcache

import celery
from celery import Celery

dataset_start = '2020-01-01'
dataset_end = '2020-11-20'
dataset_future = '2075-12-31'

celery_app = Celery(broker=os.environ.get("REDIS_URL", "redis://127.0.0.1:6379"), backend=os.environ.get("REDIS_URL", "redis://127.0.0.1:6379"))
if os.environ.get("DASH_ENTERPRISE_ENV") == "WORKSPACE":
    # For testing...
    # import diskcache
    cache = diskcache.Cache("./cache")
    background_callback_manager = DiskcacheManager(cache)
else:
    # For production...
    background_callback_manager = CeleryManager(celery_app)

app = Dash(__name__, background_callback_manager=background_callback_manager)
server = app.server  # expose server variable for Procfile

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

week_platform_options = platform_options.copy()
del week_platform_options[0]

parameter_options = []
# Iterate all long names dictionary sort by value (the long name)

for key, value in sorted(constants.long_names.items(), key=lambda x: x[1].lower()):
    if key in constants.data_variables:
        parameter_options.append({'label': constants.long_names[key], 'value': key})

app.layout = ddk.App([
    dcc.Store('data-change'),
    dcc.Store('platform-data'),
    dcc.Store('week-data'),
    ddk.Header([
        ddk.Logo(src=app.get_asset_url('OSMC_logo.png')),
        ddk.Title('Summary Information for the Historical OSMC'),
        dcc.Loading(html.Div(id='loader', style={'display': 'none'}))
    ]),
    dmc.Tabs(
    [
        dmc.TabsList(
            [
                dmc.Tab("Summary Map", value="summary"),
                dmc.Tab('Observations per Week', 'byweek'),
                dmc.Tab("Individual Platforms", value="platform"),
            ]
        ),
          dmc.TabsPanel(value='summary', children = [
            dmc.Grid(children=[
                dmc.Col(span=3, children=[
                    dmc.Card([
                        dmc.Text('Summary Controls', p=12, fz=28),
                        dmc.Text('Platforms:', p=8, fz=20),
                        dmc.MultiSelect(
                            id='platform-dropdown',
                            data=platform_options,
                            dropdownPosition='bottom',
                            clearable=False,
                            value=['VOSCLIM']
                        ),
                        dmc.Text('Date Range:', p=8, fz=20),
                        dmc.Group(position='apart', children=[
                            dcc.Input(id='start-date-picker', type="date", min=dataset_start, max=dataset_end, value=dataset_start),
                            dmc.Button(id='update', children='Update', radius="md", variant='outline'),
                            dcc.Input(id='end-date-picker', type='date', min=dataset_start, max=dataset_end, value='2020-01-31')
                        ])
                    ], style={'height': '72vh'}),    
                ]),
                dmc.Col(span=9, children=[
                    dmc.Card(id='one-graph-card', children=[
                        dmc.CardSection(children=[
                            dmc.Text(id='graph-title', children='A Plot', p=12, fz=28)
                        ]),
                        dmc.CardSection(children=[
                            dcc.Graph(id='update-graph', style={'height': '65vh'}),
                        ])
                    ])
                ])
            ]),
        ]),
          dmc.TabsPanel(value='byweek', children = [
            dmc.Grid(children=[
                dmc.Col(span=3, children=[
                    dmc.Card([
                        dmc.Text('Percentage of Weeks with', p=12, fz=28),
                        dmc.Text('At least', p=8, fz=20),
                        dmc.TextInput(id='min-obs', placeholder='Enter minimum observations', type='number'),
                        dmc.Text('observations of', p=8, fz=20),
                        dmc.Select(
                            id='week-parameter',
                            data=parameter_options,
                            value='sst',
                            dropdownPosition='bottom',
                            searchable=True,
                            nothingFound="No variable matches this search.",
                            clearable=False,
                        ),
                        dmc.Text('in each', p=8, fz=20),
                        dmc.Select(id='gridsize',
                                   data=[
                                       {'label': '5\u00B0 x 5\u00B0 Grid Cell', 'value': 5}
                                   ], value=5),
                        dmc.Text('for date range:', p=8, fz=20),
                        dmc.Group(position='apart', children=[
                            dcc.Input(id='week-start-date-picker', type="date", min=dataset_start, max=dataset_end, value=dataset_start),
                            dmc.Button(id='week-update', children='Update', radius="md", variant='outline'),
                            dcc.Input(id='week-end-date-picker', type='date', min=dataset_start, max=dataset_end, value='2020-01-31')
                        ]),
                        dmc.Group(position='center', children=[
                            dcc.Loading(html.Div(id='bigq-loader', style={'visibility': 'hidden'}))                         
                        ], mt=30)
                    ], style={'height': '72vh'}),    
                ]),
                dmc.Col(span=9, children=[
                    dmc.Card(children=[

                    ]),
                    dmc.Card(id='percent-map-card', children=[
                        dmc.CardSection([
                            dmc.Group(children=[
                                dmc.Text('as observed by platform: ', ml=15),
                                dmc.Select(
                                    style={'width': 400},
                                    # styles={"dropdown": {"z-index": 1400, "background-color": "pink"}},
                                    id='week-platform',
                                    data=week_platform_options,
                                    dropdownPosition='bottom',
                                    clearable=False,
                                    value='VOSCLIM'
                                ),
                            ])
                        ]),
                        dmc.CardSection(children=[
                            dmc.Text(id='percent-map-title', children='A Plot', p=12, fz=28)
                        ]),
                        dmc.CardSection(children=[
                            dcc.Loading(dcc.Graph(id='percent-map', style={'height': '65vh'})),
                        ])
                    ], )
                ])
            ]),
        ]),
        dmc.TabsPanel(value='platform', children=[
            dmc.Grid(children=[
                dmc.Col(span=3, children=[
                    dmc.Card([
                        dmc.Text('Platform Selection', p=12, fz=28),
                        dmc.Text('Platforms:', p=8, fz=20),
                        dmc.MultiSelect(
                            id='platform-code',
                            data=code_options,
                            dropdownPosition='bottom',
                            searchable=True,
                            nothingFound="No platform matches this search.",
                            clearable=False,
                        ),
                        dmc.Text('Data Plot Selection', p=12, fz=28),
                        dmc.Text('Parameters:', p=8, fz=20),
                        dmc.Select(
                            id='parameter',
                            data=parameter_options,
                            value='sst',
                            dropdownPosition='bottom',
                            searchable=True,
                            nothingFound="No variable matches this search.",
                            clearable=False,
                        ),
                    ], style={'height': '72vh'}),
                ]),
                dmc.Col(span=9, children=[
                    dmc.Grid(children=[
                        dmc.Col(span=6, children=[
                            dcc.Loading(dcc.Graph(id='platform-summary-map'))
                        ]),
                        dmc.Col(span=6, children=[
                            dcc.Loading(dcc.Graph(id='platform-summary-bar'))
                        ]),
                        dmc.Col(span=12, children=[
                            dcc.Loading(dcc.Graph(id='data-plot'))
                        ])
                    ])
                ])
            ])
        ])
    ], value='summary'),        
  
])


@app.callback(
    [
        Output('platform-data', 'data'),
    ],
    [
        Input('platform-code', 'value')
    ]
)
def update_platform_data(in_platform_code):
    if in_platform_code is None or len(in_platform_code) == 0:
        return no_update
    else:
        df = db.get_platform_data(dataset_start, dataset_future, in_platform_code)
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
    ], prevent_initial_call=True
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

    if min_nobs is None or not min_nobs.isdigit():
        return exceptions.PreventUpdate
    df = db.counts_by_week(sunday1.strftime('%Y-%m-%d'), sunday2.strftime('%Y-%m-%d'), in_week_var, min_nobs)
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
    df = pd.read_json(json.loads(redis_instance.hget("cache", "week_data")))
    
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
    
    df['percent'] = (df['weeks_greater']/weeks)*100.0
    df['percent'] = df['percent'].astype(int)

    pdf = df.loc[df['platform_type'] == in_plat]
    title = f'Percent of weeks (from Sunday, {s1f} to Sunday, {s2f}) in each 5\u00B0 x 5\u00B0 cell with at least {in_min_nobs} {in_plat} observations of {in_var}.'
    figure = px.scatter_geo(pdf, lat="latitude", lon="longitude", color='percent', color_continuous_scale=px.colors.sequential.YlOrRd, 
                                hover_data={'latitude': True, 'longitude': True, 'percent': True}, range_color=[0,100])
    figure.update_traces(marker=dict(size=8))
    figure.update_layout(margin={'t':45, 'b':25, 'l':0, 'r':0},)
    figure.update_coloraxes(colorbar={'orientation':'h', 'thickness':20, 'y': -.175, 'title': None})
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
    df = pd.read_json(json.loads(redis_instance.hget("cache", "platform_data")))
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
    figure = px.bar(df, x='parameter', y='count', color='platform_code', barmode='group')
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
    df = pd.read_json(json.loads(redis_instance.hget("cache", "platform_data")))
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    df = df.sort_values(['time', 'platform_code'])
    if in_parameter == 'ztmp' or in_parameter == 'zsal':
        color_choice = 'Viridis'
        if in_parameter == 'ztmp':
            color_choice = 'Inferno'
        figure = px.scatter(df, y='observation_depth', x='time', color=in_parameter, color_continuous_scale=color_choice, 
                            hover_data=['platform_code', 'longitude', 'latitude', 'observation_depth', 'time', in_parameter])
        figure.update_yaxes(autorange='reversed')
    else:        
        figure = go.Figure()
        for idx, code in enumerate(df['platform_code'].unique()):
            pdf = df.loc[df['platform_code']==code]
            trace = px.scatter(pdf, x='time', y=in_parameter,)
            trace.update_traces(marker=dict(color=plotly.colors.qualitative.Dark24[idx]), name=str(code), showlegend=True)
            figure.add_traces(list(trace.select_traces()))
        figure.update_traces(mode='lines')
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
        pdf = pd.read_json(json.loads(redis_instance.hget("cache", "totals")))
        platforms = 'all platforms'
    else:
        df = pd.read_json(json.loads(redis_instance.hget("cache", "summary")))
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
        Output('platform-dropdown', 'data'),
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