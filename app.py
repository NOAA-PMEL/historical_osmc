from dash import Dash, dcc, html, Input, Output, State, no_update, DiskcacheManager, CeleryManager
import dash_mantine_components as dmc
import dash_design_kit as ddk
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import redis
import os
import json
import db
import numpy as np

import diskcache

import celery
from celery import Celery

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
df, total = db.get_summary('2020-01-01', '2020-01-31')
redis_instance.hset("cache", "summary", json.dumps(df.to_json()))
redis_instance.hset("cache", "totals", json.dumps(total.to_json()))




platform_options = [{'label': 'All', 'value': 'all'}]
platforms = df['platform_type'].unique()
for platform in sorted(platforms):
    platform_options.append({'label': platform, 'value': platform})

app.layout = ddk.App([
    dcc.Store('data-change'),
    ddk.Header([
        ddk.Logo(src=app.get_asset_url('OSMC_logo.png')),
        ddk.Title('Summary Information for the Historical OSMC'),
        dcc.Loading(html.Div(id='loader', style={'display': 'none'}))
    ]),
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
                    dcc.Input(id='start-date-picker', type="date", min='2020-01-01', max='2020-11-20', value='2020-01-01'),
                    dmc.Button(id='update', children='Update', radius="md", variant='outline'),
                    dcc.Input(id='end-date-picker', type='date', min='2020-01-01', max='2020-11-20', value='2020-01-31')
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
])


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