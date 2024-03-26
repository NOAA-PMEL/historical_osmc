from dash import Dash, dcc, html, Input, Output, State, no_update
import dash_mantine_components as dmc
import dash_design_kit as ddk
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import redis
import os
import json
import db

app = Dash(__name__)
server = app.server  # expose server variable for Procfile

redis_instance = redis.StrictRedis.from_url(os.environ.get("REDIS_URL", "redis://127.0.0.1:6379"))
df = db.get_summary('2020-01-01', '2020-01-31')
redis_instance.hset("cache", "summary", json.dumps(df.to_json()))


platform_options = []
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
                    id='title-dropdown',
                    data=platform_options,
                    dropdownPosition='bottom',
                    clearable=False,
                    value=['VOSCLIM']
                ),
                dmc.Text('Date Range:', p=8, fz=20),
                dmc.Group(position='apart', children=[
                    dcc.Input(id='start-date-picker', type="date", min='2020-01-01', max='2020-11-05', value='2020-01-01'),
                    dmc.Button(id='update', children='Update', radius="md", variant='outline'),
                    dcc.Input(id='end-date-picker', type='date', min='2020-01-01', max='2020-11-05', value='2020-01-31')
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
        Input('title-dropdown', 'value'),
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
    df = pd.read_json(json.loads(redis_instance.hget("cache", "summary")))
    if df is not None:
        collection = []
        for platform in platform_type:
            cdf = df.loc[df['platform_type']==platform]
            collection.append(cdf)
        pdf = pd.concat(collection)
        pdf.groupby(['gid']).sum()
        title = f'Count of observations of {",".join(platform_type)} from {in_start_date} to {in_end_date}'
        figure = px.scatter_geo(pdf, lat="latitude", lon="longitude", color='obs', color_continuous_scale='Viridis',)
        figure.update_traces(marker=dict(size=12))
        figure.update_layout(margin={'t':25, 'b':25, 'l':0, 'r':0})
        figure.update_coloraxes(colorbar={'orientation':'h', 'thickness':20, 'y': -.175, 'title': None})
        figure.update_geos(showland=True, landcolor='tan', showocean=True, oceancolor="#9bedff", showlakes=True, lakecolor="#9bedff", coastlinecolor='black', coastlinewidth=1, resolution=50,
                        lataxis={'dtick':5, 'gridcolor': '#eee', "showgrid": True}, lonaxis={'dtick':5, 'gridcolor': '#eee', "showgrid": True})
        return [figure, title, 'data']
    else:
        return no_update


@app.callback(
    [
        Output('data-change', 'data'),
        Output('loader', 'children', allow_duplicate=True)
    ],
    [
        Input('update', 'n_clicks')
    ],
    [
        State('start-date-picker', 'value'),
        State('end-date-picker', 'value')
    ], prevent_initial_call=True
)
def update_cache(click, start_date, end_date):
    df = db.get_summary(start_date, end_date)
    redis_instance.hset("cache", "summary", json.dumps(df.to_json()))
    return ['yes', 'graph']


if __name__ == '__main__':
    app.run_server(debug=True, dev_tools_props_check=False)