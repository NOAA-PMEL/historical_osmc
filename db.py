import constants
import pandas as pd
import datetime
import numpy as np
from google.cloud import bigquery
import os
import json

import time
from cryptography.fernet import Fernet

with open('google.key', 'rb') as file:
    secret = file.read()

fernet = Fernet(os.environ.get('GOOGLE_KEY'))

secret_json = fernet.decrypt(secret)

with open('aw-8a5d408d-02e1-4907-9163-b4d-ed487f09f36b.json', 'w') as json_file:
    json_file.write(secret_json.decode())

client = bigquery.Client()

def get_data_from_bq(platform, time0, time1): 
    try:
        t0 = time.time()
        sql = 'SELECT * FROM `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.observations` WHERE platform_code="' + platform + '" AND time>="' + time0 + '" AND time<"' + time1 +'" ORDER BY `time`'
        print(sql)
        df = client.query(sql).to_dataframe()
        t1 = time.time()
        df.loc[:,'millis'] = pd.to_datetime(df['time']).view(np.int64)
        df.loc[:,'text_time'] = df['time'].astype(str)
        df.loc[:,'trace_text'] = df['text_time'] + "<br>" + df['platform_type'] + "<br>" + df['country'] + "<br>" + df['platform_code']
        t2 = time.time()
        print ("Read to DataFrame: %.4f | Add columns: %.4f | Rows %d " % (t1-t0, t2-t1, df.shape[0]))
    except e:
        print(e)
    return df


def get_platform_info(platform):
    sql = '''SELECT * FROM `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.metadata` WHERE WMO="{}" 
             and TIME=(SELECT max(time) from `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.metadata` where WMO="{}")
          '''
    sql = sql.format(platform, platform)
    df = client.query(sql).to_dataframe()
    df = df.dropna(axis=1)
    if df.empty:
        return df
    columns = list(df.columns)
    columns.remove('WMO')
    columns.remove('time')
    df_combo = df.groupby('WMO')[columns].agg(lambda col: '; '.join(col.unique())).reset_index()
    df_cols = pd.DataFrame(columns=['Parameter', 'Value'])
    df_cols['Parameter'] = list(df_combo.columns)
    df_cols['Value'] = df_combo.iloc[0].values
    return df_cols


def get_time_range_locations_from_bq(time0, time1):
    sql="""
    SELECT * FROM 
    `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.observations`
    INNER JOIN
    (SELECT DISTINCT platform_code, max(`time`) as maxt, min(observation_depth) as minod FROM `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.observations`  WHERE time>"{}" AND time<"{}" GROUP BY platform_code) as findme
    on
    `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.observations`.platform_code=findme.platform_code and
    `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.observations`.time=findme.maxt and
    `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.observations`.observation_depth=findme.minod
    """
    tr_q = sql.format(time0, time1)
    df = client.query(tr_q).to_dataframe()
    df = df.dropna(subset=['latitude','longitude'], how='any')
    df.query('-90.0 <= latitude <= 90', inplace=True)
    df.loc[:,'millis'] = pd.to_datetime(df['time']).view(np.int64)
    df.loc[:,'text_time'] = df['time'].astype(str)
    df.loc[:,'trace_text'] = df['text_time'] + "<br>" + df['platform_type'] + "<br>" + df['country'] + "<br>" + df['platform_code']
    return df
