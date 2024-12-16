import pandas as pd
import numpy as np
from google.cloud import bigquery
import os

import time
from cryptography.fernet import Fernet

with open('google.key', 'rb') as file:
    secret = file.read()

if not os.path.isfile('aw-8a5d408d-02e1-4907-9163-b4d-ed487f09f36b.json'):
    fernet = Fernet(os.environ.get('GOOGLE_KEY'))

    secret_json = fernet.decrypt(secret)

    with open('aw-8a5d408d-02e1-4907-9163-b4d-ed487f09f36b.json', 'w') as json_file:
        json_file.write(secret_json.decode())

def get_storm_track(sid):
    client = bigquery.Client()
    storm_query = f'''
        SELECT 
            *  
        FROM 
            `aw-8a5d408d-02e1-4907-9163-b4d.IBTRACS.storms`
        WHERE
            SID="{sid}"
        ORDER BY 
            ISO_TIME
    '''
    try: 
        df = client.query(storm_query).to_dataframe()
        return df
    except Exception as e:
        print(e)
        return pd.DataFrame()




def get_storms_by_year(year):
    client = bigquery.Client()
    storm_query = f'''
        SELECT 
            min(ISO_TIME) as MIN_ISO_TIME, max(ISO_TIME) as MAX_ISO_TIME, NAME, SID  
        FROM 
            `aw-8a5d408d-02e1-4907-9163-b4d.IBTRACS.storms`
        WHERE
            ISO_TIME>="{year}-01-01" and ISO_TIME<="{year}-12-31"
        GROUP BY
            SID, NAME 
        ORDER BY 
            MIN_ISO_TIME,NAME,SID
    '''
    try: 
        df = client.query(storm_query).to_dataframe()
        return df
    except Exception as e:
        print(e)
        return pd.DataFrame()

def get_platforms(p_start_time, p_end_time):
    # See get_platform_data
    client = bigquery.Client()
    platform_query = '''
        SELECT 
            DISTINCT platform_code
        FROM
            `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.observations`
        WHERE
            observation_date>='{}' and observation_date<='{}'
        ORDER BY
            platform_code
    '''.format(p_start_time, p_end_time)
    try: 
        df = client.query(platform_query).to_dataframe()
        return df
    except Exception as e:
        print(e)
        return pd.DataFrame()


def get_platform_locations(p_start_time, p_end_time):
    client = bigquery.Client()
    # https://stackoverflow.com/questions/19432913/select-info-from-table-where-row-has-max-date
    location_query = f'''
        SELECT t.platform_code,max_date,*
            FROM `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.observations` t
            INNER JOIN 
            (SELECT platform_code,MAX(observation_date) AS max_date, MIN(observation_depth) AS min_depth
            FROM `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.observations`
            WHERE observation_date>='{p_start_time}' AND observation_date<='{p_end_time}'
            GROUP BY platform_code) a
            ON a.platform_code = t.platform_code AND a.min_depth = observation_depth AND a.max_date = observation_date
    '''
    try: 
        df = client.query(location_query).to_dataframe()
        # Get the last entry for variables with multiple depths (couldn't quite figure it out with sql)
        df = df.groupby('platform_code', as_index=False).last()
        return df
    except Exception as e:
        print(e)
        return pd.DataFrame()




def get_platform_data(in_platform_code):

    # I used to reuse the client, but in certain situations, the second call failed with:
    # 404 GET https://bigquery.googleapis.com/bigquery/v2/projects/aw-8a5d408d-02e1-4907-9163-b4d/jobs/5f872090-dd9d-40d6-96b8-b60c05fb05c3?projection=full&prettyPrint=false: Not found: Job aw-8a5d408d-02e1-4907-9163-b4d:5f872090-dd9d-40d6-96b8-b60c05fb05c3
    #
    # Rebuilding the client before every call fixed that issue.
    
    client = bigquery.Client()

    platform_constraint = '("' + '","'.join(in_platform_code) + '")'
    platform_data = f'''
        SELECT
            *
        FROM
            `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.observations`
        WHERE platform_code IN {platform_constraint}
    '''
    try:
        df = client.query(platform_data).to_dataframe()
        df['platform_code'] = df['platform_code'].astype(str)
        return df
    except Exception as e:
        print(e)
        return None
     
        
def get_summary_for_platform(start_time, end_time, in_platform_code):
    # See comment in get_platform_data
    client = bigquery.Client()
    platform_constraint = '("' + '","'.join(in_platform_code) + '")'
    platform_summary = '''
        SELECT
            geo_id AS gid,
            ST_X(ST_CENTROID(ANY_VALUE(geometry))) AS longitude,
            ST_Y(ST_CENTROID(ANY_VALUE(geometry))) AS latitude,
            ANY_VALUE(geometry) as cell,
            COUNT(*) AS obs
        FROM
            `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.observations` AS obs,
            `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.grid-5-by-5` AS grid_cells
        WHERE ST_CONTAINS(
            grid_cells.geometry,
            ST_GeogPoint(obs.longitude, obs.latitude)
        ) AND obs.observation_date>='{}' AND obs.observation_date<='{}' AND platform_code IN {}
        GROUP BY gid
        ORDER BY gid
    '''.format(start_time, end_time, platform_constraint)
    try:
        df = client.query(platform_summary).to_dataframe()
        return df
    except Exception as e:
        print(e)
        return None


def counts_by_week(start_time, end_time, parameter):
    # See comment in get_platform_data
    client = bigquery.Client()
    week_count = '''  
            SELECT
                geo_id AS gid,
                ST_X(ST_CENTROID(ANY_VALUE(geometry))) AS longitude,
                ST_Y(ST_CENTROID(ANY_VALUE(geometry))) AS latitude,
                ANY_VALUE(geometry) as cell,
                platform_type,
                COUNT('{}') AS obs,
                EXTRACT(WEEK from observation_date) as week
            FROM
                `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.observations` AS nobs,
                `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.grid-5-by-5` AS grid_cells
            WHERE ST_CONTAINS(
                grid_cells.geometry,
                ST_GeogPoint(nobs.longitude, nobs.latitude)
            ) and nobs.observation_date>='{}' and nobs.observation_date<='{}' and EXTRACT(week from observation_date) > 0 AND nobs.{} IS NOT NULL
            GROUP BY gid, platform_type, week
            ORDER BY gid     
        '''.format(parameter, start_time, end_time, parameter)
    try:
        df = client.query(week_count).to_dataframe()
        return df
    except Exception as e:
        print(e)
        return None


def get_summary(start_time, end_time):
    # See comment in get_platform_data
    client = bigquery.Client()
    summary = '''
        SELECT
            geo_id AS gid,
            ST_X(ST_CENTROID(ANY_VALUE(geometry))) AS longitude,
            ST_Y(ST_CENTROID(ANY_VALUE(geometry))) AS latitude,
            ANY_VALUE(geometry) as cell,
            platform_type,
            COUNT(*) AS obs
        FROM
            `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.observations` AS obs,
            `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.grid-5-by-5` AS grid_cells
        WHERE ST_CONTAINS(
            grid_cells.geometry,
            ST_GeogPoint(obs.longitude, obs.latitude)
        ) and obs.observation_date>='{}' and obs.observation_date<='{}'
        GROUP BY gid, platform_type
        ORDER BY gid
    '''.format(start_time, end_time)
    try: 
        df = client.query(summary).to_dataframe()
        tdf = df.groupby(['gid', 'latitude', 'longitude'], as_index=False).sum()
        return df, tdf
    except Exception as e:
        print(e)
        return None, None


def get_data_from_bq(platform, time0, time1):
    # See get_platform_data
    client = bigquery.Client()
    try:
        t0 = time.time()
        sql = 'SELECT * FROM `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.observations` WHERE platform_code="' + platform + '" AND observation_date>="' + time0 + '" AND observation_date<"' + time1 +'" ORDER BY `observation_date`'
        df = client.query(sql).to_dataframe()
        t1 = time.time()
        df.loc[:,'millis'] = pd.to_datetime(df['observation_date']).astype(np.int64)
        df.loc[:,'text_time'] = df['observation_date'].astype(str)
        df.loc[:,'trace_text'] = df['text_time'] + "<br>" + df['platform_type'] + "<br>" + df['country'] + "<br>" + df['platform_code']
        t2 = time.time()
        # print ("Read to DataFrame: %.4f | Add columns: %.4f | Rows %d " % (t1-t0, t2-t1, df.shape[0]))
        return df
    except Exception as e:
        print(e)
        return None
    


def get_platform_info(platform):
    # See get_platform_data
    client = bigquery.Client()
    sql = '''SELECT * FROM `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.metadata` WHERE WMO="{}" 
             and observation_date=(SELECT max(observation_date) from `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.metadata` where WMO="{}")
          '''
    sql = sql.format(platform, platform)
    df = client.query(sql).to_dataframe()
    df = df.dropna(axis=1)
    if df.empty:
        return df
    columns = list(df.columns)
    columns.remove('WMO')
    columns.remove('observation_date')
    df_combo = df.groupby('WMO')[columns].agg(lambda col: '; '.join(col.unique())).reset_index()
    df_cols = pd.DataFrame(columns=['Parameter', 'Value'])
    df_cols['Parameter'] = list(df_combo.columns)
    df_cols['Value'] = df_combo.iloc[0].values
    return df_cols


def get_time_range_locations_from_bq(time0, time1):
    # See get_platform_data
    client = bigquery.Client()
    sql="""
    SELECT * FROM 
    `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.observations`
    INNER JOIN
    (SELECT DISTINCT platform_code, max(`observation_date`) as maxt, min(observation_depth) as minod FROM `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.observations`  WHERE observation_date>"{}" AND time<"{}" GROUP BY platform_code) as findme
    on
    `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.observations`.platform_code=findme.platform_code and
    `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.observations`.observation_date=findme.maxt and
    `aw-8a5d408d-02e1-4907-9163-b4d.OSMC.observations`.observation_depth=findme.minod
    """
    tr_q = sql.format(time0, time1)
    df = client.query(tr_q).to_dataframe()
    df = df.dropna(subset=['latitude','longitude'], how='any')
    df.query('-90.0 <= latitude <= 90', inplace=True)
    df.loc[:,'millis'] = pd.to_datetime(df['observation_date']).view(np.int64)
    df.loc[:,'text_time'] = df['observation_date'].astype(str)
    df.loc[:,'trace_text'] = df['text_time'] + "<br>" + df['platform_type'] + "<br>" + df['country'] + "<br>" + df['platform_code']
    return df
