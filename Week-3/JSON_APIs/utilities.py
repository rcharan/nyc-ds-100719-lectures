import config
import mysql.connector
import requests
import pandas as pd
import json
import time
# import traceback
import numpy as np

################################################################################
#
# Generic database connection provider
#
################################################################################

def get_connection():
    cnx = mysql.connector.connect(
        host = config.host,
        user = config.user,
        passwd = config.password
    )

    return cnx

def get_cursor():
    cnx = get_connection()
    cur = cnx.cursor()
    return cnx, cur

################################################################################
#
# Yelp API Calls
#
################################################################################

def yelp_call(url, url_params, api_key):
    headers = {'Authorization': 'Bearer {}'.format(api_key)}
    response = requests.get(url, headers=headers, params=url_params)
    return response

def yelp_review_call(business_dict, api_key):
    biz_id = business_dict['yelp_id']
    url = f'https://api.yelp.com/v3/businesses/{biz_id}/reviews'
    return yelp_call(url, {}, api_key)

################################################################################
#
# Yelp API Parsers
#  Returns a dataframe
#  Not generic - must be remade for each table and API-call type
#  Columns should match the name (and order?) of
#
################################################################################

def parse_business_response(yelp_response):
    keys_to_keep = ['id',
                    'alias',
                    'name',
                    'review_count',
                    'rating',
                    'location_address1',
                    'location_address2',
                    'location_address3',
                    'location_city',
                    'location_zip_code',
                    'location_country',
                    'location_state',
                    'price']
    keys_to_rename = {
        'id' : 'yelp_id'
    }

    # Make a dataframe
    df = pd.io.json.json_normalize(yelp_response.json()['businesses'],
                                   sep = '_')

    # Handle missing columns
    df_cols = set(df.columns.tolist())
    keys_to_keep = list(df_cols.intersection(set(keys_to_keep)))

    df = df[keys_to_keep]
    df.rename(keys_to_rename, axis = 'columns', inplace = True)

    if 'price' in df.columns:
        df.price = df.price.map(len, na_action = 'ignore')

    return df

def parse_review_response(yelp_response, business_dict):
    keys_to_keep = [
        'id',
        'rating',
        'text',
        'time_created'
    ]
    keys_to_rename = {'id' : 'review_yelp_id'}

    df = pd.io.json.json_normalize(yelp_response.json()['reviews'],
                                   sep = '_')

    df_cols = set(df.columns.tolist())
    keys_to_keep = list(df_cols.intersection(set(keys_to_keep)))

    df = df[keys_to_keep]
    df.rename(keys_to_rename, axis = 'columns', inplace = True)

    table_id_col = pd.Series(business_dict['table_id'],
                             index=df.index,
                             name = 'business_id')

    df = pd.concat([table_id_col, df], axis = 'columns')

    return df

################################################################################
#
# Insertion Queries
#  Parses df -> INSERT INTO, then executes the query using get_cursor
#   on the specified table and schema
#  Notes:
#    - Totally Generic (works for any table and dataframe)
#    - dataframe column names must match table column names exactly
#    -   if the columns are out of order it may return an error
#
################################################################################

def insert_data(table, schema, df):
    query, vals = build_insert_query(table, df, schema)

    try:
        cnx, cur    = get_cursor()
        cur.executemany(query, vals)
        cnx.commit()
    except:
        cnx.close()
        raise
    cnx.close()

# Called by insert data
#  Returns the query and the parsed dataframe
def build_insert_query(table, df, schema = None):
    row_1 = df.iloc[0].to_dict()
    keys = '(' + ', '.join(row_1.keys()) + ')'
    key_placeholds = ['%(' + k + ')s' for k in row_1.keys()]
    key_placeholds = '(' + ', '.join(key_placeholds) + ')'

    table_name = schema + '.' + table if schema else table
    query = ' '.join(['INSERT INTO',
                      table_name,
                      keys,
                      'VALUES',
                      key_placeholds
                     ])

    rows = []
    for i in range(len(df)):
        rows.append(cast_numpy_dict(df.iloc[i].to_dict()))

    return (query, rows)

# Utility to convert numpy types to python types recognized by
#  the mysql-connector package/MYSQL server
def cast_numpy_dict(d):
    def caster(val):
        if not val:
            return None
        if isinstance(val, float):
            if np.isnan(val):
                return None
            else:
                return val
        if isinstance(val, str):
            return val
        else:
            return val.item()
    return {k : caster(v) for (k, v) in d.items()}



################################################################################
#
# Repeated API Call management
#
################################################################################

# url_params_builder :: offset (int) -> url_params
def all_results(url, url_params_builder, df_parser, api_key,
                api_caller, table, schema, offset = 0,
                recovery = None):
    if not recovery:
        # Initialize with a first call to figure out targets
        results = yelp_call(url, url_params_builder(offset), api_key)
        if results.status_code != 200:
            print('Unable to make an API call')
            return
    else:
        results = recovery

    # Set and communicate targets and starting point
    try:
        target_num     = results.json()['total']
    except Exception as err:
        print(f'Malformed response from API.',
              f'Returing query result at offset {offset}')
        return results

    current_num    = offset
    print(f'{target_num} total matches found.')
    if offset > 0:
        print(f'Resuming from item number {offset}')

    while True:
        # Parse the results
        try:
            df = df_parser(results)
        except Exception as err:
            print(err)
            print(f'Unable to parse the results.',
                  f'Returing query result at offset {current_num}')
            return results

        # Insert the results
        try:
            insert_data(table, schema, df)
        except Exception as err:
            print(err)
            print(f'Unable to insert the results.'
                  f'Returning query results and df at offset {current_num}')
            return (results, df)

        # Communicate succesful insertion and pause
        print(f'Succesfully inserted {len(df)}',
              f'records at offset {current_num}.')
        time.sleep(.5)

        # Prepare for the next iteration, possible
        current_num += 50
        if current_num > target_num or current_num >= 1000:
            print ('Done fetching results!')
            break

        results = yelp_call(url, url_params_builder(current_num), api_key)
        if results.status_code != 200:
            print(f'Unable to make an API call on offset {current_num}')
            return


def get_reviews():
    db_conn = get_connection()
    query = querier_maker(db_conn)

    try:
        business_list = query('''SELECT table_id, yelp_id FROM yelp.business''')
        business_list = json.loads(business_list.to_json(orient = 'records'))

        already_in = query('''SELECT DISTINCT business_id FROM yelp.reviews''')
        already_in = already_in.business_id.tolist()

        print(f'{len(business_list)} businesses detected')
        print(f'{len(already_in)} businesses with reviews already detected')
        print('Collecting Yelp reviews for any remaining businesses.')
        print('This could take a while if there are many remaining')

        for biz in business_list:
            if biz['table_id'] in already_in:
                continue

            result = yelp_review_call(biz, config.api_key)
            df     = parse_review_response(result, biz)
            insert_data('reviews', 'yelp', df)
            time.sleep(0.1)
    except:
        db_conn.close()
        raise
    db_conn.close()

################################################################################
#
# Utilities
#
################################################################################

def querier_maker(db_conn):
    return lambda q : pd.read_sql_query(q, db_conn)
