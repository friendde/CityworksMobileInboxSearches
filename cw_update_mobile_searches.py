# coding: utf-8
"""Python file to update mobile searches for many users"""
# Import necessary Python libraries
import datetime
import json
import logging
import os
import pandas as pd
import pyodbc
import requests
import sys

# Setup logs
log = logging.getLogger("APIlog")
log.setLevel(logging.DEBUG)

# Create handlers
c_handler = logging.StreamHandler(sys.stdout)
f_handler = logging.FileHandler('PathToLogFile/logfile.log')
c_handler.setLevel(logging.INFO)
f_handler.setLevel(logging.DEBUG)

# Create formatters and add it to handlers
c_format = logging.Formatter('%(message)s')
f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
c_handler.setFormatter(c_format)
f_handler.setFormatter(f_format)

# Add handlers to the log
log.addHandler(c_handler)
log.addHandler(f_handler)

# Load data from the json file.
json_path = 'PathToConfigFile/config.json'
with open(json_path) as json_file:
    config_file = json.load(json_file)

# Declare global variables
cw_token = ''
cwusername = config_file['cwusername']
cwpassword = config_file['cwpassword']
cwexpires = config_file['cwexpires']
base_url = config_file['cwsite']
cwauthurl = config_file['cwauthurl']
cwapiurl = config_file['cwapiurl']
sqlusername = config_file['sqlusername']
sqlpassword = config_file['sqlpassword']
sqldriver = config_file['sqldriver']
sqlserver = config_file['sqlserver']
sqldatabase = config_file['sqldatabase']
pdmaxrows = config_file['pdmaxrows']
pdmaxcolumns = config_file['pdmaxcolumns']
pdmaxwidth = config_file['pdmaxwidth']
qrymobileinbox = config_file['qrymobileinbox']
qrygroup = config_file['qrygroup']
qrygroupempsids = config_file['qrygroupempsids']
qryemployee = config_file['qryemployee']
payload = config_file['payload']

# Set pandas dataframe display properties
pd.set_option('display.max_rows', pdmaxrows)
pd.set_option('display.max_columns', pdmaxcolumns)
pd.set_option('display.width', pdmaxwidth)

# Function to connect to SQL DB
def sqldb(driver,server,database,username,password):
    """Function to create pyodbc connection"""
    # Connect to SQL
    conn = pyodbc.connect(f'Driver={driver};'
                             f'Server={server};'
                             f'Database={database};'
                             f'UID={username};'
                             f'PWD={password};')
    return conn


# Function to transform a Python dictionary to JSON.
def data_to_json(data_dict):
    """Function to transform a Python dictionary to JSON."""
    token = cw_token
    json_data = json.dumps(data_dict, separators=(',', ':'))
    if len(list(token)) == 0:
        params = {'data':json_data}
    else:
        params = {'token': token, 'data': json_data}
    return params

# Function to make an HTTP request, return a Python dictionary.
def make_request(url, params):
    """Function to make an HTTP request, return a Python dictionary."""
    response = requests.get(url, params=params)
    return json.loads(response.text)

# Function to authenticate user credentials.
def auth_authenticate():
    """Function to authenticate user credentials."""
    data = {'LoginName': cwusername, 'Password': cwpassword, 'Expires': cwexpires}
    parameters = data_to_json(data)
    url =  cwauthurl
    response = make_request(url, parameters)
    r_value = ''
    if response['Status'] == 0:
        r_value = response['Value']['Token']
    return r_value

# Function to update the mobile search for employee SID.
def update_mobile_search(data):
    """Function to update the mobile search for employee SID."""
    parameters = data_to_json(data)
    response = make_request(cwapiurl, parameters)
    return response

try:
    # Call the auth_authenticate function. Quit if failed.
    cw_token = auth_authenticate()
    if cw_token == '':
        input('Failed to authenticate user. Check credentials. Hit enter to quit...')
        sys.exit()
    else:
        input(f'You signed into {base_url} as {cwusername}. Hit enter to continue...')

    # Get User and Group Keyword to query
    replicate_user = int(input(f'Enter the EmployeeSID of the User that has the Mobile Inbox you want to push out to other users '))
    replicate_group = input(f'Enter the Keyword of the Group that has the other users you want to update with the Mobile Inbox from the User above ')

    # Connect to Cityworks SQL Database
    sqlconn = sqldb(sqldriver, sqlserver, sqldatabase, sqlusername, sqlpassword)

    # query for replicate_user that has the list of mobile inboxes we want
    qry = f'{qrymobileinbox}{replicate_user}'
    df = pd.read_sql_query(qry, sqlconn)
    log.debug(df)

    # Update payload with mobile inbox list from replicate_user
    log.info(f"Mobile Inbox will be updated to use {df.DEFAULTVALUE[0]}")
    payload.update({"DefaultValue":df.DEFAULTVALUE[0]})
    log.debug(payload)

    # Query for the GroupID that has the Keyword from above which has the EmployeeSIDs of Users that we want to update thier Mobile Inboxes
    qry = qrygroup + replicate_group + "'"
    df = pd.read_sql_query(qry, sqlconn)
    log.debug(df)

    # Get list of EmployeeSIDs that are in the GroupID
    qry = f'{qrygroupempsids}{df.GROUPID[0].astype(int)}'
    df = pd.read_sql_query(qry, sqlconn)
    log.debug(df)
    employeesids = df.EMPLOYEESID.astype(int)

    # Loop through employeesids and update thier mobile inbox searches
    for employeesid in employeesids:
        qry = f'{qryemployee}{employeesid}'
        df = pd.read_sql_query(qry, sqlconn)
        payload.update({"EmployeeSid":employeesid})
        log.debug(payload)
        log.info(f"Updating Mobile Inbox for {df.UNIQUENAME[0]}")
        resp = update_mobile_search(payload)
        log.debug(f"JSON response: {resp}")

    log.info(f'\n\t Update Mobile Inbox Searches Finished')
    f_handler.close
    json_file.close
    sqlconn.close

except Exception:
    log.error("Something happened",exc_info=True)
    log.info(f'\n Review log file {f_handler.baseFilename}')
    f_handler.close
    json_file.close
    sqlconn.close
