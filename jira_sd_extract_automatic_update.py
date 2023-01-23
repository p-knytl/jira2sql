# -*- coding: utf-8 -*-
"""
Created on Fri Oct  1 13:58:35 2021
This script uses the REST API to query the JIRA server using a JQL query.
It handles auth, pagination/collation and nested JSON data.

v8 2021-12-03
=============
- Added env var and endpoint references for DB access
- Added SQL DB interaction and AWS enpoint
- Wrapped main proc in lambda_handler
- Moved all schema/prettification info into script so that a csv doesn't have
    to persist somewhere in the cloud.


v7 2021-11-24
=============
- Script now retrieves login and password credentials from system environment
    variables JIRA_USER and JIRA_PASS resepectively.
- Default query now uses relative date to return tickets in the last 365 days

v6 2021-10-18
=============
- Fixed issue where if a custom field column of JSON has more than one entry 
    it would return all data resulting in improperly aligned rows. Now in
    this case expand_column will only return the latest entry against that 
    field.

v5 2021-10-13
=============
- Re-pointing to production system

v4 2021-10-08
=============
- Error handling and bug fixes

@author: pknytl
"""
from time import time
from datetime import timedelta, datetime
from atlassian import Jira
import pandas as pd
import re
import os
import sqlalchemy
import urllib.parse

#AWS Aurora endpoint params
ENDPOINT = os.environ.get('SQL_ENDPOINT')
SQL_USER = os.environ.get('DB_USER')
SQL_PASS = os.environ.get('DB_PASS')

#Get JIRA endpoint URL from env variable
JIRA_URL = os.environ.get('JIRA_URL')

#Define JIRA session parameters here
jira_instance = Jira(
    url = JIRA_URL,
    username = os.environ.get('JIRA_USER'),
    password = os.environ.get('JIRA_PASS'),
)

#Set query for this run
jira_query = (
    'created >= -365d '
    'AND project = "Genomics England Service Desk" '
    'AND (type = "Incident" OR type = "Service Request" '
    'OR type = "NGIS - General Enquiry" OR type = "NGIS - Incident" '
    'OR type = "Problem Record (SD & QI Only)") '
    'ORDER BY created ASC'
)

"""
Choose fields to filter down to reduce response size. If not set will return
all(?) custom fields associated with a ticket, even if null
"""
jira_fields = ['issuetype', 
               'status',
               'summary',
               'priority',
               'reporter',
               'assignee',
               'created',
               'updated',
               'resolved',
               'customfield_12985', #Tribe/squad
               'customfield_10101', #Time to resolution
               'customfield_10102', #Time to first response
               'customfield_13030', #First Assignee
               'customfield_13031', #First Line Fix
               'customfield_10420', #Reason for pending
               'customfield_12120' #NGIS General Inquiry
              ]

#List of columns to keep in final extract
sel_cols = ['key',
            'fields.summary',
            'fields.issuetype.name',
            'fields.created',
            'fields.NGIS General Enquiry.ongoingCycle.breached',
            'fields.reporter.name',
            'fields.priority.name',
            'fields.Tribe/Squad.value',
            'fields.Tribe/Squad.child.value',
            'fields.assignee.name',
            'fields.updated',
            'fields.status.name',
            'fields.status.statusCategory.name',
            'fields.First Assignee.name',
            'fields.Time to resolution.ongoingCycle.breached',
            'fields.Time to first response.ongoingCycle.breached',
            'fields.Reason for Pending.value',
            'fields.Time to resolution.completedCycles.breached',
            'fields.Time to resolution.completedCycles.goalDuration.millis',
            'fields.Time to resolution.completedCycles.elapsedTime.millis',
            'fields.Time to first response.completedCycles.breached',
            'fields.Time to first response.completedCycles.goalDuration.millis',
            'fields.Time to first response.completedCycles.elapsedTime.millis',
            'fields.First Line Fix.value',
            'fields.NGIS General Enquiry.completedCycles.breached',
            'fields.NGIS General Enquiry.completedCycles.goalDuration.millis',
            'fields.NGIS General Enquiry.completedCycles.elapsedTime.millis']

#Dict of prettified column names
pretty = {'key': 'Ticket Number',
          'fields.summary': 'Summary',
          'fields.issuetype.name': 'Issue Type',
          'fields.created': 'Date Created',
          'fields.NGIS General Enquiry.ongoingCycle.breached': 
              'fields.NGIS General Enquiry.ongoingCycle.breached',
          'fields.reporter.name': 'Reporter',
          'fields.priority.name': 'Priority',
          'fields.Tribe/Squad.value': 'Tribe',
          'fields.Tribe/Squad.child.value': 'Squad',
          'fields.assignee.name': 'Assignee',
          'fields.updated': 'Updated Date',
          'fields.status.name': 'Status',
          'fields.status.statusCategory.name': 'Status Category',
          'fields.First Assignee.name': 'First Assignee'}

#Get and parse custom fields
def get_custom_field_names(jira_instance: Jira):
    print('Getting custom field names...')
    #Use JIRA library to get list of custom fields in the session instance
    custom_fields_list = jira_instance.get_all_custom_fields()
    
    #Create a lookup table for the field index to field name
    custom_fields_lookup = {}
    for field in custom_fields_list:
        custom_fields_lookup[field['id']] = field['name']
    print('Done.')
       
    return custom_fields_lookup

#Create collation function to handle API paging
def jira_collate(jira_instance: Jira, query_string: str, fields: list) -> list:
    timer = time() #start the timer
    print('Querying JIRA...')
    issues_per_query = 1000 #set the page length
    list_of_issues = []
    
    # Get the total issues in the results set. 
    num_issues = jira_instance.jql(query_string, limit = 0)["total"]
    print(f"Query '{query_string}' returns {num_issues} issues")
    
    # Use floor division + 1 to calculate the number of requests needed
    print('Retrieving data...')
    total_pages = (num_issues // issues_per_query) + 1
    for query_number in range(0, total_pages):
        print('\rPage {} of {}'.format(query_number+1, 
                                     total_pages),
              end="")
        results = jira_instance.jql(query_string, 
                                    fields = fields, 
                                    limit = issues_per_query, 
                                    start = query_number * issues_per_query)
        list_of_issues.extend(results["issues"])
    
    #Time keeping
    elapsed = timedelta(seconds=time()-timer)
    print('\nTime elapsed {}'.format(elapsed))
    print('Done.')
    
    return list_of_issues

#Custom-field normalization here
def expand_column(in_df: pd.DataFrame, column: str, requested_fields: list, 
                  drop=True) -> pd.DataFrame:
    print('Normalizing custom fields...')
    
    expansion = pd.DataFrame()
    
    #Make a placeholder to handle null rows and prevent a crash
    nullDict = {}
    for item in requested_fields:
        nullDict.update({item : None})
       
    #for each JSON in column, expand JSON
    for i in range(len(in_df)):
        temp = in_df.iloc[i]
        #prevent json_normalize failing on cells containing non-list types
        if (type(temp[column]) is list) and (len(temp[column]) > 0): 
            expanded = pd.json_normalize(temp[column])
            expanded = expanded[requested_fields] #trim
            expanded = expanded.iloc[-1] #only take last entry
            expansion = expansion.append(expanded, ignore_index=True)
        else:
            expansion = expansion.append(nullDict, ignore_index=True)
    
    #fix index
    expansion.reset_index(inplace=True, drop=True)
    
    #fix column names
    old_columns = expansion.columns
    new_columns = []
    for col in old_columns:
        new_columns.append(column + '.' + col)     
    col_dict = dict(zip(old_columns, new_columns))
    expansion.rename(columns=col_dict, inplace=True)
    
    #attach expanded columns    
    expansion = pd.concat([in_df, expansion], axis=1)
    
    #Drop the unexpanded columns
    if drop == True:
        expansion.drop(columns=column, inplace=True)
    
    print('Done.')
    
    return expansion

#Change customfield column names to readable versions for Tableau
def rename_custom_fields(flattened_df: pd.DataFrame, field_list: dict) -> pd.DataFrame:
    print('Renaming custom fields...')
    old_columns = flattened_df.columns
    new_columns = []
    
    for column in old_columns:
        if 'fields.customfield' in column:
            old_field = re.search(r'(customfield_[0-9]*)', column)
            new_field = field_list[old_field.group()]
            revised_name = re.sub(r'(customfield_[0-9]*)', new_field, column)
            new_columns.append(revised_name)
        else:
            new_columns.append(column)
    
    replace_key_val = dict(zip(old_columns, new_columns))
    
    print('Done.')
    return flattened_df.rename(columns=replace_key_val)

def set_types_from_str(input_text):
    types = {'Text' : sqlalchemy.types.Text,
             'DateTime' : sqlalchemy.types.DateTime,
             'Boolean' : sqlalchemy.types.Boolean,
             'BigInteger' : sqlalchemy.types.BigInteger,
             'Integer' : sqlalchemy.types.Integer
             }
    return types[input_text]
"""
----------
Main function below.
Use whichever helper functions above to query Jira and wrangle the response.
Recommend using try/except on individual column expansions to prevent crashing
in-case the requested column comes back as all nulls.
----------
"""


def lambda_handler(event, context):

    extract = jira_collate(jira_instance, jira_query, jira_fields)

    #Top level JSON normalization
    df = pd.json_normalize(extract)

    #Note only need to expand completedCycles. ongoingCycle is in top level
    #Expand Time to Resolution
    try:
        df = expand_column(df,
                           'fields.customfield_10101.completedCycles',
                           ['breached',
                               'goalDuration.millis',
                               'elapsedTime.millis']
                           )
    except Exception as err:
        print('Error: {}'.format(err))

    #Expand Time to First Response
    try:
        df = expand_column(df,
                           'fields.customfield_10102.completedCycles',
                           ['breached',
                               'goalDuration.millis',
                               'elapsedTime.millis']
                           )
    except Exception as err:
        print('Error: {}'.format(err))

    #Expand First Line Fix
    try:
        df = expand_column(df,
                           'fields.customfield_13031',
                           ['value']
                           )
    except Exception as err:
        print('Error: {}'.format(err))

    #Expand NGIS - General Inquiry SLA
    try:
        df = expand_column(df,
                           'fields.customfield_12120.completedCycles',
                           ['breached',
                               'goalDuration.millis',
                               'elapsedTime.millis'],
                           drop=True
                           )
    except Exception as err:
        print('Error: {}'.format(err))

    #Get custom field names
    field_list = get_custom_field_names(jira_instance)

    #Apply custom field names
    df = rename_custom_fields(df, field_list)

    #clean up df
    df = df[sel_cols]  # trim df
    df = df.rename(columns=pretty)  # prettify

    #Parse the password and username
    password_parsed = urllib.parse.quote_plus(SQL_PASS)
    username_parsed = urllib.parse.quote_plus(SQL_USER)

    #Set up database connection
    db_engine = sqlalchemy.create_engine(
        'postgresql+pg8000://{}:{}@{}/'.format(
            username_parsed, password_parsed, ENDPOINT), 
        echo=False, future=False)

    #Connect to DB and replace existing table with fresh extract
    print('Pushing to database...')
    timer = time()
    df.to_sql('jira_sd', db_engine, if_exists='replace', index=True,
              method='multi', chunksize=1000)  # push data
    db_engine.dispose()  # close the connection to the database
    elapsed = timedelta(seconds=time()-timer)
    print('Time elapsed {}'.format(elapsed))
    print('Success. ETL Complete.')

    return True