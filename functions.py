'''
functions.py

This file contains the functions required to both extract data from an RDS 
instance, convert that data into a dictionary and also to reformat any datetime
data into isoformat.
'''

import sqlalchemy
from sqlalchemy import text
from datetime import datetime


def extract_table(sql_engine, sql_query):
    """
    This function takes an sql engine and an SQL query as arguments and 
    returns a table of the extracted data. The engine needs to connect to the
    AWS RDS instance and the SQL query must be in standard SQL format.

    Args:
        engine_class (Engine): An instance of an SQL engine.
        sql_query (str): SQL query to execute on database.

    Returns:
        An sqlalchemy table object of data.
    """
    with sql_engine.connect() as connection:
        query_string = text(sql_query)
        sqlalchemy_table = connection.execute(query_string)
        return sqlalchemy_table


def map_table_to_dictionary(extracted_table):
    '''
    This function takes an sqlalchemy table object as an argument and returns a 
    dictionary where the keys are the column names of the table and the values 
    are the respective data in string format.
    
    Args:
        extracted_table (sqlalchemy table): An SQLAlchemy Table Object.
    
    Returns:
        dict: A dictionary with column name as key and value as data as strings.
    '''
    for row in extracted_table:
        dictionary_of_data = dict(row._mapping)
        return dictionary_of_data
    

def correct_date_format(raw_data):
    """
    This pass-by-value function takes a dictionary of raw data and creates a 
    new dictionary where all datetime values are converted into an iso format. 
    If the dictionary that is passed in is found not to contain any datetime 
    objects, then the dictionary will be copied unchanged.

    Args:
        raw_data (dictionary): a dictionary containing a single row of data
        created from the extracted sqlalchemy table object.

    Returns:
        dictionary: Dictionary with datetime values cleaned into an isoformat.
    """
    clean_data = {}
    for item in raw_data:
        if isinstance(raw_data[item], datetime):
            clean_data[item] = (raw_data[item]).isoformat()
        else:
            clean_data[item] = raw_data[item]
    return clean_data

def extract_data_to_dictionary(engine, query):
    '''
    This function takes an sql engine and an SQL query as arguments. Using the 
    SQL querry and sql engine, it executes the query on the database and 
    produces a table of data. The 'extract_table', 'map_table_to_dictionary' 
    and 'correct_date_format' functions are then performed against this data to
    return a dictionary of the data where the datetime  fields have been  
    formatted correctly into  ISO Format.

    Args:
        engine (sqlalchemy.engine): An initialized sqlalchemy engine connected
        to a specific RDS instance.
        query (str): A valid SQL query statement in standard SQL format.

    Returns:
        Dictionary: A dictionary where the keys are column names and the values
        are strings of data under those headers.
    '''
    pulled_table = extract_table(engine,query)
    raw_data_dictionary = map_table_to_dictionary(pulled_table)
    cleaned_data = correct_date_format(raw_data_dictionary)
    return cleaned_data