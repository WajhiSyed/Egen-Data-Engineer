import sqlite3
from sqlite3 import Error

import json
import requests
from urllib.request import Request, urlopen
import os
from datetime import datetime
import concurrent.futures
from pathlib import Path

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file, check_same_thread = False)
        return conn
    except Error as e:
        print(e)

    return conn


# Filtering the API Data for the respective county
def data_for_county(county_name, data):
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    fin = []
    for row in data:
        if row[9] == county_name:
            fin.append((row[8], row[10], row[11], row[12], row[13], dt_string))
    
    return fin

        
def main():
    

    my_file = Path("pythonsqlite.db")
    if my_file.is_file():
        os.remove("pythonsqlite.db")

    database = r"pythonsqlite.db"

    # create a database connection
    conn = create_connection(database)
    
    # Getting the Data from the API
    request = Request("https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD")
    response = urlopen(request)
    response = response.read()
    data = json.loads(response)
    df = data["data"]
    nested_lst_of_tuples = [tuple(l) for l in df]
    counties = []
    for row in nested_lst_of_tuples:
        modified = row[9].replace(" ", "")
        modified = modified.replace(".", "")
        if modified not in counties:
            counties.append(modified)

    # Creating and inserting data into county tables
    '''
    Current Implementation :
        1) Everyday when the cron job runs I am deleting the old county tables
        2) Reinserting the entire data
        
    Improvised Logic: (If more time)
    
        1) Once the county tables are created initially
        2) Only insert new records for that particular day by checking the last row in old dataset
        3) Dont need to keep dropping all the tables daily
        4) Dont need to drop the database daily
    
    '''
    if conn is not None:
        for county in counties:
            try:
                c = conn.cursor()
                c.execute("DROP TABLE IF EXISTS %s;" % (county))
                c.execute("CREATE TABLE IF NOT EXISTS %s (test_date text, new_positives integer \
                          , cumal_positives integer, total_tests integer, cumal_tests integer \
                          , load_date text);" % (county))
                
                sqlite_insert_query = "INSERT INTO %s \
                                  (test_date, new_positives, cumal_positives, total_tests, cumal_tests, load_date) \
                                  VALUES (?, ?, ?, ?, ?, ?);" % (county)

                recordList = data_for_county(county, nested_lst_of_tuples)
                c.executemany(sqlite_insert_query, recordList)
                conn.commit()
            except Error as e:
                print(e)
    else:
        print("Error! cannot create the database connection.")


if __name__ == '__main__':
    main()
    
    
    
    
# '''

# Testing Checks- 

# 1) From the API data and data inserted into the tables we can check if the number of records are the same
# 2) We can check if the count of new positives from the table and data from API match at a daily level
# 3) We can check if the Cumulative Number of Positives count from the table and data from API match at a daily level
# 4) Check if the number of counties are the same in the database and from the API data
# 5) Make sure the date entered into the tables is correct and updated correctly everyday

# '''
    
    
    
# Trying out Mulithreading Approach

# def createAndInsertDataIntoTables(county):
#     try:
#         c = conn.cursor()
#         c.execute("DROP TABLE IF EXISTS %s;" % (county))
#         c.execute("CREATE TABLE IF NOT EXISTS %s (test_date text, new_positives integer \
#                   , cumal_positives integer, total_tests integer, cumal_tests integer \
#                   , load_date text);" % (county))

#         sqlite_insert_query = "INSERT INTO %s \
#                           (test_date, new_positives, cumal_positives, total_tests, cumal_tests, load_date) \
#                           VALUES (?, ?, ?, ?, ?, ?);" % (county)
        
#         print(sqlite_insert_query)

#         recordList = data_for_county(county, nested_lst_of_tuples)
#         print(recordList)
#         c.executemany(sqlite_insert_query, recordList)
#         conn.commit()
#     except Error as e:
#         print(e)


# Implementing a Multithreading Approach
    '''
    if conn is not None:   
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(createAndInsertDataIntoTables, counties)
    else:
        print("Error! cannot create the database connection.")
    '''

