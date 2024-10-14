# Code for ETL operations on Largest Banks data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
import os

# Function to log the progress of the ETL process
def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the 
    code execution to a log file. Function returns nothing.'''
    timestamp_format = '%Y-%m-%d %H:%M:%S'  # Year-Month-Day Hour:Minute:Second
    now = datetime.now()  # Get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("code_log.txt", "a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

# Function to extract data from the specified URL
def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)

    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    
    print(f"Total rows found: {len(rows)}")  # Masih bisa dibiarkan untuk pengecekan

    for row in rows:
        col = row.find_all('td')
        if len(col) >= 3:
            bank_name = col[1].get_text(strip=True)
            mc_usd = col[2].get_text(strip=True)
            
            if bank_name: 
                data_dict = {
                    "Name": bank_name,
                    "MC_USD_Billion": mc_usd
                }
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)

    return df      

# Function to transform the dataframe with currency conversion
def transform(df, exchange_rate):
    ''' This function converts the market capitalization to GBP, EUR, and INR 
    based on the exchange rate information from a CSV file. '''
    # Reading the exchange rate CSV file and converting it to a dictionary
    exchange_rate = pd.read_csv(exchange_rate)
    exchange_dict = exchange_rate.set_index('Currency').to_dict()['Rate']
    
    # Remove commas and convert the 'MC_USD_Billion' column to numeric, forcing any errors to NaN
    df['MC_USD_Billion'] = pd.to_numeric(df['MC_USD_Billion'].str.replace(',', ''), errors='coerce')

    # Adding new columns for market capitalization in different currencies
    df['MC_GBP_Billion'] = [np.round(x * exchange_dict['GBP'], 2) if not np.isnan(x) else np.nan for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_dict['EUR'], 2) if not np.isnan(x) else np.nan for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_dict['INR'], 2) if not np.isnan(x) else np.nan for x in df['MC_USD_Billion']]
    
    # Convert the new columns to float type
    df['MC_GBP_Billion'] = df['MC_GBP_Billion'].astype(float)
    df['MC_EUR_Billion'] = df['MC_EUR_Billion'].astype(float)
    df['MC_INR_Billion'] = df['MC_INR_Billion'].astype(float)

    return df

# Function to load the dataframe into a CSV file
def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a CSV file in the provided path. 
    Function returns nothing.'''
    df.to_csv(csv_path, index=False)

# Function to load the dataframe into a database
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe to a database table 
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

# Function to run queries on the database and print results
def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and 
    prints the output on the terminal. Function returns nothing. '''
    print(f"Running query: {query_statement}")
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# Main script to execute the ETL process
if __name__ == "__main__":
    # Defining constants
    url = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
    table_attribs = ["Name", "MC_USD_Billion"]
    db_name = 'Banks.db'
    table_name = 'Largest_banks'
    csv_path = './Largest_banks_data.csv'
    exchange_rate_path = './exchange_rate.csv'

    # Remove the existing log file if present
    if os.path.exists("code_log.txt"):
        os.remove("code_log.txt")
        print("Existing code_log.txt file has been removed.")

    # Logging the start of the ETL process
    log_progress('Preliminaries complete. Initiating ETL process')

    # Step 1: Data Extraction
    df = extract(url, table_attribs)
    log_progress('Data extraction complete. Initiating Transformation process')

    # Step 2: Data Transformation
    df = transform(df, exchange_rate_path)
    log_progress('Data transformation complete. Initiating loading process')

    # Step 3: Load to CSV
    load_to_csv(df, csv_path)
    log_progress('Data saved to CSV file')

    # Step 4: Load to Database
    sql_connection = sqlite3.connect(db_name)
    log_progress('SQL Connection initiated.')
    
    load_to_db(df, sql_connection, table_name)
    log_progress('Data loaded to Database as table. Executing queries')

    # Step 5: Running Queries
    run_query(f"SELECT * FROM {table_name}", sql_connection)
    log_progress('Printed the contents of the entire table.')
    
    run_query(f"SELECT AVG(MC_USD_Billion) FROM {table_name}", sql_connection)
    log_progress('Printed the average market capitalization of all the banks in Billion USD.')
    
    run_query(f"SELECT Name FROM {table_name} LIMIT 5", sql_connection)
    log_progress('Printed the names of the top 5 banks.')

    log_progress('Process Complete.')

    # Closing the SQL connection
    sql_connection.close()
    log_progress('Server Connection closed.')
