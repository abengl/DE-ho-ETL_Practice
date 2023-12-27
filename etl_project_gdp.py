'''
You have been hired as a junior Data Engineer and are tasked with creating an automated script that can extract the list of all countries in order of their GDPs in billion USDs (rounded to 2 decimal places), as logged by the International Monetary Fund (IMF).
The required information needs to be made accessible as a CSV file Countries_by_GDP.csv as well as a table Countries_by_GDP in a database file World_Economies.db with attributes Country and GDP_USD_billion.

Your boss wants you to demonstrate the success of this code by running a query on the database table to display only the entries with more than a 100 billion USD economy. Also, you should log in a file with the entire process of execution named etl_project_log.txt.

You must create a Python code 'etl_project_gdp.py' that performs all the required tasks.
'''

# CODE FOR ETL OPERATIONS ON COUNTRY-GDP DATA

# 1. Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

# Initialization of known entities
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['Country', 'GDP_USD_millions']
db_name = 'World_economies.db'
table_name = 'Countries_by_GDP'
csv_path = '/home/aben/Documents/IBM/Course3/W2/E1_Practice/Countries_by_GDP.csv'

# 2. Write a data extraction function to retrieve the relevant information from the required URL.

def extract(url, table_attribs):
    ''' 
    This function extracts the required information from the website and saves it to a dataframe. The function returns the dataframe for further processing. 
    '''

    # Extract the web page as text.
    html_page = requests.get(url).text

    # Parse the text into an HTML object.
    data = BeautifulSoup(html_page, 'html.parser')

    # Create an empty pandas DataFrame named df with columns as the table_attribs.
    df = pd.DataFrame(columns=table_attribs)

    # Extract all 'tbody' attributes of the HTML object and then extract all the rows of the index 2 table using the 'tr' attribute.
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    
    """ 
    Check the contents of each row, having attribute <td>, for the following conditions.
    a. The row should not be empty.
    b. The first column should contain a hyperlink.
    c. The third column should not be '—'.
    """
    for row in rows:        
        col = row.find_all('td')
        if len(col) != 0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {
                    "Country": col[0].a.contents[0],
                    "GDP_USD_millions": col[2].contents[0]
                }
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)

    return df

# 3. Transform the available GDP information into 'Billion USD' from 'Million USD'.

def transform(df):
    ''' 
    This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from USD (Millions) to USD (Billions) rounding to 2 decimal places. The function returns the transformed dataframe.
    '''
    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]

    # Divide all these values by 1000 and round it to 2 decimal places.
    GDP_list = [np.round(x/1000,2) for x in GDP_list]
    
    # Modify the name of the column from 'GDP_USD_millions' to 'GDP_USD_billions'.
    df["GDP_USD_millions"] = GDP_list
    df = df.rename(columns={"GDP_USD_millions":"GDP_USD_billions"})

    return df

# 4. Load the transformed information to the required CSV file and as a database file.

def load_to_csv(df, csv_path):
    ''' 
    This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.
    '''
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' 
    This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.
    '''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

# 5. Run the required query on the database.
def run_query(query_statement, sql_connection):
    ''' 
    This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. 
    '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# 6. Log the progress of the code with appropriate timestamps.
def log_progress(message):
    ''' 
    This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing
    '''
    # Year-Monthname-Day-Hour-Minute-Second 
    timestamp_format = '%d-%B-%Y-%H:%M:%S' 
    # Get current timestamp 
    now = datetime.now() 
    timestamp = now.strftime(timestamp_format)

    with open("/home/aben/Documents/IBM/Course3/W2/E1_Practice/testing.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n\n')


''' 
Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.
'''
log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('World_Economies.db')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()

