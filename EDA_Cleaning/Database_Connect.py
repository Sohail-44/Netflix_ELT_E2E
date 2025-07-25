# Loading data in postgre SQL -- Netflix_End_2_End 
import psycopg2
import pandas as pd

''' Credentials '''

hostname = "localhost" 
database = "Netflix_E2E"
username = "postgres"
pwd = "Sohail"
port_id = 5432
cur = None

''' Connecting'''

connection = psycopg2.connect(
            host= hostname,
            dbname = database,
            user = username,
            password = pwd, 
            port = port_id)

cur = connection.cursor()

''' Reading CSV, adjusting Columns and Loading'''

df = pd.read_csv('StayOnFlix/EDA_Cleaning/netflix_titles.csv')
df['date_added'] = pd.to_datetime(df['date_added'].str.strip(), format='%B %d, %Y').dt.strftime('%Y-%m-%d')

# Saving the cleaned CSV to temporary file without a header to avoid header repetetion as I aleady created header in Database 
df.to_csv('StayOnFlix/EDA_Cleaning/netflix_cleaned.csv', index=False, header=False)

cur.execute("TRUNCATE TABLE Netflix;") # clearing the table to avoid duplicate and missing of first row

with open('StayOnFlix/EDA_Cleaning/netflix_cleaned.csv', 'r') as f:
    # next(f) # to skip 1st row (since we don't want to skip first row)
    cur.copy_expert("COPY Netflix_Rawdata FROM STDIN WITH CSV", f)
 # postgre SQL's copy command via phychopg2's copy_expert() method, it tells sql to bulk load data. Put your table's name

connection.commit()
cur.close()




