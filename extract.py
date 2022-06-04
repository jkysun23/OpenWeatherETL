import requests
import json
import time
from datetime import datetime
import psycopg2
import pandas as pd
from sqlalchemy import create_engine


with open('credentials.txt','r') as f:
    cred = json.load(f)

API_key = cred['OWM_API']
db_pass = cred['psql_pass']
db_user = cred['psql_user']
db_name = cred['db_name']
hostname = cred['host']

# city ids of the 10 most populous cities in the US.  I'm using 10 for proof of concept and to not overwhelm the api server.
ids = [4560349,4684888,4699066,4726206,4887398,5128581,5308655,5368361,5391811,5392171]


with open('city_list.json', encoding = 'UTF-8') as city_file:
    city_data = json.load(city_file)

  
df = pd.json_normalize(city_data)
df.rename(columns = {'coord.lon':'long', 'coord.lat':'lat'}, inplace = True)
df.set_index('id', inplace=True)


def getresponse(url):
    try:
        response = requests.get(url, params = payload)
        response.raise_for_status()
##        print(response.url)
        print("Request Status Code:", response.ok)
        return response
    except requests.exceptions.RequestException as e:
        print('Error\n')
        raise SystemExit(e)

def create_db(cursor):
    # check if database openweather_db exists, if not exist, create it.
    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %(db_name)s", {'db_name':db_name})  # Select 1 is used instead of Select * to reduce processing time.
    exists = cursor.fetchone()  # fetchone method returns one record as a tuple or none if theres none.
##    print(exists)
    if not exists: # sequences (strings, lists, tuples_ that are empty are false.  None is false.
        # so if the variable 'exists' is none, then the if not exists statement is satisfied and the following block is executed.
        cursor.execute("CREATE DATABASE " + str(db_name))
        print(str(db_name) + ' created successfully. \n')
    else:
        print('Database already exists.\n')

def create_table(cursor):
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS source_table(
            lat FLOAT,
            long FLOAT,
            timezone text,
            time bigint,
            timezone_offset INTEGER,
            temp FLOAT,
            pressure FLOAT,
            humidity FLOAT,
            wind_speed FLOAT,
            wind_deg FLOAT,
            description text
            );''')
        print('Source table created successfully.\n')   
    except(Exception, psycopg2.DatabaseError)as error:
        print('Error while creating PostgreSQL table:\n',error)

def insert_data(cursor,api_data,hr):
# please keep in mind that the original insert_data function inserts data into a temp table to filter out duplicates before being appended to the final table.
    table_data = (api_data['lat'],
                  api_data['lon'],
                  api_data['timezone'],
                  api_data['hourly'][hr]['dt'],
                  api_data['timezone_offset'],
                  api_data['hourly'][hr]['temp'],
                  api_data['hourly'][hr]['pressure'],
                  api_data['hourly'][hr]['humidity'],
                  api_data['hourly'][hr]['wind_speed'],
                  api_data['hourly'][hr]['wind_deg'],
                  api_data['hourly'][hr]['weather'][0]['description'])
    
    query = '''
        INSERT INTO source_table(lat, long, timezone, time, timezone_offset, temp, pressure, humidity, wind_speed, wind_deg, description)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    '''
    try:
        cursor.execute(query,table_data)  # this paasses the parameters into the sql query.
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)

# This function was used to remove duplicates inserted into our table through filtering in a temp table.
# However, for the purpose of demonstrating removing duplicates via SQL query, this function was not used.
def temp_to_final(cursor):
    try:
        cursor.execute('''
            INSERT INTO socal_weather(lat, long, timezone, time, timezone_offset, temp, pressure, humidity, wind_speed, wind_deg, description)
            SELECT * FROM socal_weather_temp
            WHERE NOT EXISTS(SELECT * FROM socal_weather WHERE
            socal_weather.lat = socal_weather_temp.lat
            AND socal_weather.long = socal_weather_temp.long
            AND socal_weather.time = socal_weather_temp.time)
            ORDER BY time;
        ''')
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)


    
def get_lat_lon(id):
    for value in city_data:
        if value['id'] == id:
            return value['coord']



start_time = int(time.time()) - 432000
# this returns unix timestamp from 5 days in the past.


# print(start_time)
print("\nRequest datetime: ", datetime.utcfromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S'),"\n")

site = 'https://api.openweathermap.org/data/2.5/onecall/timemachine'


conn = psycopg2.connect(user = db_user,password = db_pass,host = hostname)
conn.autocommit = True
cur = conn.cursor()
create_db(cur)

cur.close()
conn.close()

conn = psycopg2.connect(database = db_name, user = db_user,password = db_pass,host = hostname)
conn.autocommit = True
cur = conn.cursor()


engine = create_engine('postgresql://'+ str(db_user) +':'+ str(db_pass)+'@' + str(hostname) + '/'+ str(db_name))
engine_conn = engine.connect()

result = engine_conn.execute("SELECT 1 FROM city_list")
exists = result.fetchone()
if not exists:
    with open('city_list.json', encoding = 'UTF-8') as city_file:
        city_data = json.load(city_file)
        print('Converting city_list.json to pandas dataframe...\n')
        df = pd.json_normalize(city_data)
        df.rename(columns = {'coord.lon':'long', 'coord.lat':'lat'}, inplace = True)
        df.set_index('id', inplace=True)

    print('Importing city_list to postgres table: city_list...\n')
    df.to_sql('city_list', con=engine_conn)
    print('city_list table created\n')
else:
    print('city_list already exists.\n')


create_table(cur)

for i in ids:
    lon = get_lat_lon(i)['lon']
    lat = get_lat_lon(i)['lat']
    print(lat)
    print(lon)
    print('\n')

    payload = {'lat':lat,'lon':lon,'dt':start_time,'appid':API_key, 'units':'imperial'}

    try:
        
        for past_days in range(0,6):  # 0 for current day all the way back to 5 days ago.
            api_data = getresponse(site).json()
            for hr in range(0, len(api_data['hourly'])):
                insert_data(cur,api_data,hr)
    ##            print(json.dumps(api_data['hourly'],indent=4))
            print("Inserted weather data for UTC date:",datetime.utcfromtimestamp(payload['dt']).strftime('%Y-%m-%d %H:%M:%S'))
            payload['dt'] = payload['dt'] + 86400
            print('\n')
        print('Data inserted to source table successfully.\n\n')
            ##            temp_to_final(cur)
            ##            print('\nData moved from staging table to production table.')
            ##            cur.execute('TRUNCATE TABLE source_table_temp')

    except(Exception, psycopg2.DatabaseError)as error:
        print('Error:\n',error)
        pass

# Check for other errors: DataError, InternalError, OperationalError, ProgrammingError, etc.

# Truncate table deletes all data from a table without scanning it.

cur.close()
conn.close()
engine_conn.close()
engine.dispose()
print('\nPostgreSQL connection is now closed\n')
