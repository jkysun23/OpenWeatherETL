import requests
import json
import time
from datetime import datetime
import psycopg2

with open('credentials.txt','r') as keyfile:
    strkey = json.load(keyfile)

API_key = strkey['OWM_API']

with open('credentials.txt','r') as passfile:
    pwd = json.load(passfile)

dbpass = pwd['psql_pass']



start_time = int(time.time()) - 432000
# this returns unix timestamp from 5 days in the past.


print(start_time)
print(datetime.utcfromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S'))

lat = 34.052231
lon = -118.243683

payload = {'lat':lat,'lon':lon,'dt':start_time,'appid':API_key}

site = 'https://api.openweathermap.org/data/2.5/onecall/timemachine'


def getresponse(url):
    try:
        response = requests.get(url, params = payload)
        response.raise_for_status()
        print(response.url)
        print(response.ok)
        return response
    except requests.exceptions.RequestException as e:
        print('Error\n')
        raise SystemExit(e)

def insert_data(cursor,data,hr):
    table_data = (data['lat'],
                  data['lon'],
                  data['timezone'],
                  data['hourly'][hr]['dt'],
                  data['timezone_offset'],
                  data['hourly'][hr]['temp'],
                  data['hourly'][hr]['pressure'],
                  data['hourly'][hr]['humidity'],
                  data['hourly'][hr]['wind_speed'],
                  data['hourly'][hr]['wind_deg'],
                  data['hourly'][hr]['weather'][0]['description'])
    
    query = '''
        INSERT INTO socal_weather_temp(lat, long, timezone, time, timezone_offset, temp, pressure, humidity, wind_speed, wind_deg, description)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    '''
    try:
        cursor.execute(query,table_data)
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)

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





try:
    with psycopg2.connect(
        database = 'socal_weather',
        user = 'postgres',
        password = dbpass,
        host = 'localhost'
    ) as conn:
        with conn.cursor() as cur:
            for past_days in range(0,6):  # 0 for current day all the way back to 5 days ago.
                data = getresponse(site).json()
                for hr in range(0, len(data['hourly'])):
                    insert_data(cur,data,hr)
            ##    print(json.dumps(data['hourly'],indent=4))
                payload['dt'] = payload['dt'] + 86400
                print('\n\n\n')
            print('\nData inserted to staging table successfully.')
            temp_to_final(cur)
            print('\nData moved from staging table to production table.')
            cur.execute('TRUNCATE TABLE socal_weather_temp')

    
    

    # Add data from temp table to main table while ignoring duplicates:
    
    
    
    
except(Exception, psycopg2.DatabaseError)as error:
    print('Error:\n',error)

# Check for other errors: DataError, InternalError, OperationalError, ProgrammingError, etc.
    

conn.close()
print('\nPostgreSQL connection is closed')


# Truncate table deletes all data from a table without scanning it.

##CREATE TABLE la_weather_utc
##AS 
##SELECT lat,long,to_timestamp(time) AT TIME ZONE 'UTC' as utc_time, time, 
##timezone,timezone_offset, temp, pressure, humidity, wind_speed, wind_deg, description
##FROM ca_weather;



