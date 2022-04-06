import psycopg2
import json


with open('credentials.txt','r') as passfile:
    pwd = json.load(passfile)

dbpass = pwd['psql_pass']


try:
    with psycopg2.connect(
        database = 'socal_weather',
        user = 'postgres',
        password = dbpass,
        host = 'localhost'
    ) as conn:
        with conn.cursor() as cur:
            cur.execute('''
                    CREATE TABLE socal_weather(
                    lat FLOAT,
                    long FLOAT,
                    timezone VARCHAR(50),
                    time bigint,
                    timezone_offset INTEGER,
                    temp FLOAT,
                    pressure FLOAT,
                    humidity FLOAT,
                    wind_speed FLOAT,
                    wind_deg FLOAT,
                    description VARCHAR(50)
                    );''')
            
    print('\nTable created successfully.')
    
except(Exception, psycopg2.DatabaseError)as error:
    print('Error while creating PostgreSQL table:\n',error)
    

conn.close()
print('PostgreSQL connection is closed')

