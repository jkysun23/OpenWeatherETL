# OpenWeather ETL Project

This project is a proof of concept for the Extraction, Transformation, and Loading (ETL) process of weather data from the OpenWeather API server to a Postgres database.  A Tableau dashboard was also created using the populated weather data from the postgres database.

Link to the Tableau dashboard:


### Data used

city.list.json.gz - http://bulk.openweathermap.org/sample/

OpenWeather - https://openweathermap.org/api/one-call-api#history

Top 10 most populous US cities in 2020 - https://en.wikipedia.org/wiki/List_of_United_States_cities_by_population
(Please note that Wikipedia has updated it's ranking to base off of 2021 data, the data used for the dashboard at the time was from 2020 Wikipedia ranking.)

### Extraction - Python

Prior to the start of the extraction process, you must type in your postgres database credentials into the credentials.txt file.

The extraction process is automated by running the extract.py file.  

The extraction process:

1. extract.py creates a database and a source table to store the weather data.  The database credentials will be based on the user specifications set in the credentials.txt file.

2. extract.py then converts the city_list.json file into a pandas dataframe before finally converting the dataframe into a postgres table.

3. Finally, extract.py extracts weather data from OpenWeather API server and loads them into the postgres source table.

### Transformation - Postgres

data clean is postgres query script that will join the weather data with the city data, remove duplicates, adjust date and time formats, and insert cleaned weather data into production table without any overlapping.

the production_output excel file is then manually loaded into Tableau Public and is used to create this dashboard:
https://public.tableau.com/views/OpenWeather/Sheet1?:language=en-US&publish=yes&:display_count=n&:origin=viz_share_link

This project is a proof of concept for the ETL process through an API server.
