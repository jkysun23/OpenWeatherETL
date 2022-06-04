# OpenWeather ETL Project

This project is a proof of concept for the  (Extraction, Transformation, and Loading) process of weather data from the OpenWeather API server to a Postgres database.  A Tableau dashboard was also created using the populated weather data from the postgres database.

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

### Transformation and Loading - Postgres

While the data transformation and loading process can be fully automated as well, I chose to write it as a postgresql script for demonstrative purposes.  

Please run the data clean script for data transformation and loading.

The transformation and loading process:

1. Create a staging table by joining the weather data from souce_table with the city_list data from the city_list table.
2. Convert unix time into UTC time and local time relative to the city.  The UTC and local times are then further broken down into separate columns by date and time.
3. Check and delete any duplicate data.
4. Create production table.
5. Load the transformed weather data from the staging table into the production table.  The newly loaded data does not overlap with any of existing data in the production table.

### Dashboard - Tableau

The output from the production table was copy and pasted into the excel file, production_table_output.xlsx, which was then used to create the dashboard:
https://public.tableau.com/views/OpenWeather/Sheet1?:language=en-US&publish=yes&:display_count=n&:origin=viz_share_link

