# OpenWeatherETL

The script is currently set to extract historical weather data for Los Angeles, California, using lat long coordinates.

extract.py will automatically extract the weather data for the ten most populous US cities from OpenWeather API server and load them into a postgres database.
extract.py will automatically create a database and source table to store the weather data.  User can specify the database name and credentials using the credentials.txt file.
extract.py will also convert city_list json file to a pandas dataframe and then finally, a postgres table.

data clean is postgres query script that will join the weather data with the city data, remove duplicates, adjust date and time formats, and insert cleaned weather data into production table without any overlapping.

the production_output excel file is then manually loaded into Tableau Public and is used to create this dashboard:
https://public.tableau.com/views/OpenWeather/Sheet1?:language=en-US&publish=yes&:display_count=n&:origin=viz_share_link

This project is a proof of concept for  the ETL process through an API server.
