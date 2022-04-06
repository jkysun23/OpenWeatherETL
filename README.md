# OpenWeatherETL

Run create_table.py and create_temp_table files first to load up the table and staging table in postgres server.

Then run request to extract weather data from Openweather API and load it into the table.

The API can only fetch weather data up to 5 days into the past.  It returns weather data hourly.  The script is currently set to extract weather data for Los Angeles, California.

This is merely a proof of concept for data extraction through API servers.
