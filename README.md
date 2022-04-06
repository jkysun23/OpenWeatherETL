# OpenWeatherETL

Run create_table.py and create_temp_table files first to load up the table and staging table in postgres server.

Then run request to extract data from Openweather API and load it into the table.

The API can only fetch data up to 5 days into the past.  It returns weather data hourly.

This is merely a proof of concept for data extraction through API servers.
