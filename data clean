DROP TABLE IF EXISTS staging_table;

/* create staging table with the source_table and city_list table inner joined */
CREATE TABLE staging_table AS 
SELECT id, name, state, country, a.lat, a.long,time,timezone, 
timezone_offset/3600 as utc_timezone_offset, temp, pressure, humidity, wind_speed, wind_deg, description  
FROM source_table a
INNER JOIN city_list b
ON a.lat = ROUND(b.lat::numeric,4)
AND a.long = ROUND(b.long::numeric,4);

/* Added time columns */
ALTER TABLE staging_table
ADD utc_datetime timestamp,
ADD local_datetime timestamp;

/*convert unix time for utc and local time to date time format*/
UPDATE staging_table
SET utc_datetime = to_timestamp(time) AT TIME ZONE 'UTC',
	local_datetime = to_timestamp(time + (utc_timezone_offset*3600)) AT TIME ZONE 'UTC';
	

ALTER TABLE staging_table
ADD utc_date date,
ADD utc_time time;

/* breaking utc datetime column into individual date and time columns */
UPDATE staging_table
SET
	utc_date = utc_datetime::date,
	utc_time = utc_datetime::time;

ALTER TABLE staging_table
ADD local_date date,
ADD local_time time;

/* breaking local datetime column into individual date and time columns */
UPDATE staging_table
SET
	local_date = local_datetime::date,
	local_time = local_datetime::time;

ALTER TABLE staging_table
DROP COLUMN time, 
DROP COLUMN utc_datetime,
DROP COLUMN local_datetime;

/* Check the cities in the staging table */
SELECT name FROM staging_table
GROUP BY name;

/* deleting some cities used to test python extraction code */
DELETE FROM staging_table
WHERE name = 'Manhattan' or name ='New York County';


-- SELECT *, ROW_NUMBER() OVER (PARTITION BY id, name, state, country, lat, long, utc_date, utc_time
-- ORDER BY id) row_num FROM staging_table
-- ORDER BY utc_date, utc_time;

/* select the duplicate rows */

WITH row_numCTE AS(
SELECT id, name, state, country, lat, long, utc_date, utc_time, ROW_NUMBER() OVER (PARTITION BY id, name, state, country, lat, long, 
utc_date, utc_time ORDER BY id, utc_date, utc_time) as row_num  /*the order by sorts the rows within the partition*/
FROM staging_table)
select * FROM row_numCTE
WHERE row_num > 1;

/* delete dupe rows from CTE in postgresql does not work */
-- note: no primary key or unique identifiers, so use ctid.  Usually in sql server, you would

-- DELETE FROM staging_table t1
-- USING staging_Table t2
-- WHERE t1.ctid < t2.ctid
-- AND t1.id = t2.id
-- AND t1.utc_date = t2.utc_date
-- AND t1.utc_time = t2.utc_time;
-- Postgresql doesn't support delete from join, so we use delete from using.
-- WHERE clause is used to specify which columns to join with the columns from the USING clause.


-- Alternative way of deleting duplicates using DELTE FROM WHERE subquery
DELETE FROM staging_table
	WHERE EXISTS (SELECT * FROM staging_table t2
				 WHERE t2.id = staging_table.id
				 AND t2.utc_date = staging_table.utc_date
				 AND t2.utc_time = staging_table.utc_time
				 AND t2.ctid < staging_table.ctid);

CREATE TABLE IF NOT EXISTS production_table (LIKE staging_table);

-- Insert 
INSERT INTO production_table(id, name, state, country, lat, long, timezone, utc_timezone_offset, temp, pressure, humidity, wind_speed, wind_deg, description, utc_date, utc_time, local_date, local_time)
SELECT * FROM staging_table 
WHERE NOT EXISTS(SELECT * FROM production_table
WHERE staging_table.id = production_table.id
AND staging_table.utc_date = production_table.utc_date
AND staging_table.utc_time = production_table.utc_time)
ORDER BY name, utc_date, utc_time;
	
SELECT * FROM production_table;
