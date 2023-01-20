/*
This is our setup script to create a new database for the Formula1 data in Snowflake.
We are copying data from a public s3 bucket into snowflake by defining our csv format and snowflake stage. 
*/
-- create and define our formula1 database
create or replace database formula1;
use database formula1; 
create or replace schema raw; 
use schema raw; 

-- define our file format for reading in the csvs 
create or replace file format csvformat
type = csv
field_delimiter =','
field_optionally_enclosed_by = '"', 
skip_header=1; 

--
create or replace stage formula1_stage
file_format = csvformat 
url = 's3://formula1-dbt-cloud-python-demo/formula1-kaggle-data/';

-- load in the 8 tables we need for our demo 
-- we are first creating the table then copying our data in from s3
-- think of this as an empty container or shell that we are then filling
create or replace table formula1.raw.circuits (
	CIRCUITID NUMBER(38,0),
	CIRCUITREF VARCHAR(16777216),
	NAME VARCHAR(16777216),
	LOCATION VARCHAR(16777216),
	COUNTRY VARCHAR(16777216),
	LAT FLOAT,
	LNG FLOAT,
	ALT NUMBER(38,0),
	URL VARCHAR(16777216)
);
-- copy our data from public s3 bucket into our tables 
copy into circuits 
from @formula1_stage/circuits.csv
on_error='continue';

create or replace table formula1.raw.constructors (
	CONSTRUCTORID NUMBER(38,0),
	CONSTRUCTORREF VARCHAR(16777216),
	NAME VARCHAR(16777216),
	NATIONALITY VARCHAR(16777216),
	URL VARCHAR(16777216)
);
copy into constructors 
from @formula1_stage/constructors.csv
on_error='continue';

create or replace table formula1.raw.drivers (
	DRIVERID NUMBER(38,0),
	DRIVERREF VARCHAR(16777216),
	NUMBER VARCHAR(16777216),
	CODE VARCHAR(16777216),
	FORENAME VARCHAR(16777216),
	SURNAME VARCHAR(16777216),
	DOB DATE,
	NATIONALITY VARCHAR(16777216),
	URL VARCHAR(16777216)
);
copy into drivers 
from @formula1_stage/drivers.csv
on_error='continue';

create or replace table formula1.raw.lap_times (
	RACEID NUMBER(38,0),
	DRIVERID NUMBER(38,0),
	LAP NUMBER(38,0),
	POSITION FLOAT,
	TIME VARCHAR(16777216),
	MILLISECONDS NUMBER(38,0)
);
copy into lap_times 
from @formula1_stage/lap_times.csv
on_error='continue';

create or replace table formula1.raw.pit_stops (
	RACEID NUMBER(38,0),
	DRIVERID NUMBER(38,0),
	STOP NUMBER(38,0),
	LAP NUMBER(38,0),
	TIME VARCHAR(16777216),
	DURATION VARCHAR(16777216),
	MILLISECONDS NUMBER(38,0)
);
copy into pit_stops 
from @formula1_stage/pit_stops.csv
on_error='continue';

create or replace table formula1.raw.races (
	RACEID NUMBER(38,0),
	YEAR NUMBER(38,0),
	ROUND NUMBER(38,0),
	CIRCUITID NUMBER(38,0),
	NAME VARCHAR(16777216),
	DATE DATE,
	TIME VARCHAR(16777216),
	URL VARCHAR(16777216),
	FP1_DATE VARCHAR(16777216),
	FP1_TIME VARCHAR(16777216),
	FP2_DATE VARCHAR(16777216),
	FP2_TIME VARCHAR(16777216),
	FP3_DATE VARCHAR(16777216),
	FP3_TIME VARCHAR(16777216),
	QUALI_DATE VARCHAR(16777216),
	QUALI_TIME VARCHAR(16777216),
	SPRINT_DATE VARCHAR(16777216),
	SPRINT_TIME VARCHAR(16777216)
);
copy into races 
from @formula1_stage/races.csv
on_error='continue';

create or replace table formula1.raw.results (
	RESULTID NUMBER(38,0),
	RACEID NUMBER(38,0),
	DRIVERID NUMBER(38,0),
	CONSTRUCTORID NUMBER(38,0),
	NUMBER NUMBER(38,0),
	GRID NUMBER(38,0),
	POSITION FLOAT,
	POSITIONTEXT VARCHAR(16777216),
	POSITIONORDER NUMBER(38,0),
	POINTS NUMBER(38,0),
	LAPS NUMBER(38,0),
	TIME VARCHAR(16777216),
	MILLISECONDS NUMBER(38,0),
	FASTESTLAP NUMBER(38,0),
	RANK NUMBER(38,0),
	FASTESTLAPTIME VARCHAR(16777216),
	FASTESTLAPSPEED FLOAT,
	STATUSID NUMBER(38,0)
);
copy into results 
from @formula1_stage/results.csv
on_error='continue';

create or replace table formula1.raw.status (
	STATUSID NUMBER(38,0),
	STATUS VARCHAR(16777216)
);
copy into status 
from @formula1_stage/status.csv
on_error='continue';