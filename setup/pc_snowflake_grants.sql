/*
If you accidentally drop your original database after setting up partner connect, you might need to grant or regrant permissions to build your dbt models.
This file is here in the project for these circumstances. A great video on this setup can be found at: https://www.youtube.com/watch?v=kbCkwhySV_I. 
The code below assumes that the PC_DBT_ROLE role and PC_DBT_WH warehouse have already been created in Snowflake as part of the partner connect process. 
*/

/*
Giving access to database, schema, and all tables within the schema to our transformer role. 
The permissions here are for the raw data that we read in.
*/
grant usage on database formula1 to role PC_DBT_ROLE;
grant usage on schema formula1.raw to role PC_DBT_ROLE;
grant select on all tables in schema formula1.raw to role PC_DBT_ROLE;

/*
Giving access to database, schema, and all tables within the schema to our PC_DBT_ROLE role. 
The permissions here are for database and schema for the transformed data that we will write to creating new schemas and tables. 
*/
grant usage on database PC_DBT_DB to role PC_DBT_ROLE;
grant modify on database PC_DBT_DB to role PC_DBT_ROLE;
grant monitor on database PC_DBT_DB to role PC_DBT_ROLE;
grant create schema on database PC_DBT_DB to role PC_DBT_ROLE;


/*
Give permission for our PC_DBT_ROLE role to use the PC_DBT_WH warehouse to run our compute and commands on. 
*/
grant operate on warehouse PC_DBT_WH to role PC_DBT_ROLE;
grant usage on warehouse PC_DBT_WH to role PC_DBT_ROLE;