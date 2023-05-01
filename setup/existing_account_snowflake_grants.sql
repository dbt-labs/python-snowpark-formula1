/*
If you already have an existing account or accidentally drop your original database, you might need to grant or regrant permissions.
This file is here in the project for these circumstances. A great video on this setup can be found at: https://www.youtube.com/watch?v=kbCkwhySV_I. 
The code below assumes that the tranformer role and transforming warehouse have already been created in Snowflake. 
*/

/*
Giving access to database, schema, and all tables within the schema to our transformer role. 
The permissions here are for the raw data that we read in.
*/
grant usage on database formula1 to role transformer;
grant usage on schema formula1.raw to role transformer;
grant select on all tables in schema formula1.raw to role transformer;

/*
Giving access to database, schema, and all tables within the schema to our transformer role. 
The permissions here are for database and schema for the transformed data that we will write to creating new schemas and tables. 
*/
grant usage on database analytics to role transformer;
grant modify on database analytics to role transformer;
grant monitor on database analytics to role transformer;
grant create schema on database analytics to role transformer;


/*
Give permission for our transformer role to use the warehouse to run our compute and commands on. 
*/
grant operate on warehouse transforming to role transformer;
grant usage on warehouse transforming to role transformer;