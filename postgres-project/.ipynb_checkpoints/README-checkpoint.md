# summary of the project
In this project, we use Postgres to build an ETL pipeline using Python. and define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

# files
data(dir):Contains song info and log info
create_tables.py:Clear all data in tabel(drop all table and create new table)
etl.ipynb:Read song info and log info from json file.Then call sql to set data to tables.
etl.py:Same to etl.ipynb but a python file.
sql_queries.py:drops and creates tables.and insert to tabel or select from table.
test.ipynb:displays the first few rows of each table to let you check your database.

# how to run
1. run create_tables.py to clear all table;
2. run etl.ipynb to get data and set to pqsql db;
3. run test.ipynb to check the data in pqsql db