# Data Modelling with Postgres: Sparkifydb

## Summary

This projects attempts to ingest Sparkify music data from S3 json files to AWS Redshift through an ETL process, to be able to perform analytics in the data in an efficient way.

AWS infrastructure was created using Infrastructure as a Code.

## Running the scripts


*sql_queries.py* allows us to design our tables. The script contains the fact and dimension tables for the Data Warehouse, along with the temporary tables used as a staging area to perform the ETL efficiently. Drop, Create and Insert queries are written in order to test the results.

*create_tables.py* allows us to create tables defined in *sql_queries.py*. The script allows us to connect to Redshift's and initialize the database schema. 

*etl.py*, as the name stands, allows us to ingest the data from S3 buckets and poblate it in Redshift. 


## Execution 
1. Execute `create_tables.py` to define the structure of the database. \br
2. Execute `etl.py` to populate the database with S3 data. 

Note: if modifications in the schema are done (through *sql_queries.py*), they're going to be reflected in Redshift if prior to the ETL, *create_tables.py* is executed.

