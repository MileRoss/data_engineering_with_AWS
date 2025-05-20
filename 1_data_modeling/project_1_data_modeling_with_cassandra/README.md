
# Project 1: Data Modeling with Cassandra


## Business problem
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.  
The analysis team is particularly interested in understanding what songs users are listening to.  
Currently, there is no easy way to query the data to generate the results.  
The data reside in a directory of CSV files on user activity on the app.


## Requested solution
Sparkify needs an Apache Cassandra database which can create queries on song play data to answer the questions.  


## Starter code provided
Sparkify provide a part of the ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.  
They have a project template that takes care of all the imports and provides a structure for the ETL pipeline needed to process this data.


## Datasets
- Used the `event_data` directory containing CSV files partitioned by date:
  - Example files: `event_data/2018-11-08-events.csv`, `event_data/2018-11-09-events.csv`


## Steps Taken


### ETL Pipeline
- Parsed and processed all CSV files into a single denormalized file: `event_datafile_new.csv`
- Cleaned and transformed the data to prepare for Cassandra insertion


### Data Modeling in Cassandra
- Designed three tables to support specific queries defined by the analytics team
- Used meaningful table names based on the type of query each supports
- Defined appropriate `PRIMARY KEY` columns using partition keys and clustering columns
- Ensured correct datatypes for all fields


### Data Loading
- Inserted transformed data into Cassandra tables using `INSERT` statements
- Verified data correctness by running the corresponding `SELECT` queries


## Notes on Modeling
- Chose primary keys based on the query requirements:
  - Used single or composite keys to ensure uniqueness and query efficiency
- Avoided using `ALLOW FILTERING` in queries


## Final Touches
- Structured the Jupyter Notebook cleanly with modular code
