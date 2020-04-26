### Project: Data Modeling with Postgres

#### Introduction
Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is  particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The end result of this project will be a Postgres database with tables designed to optimize queries on song play analysis which will satisfy the needs of the analytics team.

#### Project Description
In this project, as a data engineer I have applied data modeling with Postgres and build an ETL pipeline using Python. I have defined fact and dimension tables for a star schema for a particular analytic focus, and written an ETL pipeline that transfers data from JSON files in two local directories into the tables in Postgres using Python and SQL. 

#### Data Model

**Fact Table** 

_songplays_ - records in log data associated with song plays, records with page NextSong.

**Dimension Tables**

_users_ - Users in the app.

_songs_ - Songs in music database.

_artists_ - Artists in music database.

_time_ - timestamps of records in songplays broken down into specific units

#### Database Schema

The database schema is normalized and well suited for the data analyist team to perform their analysis by doing the required joins.

#### ETL Flow
ETL proces reads the json files, parses the data into dataframes and then loads them into the tables following all the business rules.

**ETL Files**
_create_tables.py_ - Drops and creates new tables. This file is to reset your tables before each run of the ETL scripts.
_etl.ipynb_ - Reads and processes a single file from song_data and log_data and loads the data into the tables.
_etl.py_ - Reads and processes files from song_data and log_data and loads them into the tables.
_sql_queries.py_ - Contains all sql queries, and is imported into create_tables.py, etl.ipynb and etl.py.
_test.ipynb_ - Displays the first few rows of each table in the database.
_README.md_ - Contains detailed description of this project.

**Data Files**
_song_data_ - These files are of JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.
-log_data_ - These files consists of log files in JSON format, based on the songs in the dataset above. The log files are partitioned by year and month.

**ETL Flow Execution**
Step 1 and 2 should be executed from the terminal(using python <FileName>.py command for .py files and ipynb using Jupyter) and should be executed in the same order.
_1.create_tables.py_ - Execute this Python code drop tables and create new tables before each run of the flow.
_2.etl.py_ - Execute this Python code to read and load song_data and log_data into the Postgres database.
_3.test.ipynb_ - This is not a mandatory execution step, but can be used to validate the data after the execution of above two steps.