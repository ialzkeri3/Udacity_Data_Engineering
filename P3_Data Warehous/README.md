# Sparkify - Data Modeling and Pipelining

## Context

Sparkigy is a startup that recently made a new music streaming app.  Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. The analytics team is interested in what songs users are listening to.

The purpose of this project is to build an ETL pipeline for a database hosted on Redshift.

## Database Schema

The schema that has been seen to meet this project requirements is the Star Schema. It includes one fact table containing all measures that is called song plays. Along with other four dimension tables: time, users, songs, and artists. Each primary key in dimension tables is referenced from foreign key in the fact table. A representation of how this Star Schema is structured is in the following image:

![Database Design](db_design.png)

Why does Relational Database meets product requirement:

- Easier to change requirements.
  - Sparkify is a growing startup. And it is most likely to change its requirement.

- Aggregation methods are very useful for Analytics team.

- Queries are more felxible and powerful with joins

- No need for big data solutions as data amount is limited.

Why does Star Schema meets product requirement:

- Denormalized form improves query performance which is the main purpose of building the database.

- Improves readability and easy to follow.

### Tables structure detailed

#### Staging Tables

**staging_events**
**staging_songs**


#### Fact Table

**songplays** - records in log data associated with song plays i.e. records with page NextSong

- songplay_id (int) PRIMARY KEY: a unique ID givin to each songplay event
- start_time (timestamp) REFERENCES from time table: timestamp define the start of a certain user activity
- user_id (int) REFERENCES from users table: ID of the user concerned to this certain activity
- level (varchar): the tupe of the user, eather free or paid
- song_id (varchar) REFERENCES from songs table: ID of the played song
- artist_id (varchar) REFERENCES from artists table: ID of the artist of the played song
- session_id (int): ID of the user session
- location (varchar): location of the user
- user_agent (varchar): The agent used by the user to access Sparkify platform i.e. Chrome, Firefox

##### Dimension Tables

**time** - timestamps of records in songplays broken down into specific units

- start_time (TIMESTAMP) PRIMARY KEY: time stamp of the record, acts like an ID
- hour (int): hour identified by the timestamp
- day (int): day identified by the timestamp
- week (int): week identified by the timestamp
- month (int): month identified by the timestamp
- year (int): year identified by the timestamp
- weekday (varchar): weekday identified by the timestamp

**users** - users in the app

- user_id (int) PRIMARY KEY: user ID
- first_name (varchar) NOT NULL: user first name
- last_name (varchar) NOT NULL: user last name
- gender (varchar): user gender, Male or Female
- level (varchar): the type of the user, eather free or paid

**songs** - songs in music database

- song_id (varchar) PRIMARY KEY: song ID
- title (varchar) NOT NULL: song tilte
- artist_id (varchar) REFERENCES from artists table: song artist ID
- year (int): year when song is released
- duration (float) NOT NULL: duration of the song in milliseconds

**artists** - artists in music database

- artist_id (varchar) PRIMARY KEY: artist ID
- name (varchar) NOT NULL: name of the artist
- location (varchar): the city where the artists lives
- latitude (float): latitude of artist's location
- longitude (float): longitude of artist's location

## Project Structure

1- create_table.py creates your fact and dimension tables for the star schema in Redshift.

2- IaC.ipnyb can be used to create and delete the cluster on the cloud.

3- etl.py loads data from S3 into staging tables on Redshift and then process that data into analytics tables on Redshift.

4- sql_queries.py defines SQL statements, which will be imported into the two other files above.

5- dwh.cfg defines data warehouse configuration and credentials

6- README.md provides discussion on process and decisions for this project with some instructions on how to run.

## Steps to run the project

1- fill dwh.cfg file with the information needed

```bash
[CLUSTER]
HOST= ''
DB_NAME= ''
DB_USER= ''
DB_PASSWORD= ''
DB_PORT= 5439

[AWS]
KEY=
SECRET=

[S3]
LOG_DATA= 's3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

[IAM_ROLE]
ARN= 

[DWH] 
DWH_CLUSTER_TYPE       = multi-node
DWH_NUM_NODES          = 4
DWH_NODE_TYPE          = dc2.large
DWH_IAM_ROLE_NAME      =
DWH_CLUSTER_IDENTIFIER =
DWH_DB                 =
DWH_DB_USER            =
DWH_DB_PASSWORD        =
DWH_PORT               = 5439

```

2- Run IaC.ipynb to create Redshift cluster and other files ready to connect

3- Run create_cluster.py to create database schema, and to make our data warehouse ready for pipelining

```bash
python create_tables.py
```

4- Run in terminal etl.py to extract data from the files in S3, stage it in redshift, and finally store it in the dimensional tables.

```bash
python etl.py
```

5- Use IaC.ipynb to delete cluster if you are not going to use it again.

### Hope You Enjoy the Project
