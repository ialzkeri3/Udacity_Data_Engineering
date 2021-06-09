# Sparkify - Data Modeling and Pipelining

## Context

Sparkigy is a startup that recently has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The purpose of this project is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.

## Database Schema

The schema that has been seen to meet this project requirements is the Star Schema. It includes one fact table containing all measures that is called song plays. Along with other four dimension tables: time, users, songs, and artists. Each primary key in dimension tables is referenced from foreign key in the fact table.

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

#### Fact Table

**songplays** - records in log data associated with song plays i.e. records with page NextSong

##### Dimension Tables

**time** - timestamps of records in songplays broken down into specific units

**users** - users in the app

**songs** - songs in music database

**artists** - artists in music database

## Project Structure

1- dl.cfg is a configuration file containing AWS IAM credentials

2- etl.py Extracts data from S3, processe it, and load it back in S3 using Spark

3- README.md provides discussion on process and decisions for this project with some instructions on how to run.

## Steps to run the project

1- fill dl.cfg file with the your AWS credentials

```bash
[AWS]
AWS_ACCESS_KEY_ID=''
AWS_SECRET_ACCESS_KEY=''
```

2- Specify desired output data path in the main function of etl.py

3- Run etl.py

### Hope You Enjoy the Project
