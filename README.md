# Data Warehouse Project

## **Project description:**
This project intends to attend a needance of the analytics team, 
of the Sparkfy Company, that is a music streaming startup,
creating a Data Warehouse cloud based with AWS, based on the 
JSON log files they collected from songs and users activities,
as well as a directory with JSON metadata on the songs in their app.
The main objective of this project is to build an ETL pipeline that
extracts data from a S3 bucket, stages them in Redshift and transforms
data into a set of dimensional tables.

The ETL pipeline was built as follow:
First the extraction of the data from JSON files, that are in the S3 bucket,
and using the COPY statement, load it into staging tables.
After that, the ETL call the insert statements which are responsible
for some transformation.
Finally the data was loaded into the previously created postgres tables.

## **Using this Pipeline**
Before start running, is necessary to fill the file dwh.cfg with the information
of the cluster and IAM.
To run this pipeline, the first thing to do is to run the create_tables.py,
which will drop all tables existing with the choosed names, and create new 
blank tables, based on the SQL statements that was created in the sql_queries.py
file.

```bash
python create_tables.py
```

Then run the etl.py, that will extract the data from JSON files, that are
in the S3 bucket, and load it into the staging tables created in the first step.
After that, it will start to transform some data and do insert into the fact and
dimesional tables.

```bash
python etl.py
```

## Files in this repository:
### create_tables.py:
Responsible for drop and create all the tables, using the sql_queries.py.

### dwh.cfg:
A config file that holds the parameters of the cluster, IAM and S3 bucket.

### etl.py:
Responsible for the ETL pipeline execution.

### sql_queries.py
Contains the commands to CREATE, DROP and INSERT into the tables, and serves
the create_tables.py.