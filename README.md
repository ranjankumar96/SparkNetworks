# SparkNetworks

The assesment is completed. It includes the following 3 assesments solutions:
A>	Create a data collector Python script doing jobs: 
      file name: 'main.py' & 'crontab.py'
  1. connect via HTTP Request to the two endpoints 
  2. create/populate three tables (Users, Subscriptions and Messages).
  3. avoid the ingestion of duplicated data into the tables
  
 
B>  propose how to model the tables, columns and relationships
      file name: 'data model.jpeg'
      
C> add a file sql_test.sql in your project with the queries that solve the below questions:
      file name: 'sql_test.sql'
      
 
 #Installation
install the library
import requests as requests
import json as json
from pyspark.pandas import spark
from pyspark.sql import SparkSession
from pandas import read_json
from dataframe_sql import register_temp_table, query

#Endpoints
https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/users
https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/messages
