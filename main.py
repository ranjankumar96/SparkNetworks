#importing packages
import requests as requests
import json as json
from pyspark.pandas import spark
from pyspark.sql import SparkSession
from pandas import read_json
from dataframe_sql import register_temp_table, query

#https Request Endpoints
endpoint1 = requests.get('https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/users')
endpoint2 = requests.get('https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/messages')

#writing endpoints to local folder
with open(r'E:\Company\SparkNetworks\endpoint1_json','wb') as f:
    f.write(endpoint1.content)

with open(r'E:\Company\SparkNetworks\endpoint2_json','wb') as f:
    f.write(endpoint2.content)

#Create/Populate dataframe
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

#Read Json tables
message = spark.read.json("E:\Company\SparkNetworks.endpoint1_json.json")
User = spark.read.json("E:\Company\SparkNetworks.endpoint2_json.json")
Subscription = spark.read.json("E:\Company\SparkNetworks.endpoint2_json.json")

message.show()
message.printSchema()

#CREATE/POPULATE & truncate duplicate TABLE: message
message.select(message['createdAt'], message['message'], message.[receiverId], message.[id], message[senderId]).show()
message.drop_duplicates(inplace=True)

#CREATE/POPULATE & truncate duplicate TABLE: Users
User.select(User['createdAt'], User['updatedAt'], User.[firstName], User.[id], User[lastName].show(), User.[address],
User.[city], User.[country], User.[zipCode] User.[email], User.[birthDate], User.[gender], User.[isSmoking], User.[profession],
User.[income], User.[id]).show()
User.drop_duplicates(inplace=True)

#CREATE/POPULATE & truncate duplicate TABLE: Subscription
subscription.select(subscription['createdAt'], subscription['startDate'], subscription.[endDate],subscription['status'], subscription['amount']).show()
subscription.drop_duplicates(inplace=True)

#create temporary view until the sessoion is ON
df.createOrReplaceTempView("message")
df.createOrReplaceTempView("User")
df.createOrReplaceTempView("subscription")