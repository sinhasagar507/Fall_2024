from pymongo import MongoClient
import json
import re 

client = MongoClient('localhost', 27017)
print(client.list_database_names())