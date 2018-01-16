# import specified json file into a mongodb database

import sys
from pymongo import MongoClient
import subprocess

dbname = sys.argv[1]
collection_name = sys.argv[2]
input_file_name = sys.argv[3]

print 'Loading data from --> File: {}'.format(input_file_name)
print 'into --> Collection: {}'.format(collection_name)
print 'within --> Database: {}'.format(dbname)


def import_query(dbname, collection_name, input_file_name):
    mongoimport_query = 'mongoimport --db {} --collection {} {}'.format(dbname, collection_name, input_file_name)
    return mongoimport_query


client = MongoClient() #Create connection default-localhost:27017
db = client[dbname]   #Connect to database

#Insert all data from the input_file_name into the mongo database
mongoimport_query = import_query(dbname, collection_name, input_file_name)
subprocess.call(mongoimport_query, shell=True)

print 'File successfully imported into mongodb.'