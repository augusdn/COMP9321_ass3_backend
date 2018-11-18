#!/usr/bin/python3

#################################################
# Assignment 3 COMP9321							#
# Written by group 2b-or-not-2b					#
#################################################

from pymongo import MongoClient
import pandas as pd
import numpy as np 

####################################################################################
# username: 2b-or-not-2b														   #
# password: 2b-or-not-2b														   #
# uri     : mongodb://2b-or-not-2b:2b-or-not-2b@ds151382.mlab.com:51382/mydatabase #
####################################################################################


MONGODB_URI = "mongodb://2b-or-not-2b:2b-or-not-2b@ds151382.mlab.com:51382/mydatabase"
myclient = MongoClient(MONGODB_URI, connectTimeoutMS=30000)
mydb = myclient.get_database("mydatabase")


def write_in_mongodb_hospital():

	mydb.authenticate('2b-or-not-2b','2b-or-not-2b')
	collection = "Hospitals"
	c = mydb[collection]

	hospital_df = pd.read_csv('clean_hospital_data.csv')
	hospital_df.insert(0, 'Hospital_id', range(0, len(hospital_df)))
	hospital_list = hospital_df.to_dict('records')

	document_count = c.count_documents({})
	#print(document_count)
	#print(hospital_df)
	if document_count == 0:
		c.insert_one(
			{
				"Hospital List": hospital_list
			}
		)
	else:
		c.delete_one({})
		c.insert_one(
			{
				"Hospital List": hospital_list
			}
		)


if __name__ == '__main__':
    write_in_mongodb_hospital()
