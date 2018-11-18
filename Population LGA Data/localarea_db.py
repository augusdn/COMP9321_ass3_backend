###################################
# Title: COMP9321 - Assignment 3  #
# Author: Yuxing Wu               #
# Info: Local area sizes Database #
###################################

#!/usr/bin/env python3

# import statements
import pandas as pd
from pymongo import MongoClient

if __name__ == '__main__':
   # connect to MongoDB
   client = MongoClient('mongodb://user:pass123@ds137263.mlab.com:37263/localarea-database')
   db = client['localarea-database']

   # get clean csv file for local areas
   csv_file = 'clean_lga_aus.csv'
   
   # populate the database
   collection = db['localarea_sizes']
   
   # convert csv file into pandas df
   df = pd.read_csv(csv_file)

   # insert dataframe into mlab
   collection.insert_many(df.to_dict('records'))