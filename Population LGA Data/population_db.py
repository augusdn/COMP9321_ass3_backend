##################################
# Title: COMP9321 - Assignment 3 #
# Author: Yuxing Wu              #
# Info: Population Database      #
##################################

#!/usr/bin/env python3

# import statements
import pandas as pd
from pymongo import MongoClient
import re
import os

if __name__ == '__main__':
   # connect to MongoDB
   client = MongoClient('mongodb://user:pass123@ds129003.mlab.com:29003/population-database')
   db = client['population-database']  
    
   # get list of csv_files
   csv_list = os.listdir()
   csv_list = [file for file in csv_list if 'clean_' in file]

   for csv_file in csv_list:
       # get year of the csv file
       retreive_year = re.match('clean_([\d]{4})_population\.csv', csv_file)
       year = retreive_year.group(1)
       collection = db[year]
       
       # convert csv file into pandas df
       df = pd.read_csv(csv_file, index_col=0)
       
       # get unique local area names + states
       lga_list = df.index.unique().tolist()
       state_list = df['State/Territory'].unique().tolist()

       # store as a document by state->lga
       for state in state_list:
           for lga in lga_list:
               # fetch all dataframe rows with this lga
               temp_df = df[df['State/Territory'] == state]
               temp_df = temp_df[temp_df.index == lga]
               
               if temp_df.empty:
                   continue
               
               all_ages = temp_df['Population'].sum()
               
               # store all the population of this lga by age group
               age_group = []
               for index, row in temp_df.iterrows():
                   group = {"group_name": row['Age Group'], "population_count": row['Population']}
                   age_group.append(group)
               
               # model to store the data on mlab
               local_doc = [{ "all_ages": int(all_ages),
                              "age_group": age_group
                           }]
               state_doc = { "state_id" : state,
                             "year": year,  
                             "local_area" : lga,
                             "local_population": local_doc
                           }
               
               # insert document to collection by year
               collection.insert_one(state_doc)