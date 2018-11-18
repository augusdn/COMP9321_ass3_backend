################################################
# Assignment 3 COMP9321
# Written by group 2b-or-not-2b
################################################
import pandas as pd
from pymongo import MongoClient 

MONGODB_URI = "mongodb://comp9321:comp9321@ds131313.mlab.com:31313/school-database"
myclient = MongoClient(MONGODB_URI, connectTimeoutMS=30000)
mydb = myclient.get_database("school-database")

def write_in_mongodb():
    mydb.authenticate('comp9321','comp9321')
    collection = "test-collection"
    c = mydb[collection]

    #store NSW school data
    nsw_df = pd.read_csv('clean_nsw_data.csv')
    nsw_df['Suburb'] = nsw_df['Suburb'].str.strip()
    nsw_df.insert(0, 'School_id', range(0,len(nsw_df)))
    counter = len(nsw_df[nsw_df['School Level'].str.contains('Primary')])
    counter2 = len(nsw_df[nsw_df['School Level'].str.contains('Secondary')])
    nsw_list = nsw_df.to_dict('records')

    c.insert_one(
        {
            "State" : "New South Wales",
            "State_ID" : "NSW", 
            "Total_Primary_School":counter,
            "Total_Secondary_School":counter2,
            "School_List" : nsw_list
        }
    )


    qld_df = pd.read_csv('clean_qld_data.csv')
    qld_df.insert(0, 'School_id', range(0,len(qld_df)))
    #qld_df.insert(4, 'Total Students', '')
    qld_list = qld_df.to_dict('records')

    c.insert_one(
        {
            "State" : "Queensland",
            "State_ID" : "QLD",
            "Total_Primary_School":"N/A",
            "Total_Secondary_School":"N/A",
            "School_List" : qld_list
        }
    )

    sa_df = pd.read_csv('clean_sa_data.csv')
    sa_df.insert(0, 'School_id', range(0,len(sa_df)))
    #sa_df.insert(5, 'Total Students', '')
    counter = len(sa_df[sa_df['School Level'].str.contains('Primary')])
    counter2 = len(sa_df[sa_df['School Level'].str.contains('Secondary|High')])
    sa_list = sa_df.to_dict('records')

    c.insert_one(
        {
            "State" : "South Australia",
            "State_ID" : "SA",
            "Total_Primary_School":counter,
            "Total_Secondary_School":counter2,
            "School_List" : sa_list
        }
    )

    vic_df = pd.read_csv('clean_vic_data.csv')
    vic_df.insert(0, 'School_id', range(0,len(vic_df)))
    #vic_df.insert(5, 'Total Students', '')
    counter = len(vic_df[vic_df['School Level'].str.contains('Pri')])
    counter2 = len(vic_df[vic_df['School Level'].str.contains('Sec')])
    vic_list = vic_df.to_dict('records')
    
    c.insert_one(
        {
            "State" : "Victoria",
            "State_ID" : "VIC",
            "Total_Primary_School":counter,
            "Total_Secondary_School":counter2,
            "School_List" : vic_list
        }
    )

    wa_df = pd.read_csv('clean_wa_data.csv')
    wa_df.insert(0, 'School_id', range(0,len(wa_df)))
    counter = len(wa_df[wa_df['School Level'].str.contains('PRI',na=False)])
    counter2 = len(wa_df[wa_df['School Level'].str.contains('SEC|HIGH',na=False)])
    wa_list = wa_df.to_dict('records')  

    c.insert_one(
        {
            "State" : "Western Australia",
            "State_ID" : "WA",
            "Total_Primary_School":counter,
            "Total_Secondary_School":counter2,
            "School_List" : wa_list
        }
    )

if __name__ == '__main__':
    write_in_mongodb()