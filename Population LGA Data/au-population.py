#############################################
# Title: COMP9321 - Assignment 3            #
# Author: Yuxing Wu                         #
# ZID: z5061719                             #
# Info: Process Australia's population      #
#       based on Age and LGA (2001 to 2017) #  
#############################################

# import statements
import pandas as pd
import os
import re

# ignore unimportant warnings
pd.options.mode.chained_assignment = None

# cleanse the lga dataset (seperate dataset to linked lga with its state)
def cleanse_lga(dataframe):
    # columns to remove
    columns_to_drop = ['LGA_CODE11', 'STE_CODE11', 'AREA_SQKM']
    dataframe.drop(columns_to_drop, inplace=True, axis=1)
    
    # rows to remove
    pattern = 'No usual address'
    filter_rows = dataframe['LGA_NAME11'].str.contains(pattern)
    dataframe = dataframe[~filter_rows]   

# drop columns
def drop_columns(dataframe):
    # columns to remove
    columns_to_drop = ['MEASURE',
                       'Measure',
                       'SEX_ABS',
                       'Sex',
                       'AGE',
                       'LGA_2017',
                       'FREQUENCY',
                       'Frequency',
                       'TIME',
                       'Time',
                       'Flag Codes',
                       'Flags']
    dataframe.drop(columns_to_drop, inplace=True, axis=1)

# drop rows
def drop_rows(dataframe):
    dataframe = dataframe[dataframe['Region'] != 'Australia']
    dataframe = dataframe[dataframe['Region'] != 'New South Wales']
    dataframe = dataframe[dataframe['Region'] != 'Victoria']
    dataframe = dataframe[dataframe['Region'] != 'Queensland']
    dataframe = dataframe[dataframe['Region'] != 'South Australia']
    dataframe = dataframe[dataframe['Region'] != 'Western Australia']
    dataframe = dataframe[dataframe['Region'] != 'Tasmania']
    dataframe = dataframe[dataframe['Region'] != 'Northern Territory']
    dataframe = dataframe[dataframe['Region'] != 'Australian Capital Territory']
    dataframe = dataframe[dataframe['Region'] != 'Other Territories']
    return dataframe
    
if __name__ == "__main__":
    # fetch the population csv files from 2001 to 2017 (seperate files) in directory
    dir_items = os.listdir()
    csv_files = [x for x in dir_items if re.search('\.csv$', x)]
    csv_files = [x for x in csv_files if 'ABS_ANNUAL_ERP' in x]      

    # create seperate dataset with local government area and the corresponding state
    # cleanse the dataframe to use for joining
    lga_csv = 'lga_aus.csv'
    df_lga = pd.read_csv(lga_csv)
    cleanse_lga(df_lga)
    
    # rename column names
    df_lga = df_lga.rename(columns = {'LGA_NAME11': 'Local Government Area', 'STE_NAME11': 'State/Territory'})
    
    # change state\territory names to abbrieviated
    df_lga['State/Territory'] = df_lga['State/Territory'].replace({'New South Wales': 'NSW', 
                                                                   'Queensland': 'QLD',
                                                                   'Victoria': 'VIC',
                                                                   'South Australia': 'SA',
                                                                   'Northern Territory': 'NT',
                                                                   'Australian Capital Territory': 'ACT',
                                                                   'Tasmania': 'TAS',
                                                                   'Western Australia': 'WA',
                                                                   'Other Territories': 'OT'})
    
    # set index to lga name
    df_lga = df_lga.set_index(df_lga.columns[0])
    
    # clean up names
    df_lga.index = df_lga.index.map(lambda x: re.sub('\s\(.*', '', x))
    
    # process each population csv file
    for csv_file in csv_files:
        # get the year of the data (to include in the name of output file)
        find_year = re.match('ABS_ANNUAL_ERP_LGA([\d]{4})\.csv$', csv_file)
        csv_year = find_year.group(1) 
        
        # store csv as pandas dataframe
        df = pd.read_csv(csv_file)
        
        # drop useless rows/columns
        drop_columns(df)
        df = drop_rows(df)
        
        # rename and rearrange columns
        df = df.rename(columns = {'Age': 'Age Group', 'Region': 'Local Government Area', 'Value': 'Population'})
        df = df[['Local Government Area', 'Age Group', 'Population']]
        
        # set index to lga name
        df = df.set_index(df.columns[0])
    
        # clean up names
        df.index = df.index.map(lambda x: re.sub('\s\(.*', '', x))
        
        # merge the lga and population datasets
        merged_df = pd.merge(df_lga, df, left_index=True, right_index=True)
        
        # output to csv
        output_file = "clean_" + csv_year + "_population.csv"
        merged_df.to_csv(output_file, index=True)
