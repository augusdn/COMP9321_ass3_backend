##################################
# Title: COMP9321 - Assignment 3 #
# Author: Yuxing Wu              #
# Info: Process local area       #  
#       government land sizes    #
##################################

#!/usr/bin/env python3

# import statements
import pandas as pd
import re

# ignore unimportant warnings
pd.options.mode.chained_assignment = None

if __name__ == "__main__":
    # get local area csv file
    lga_csv = 'lga_aus.csv'
    df = pd.read_csv(lga_csv)
    
    # columns to remove
    columns_to_drop = ['LGA_CODE11', 'STE_CODE11']
    df.drop(columns_to_drop, inplace=True, axis=1)
    
    # rows to remove
    pattern = 'No usual address'
    filter_rows = df['LGA_NAME11'].str.contains(pattern)
    df = df[~filter_rows] 
    
    # rename column names
    df = df.rename(columns = {'LGA_NAME11': 'local_area', 
                              'STE_NAME11': 'state_id',
                              'AREA_SQKM': 'area_size'})
    
    # change state\territory names to abbrieviated
    df['state_id'] = df['state_id'].replace({'New South Wales': 'NSW', 
                                             'Queensland': 'QLD',
                                             'Victoria': 'VIC',
                                             'South Australia': 'SA',
                                             'Northern Territory': 'NT',
                                             'Australian Capital Territory': 'ACT',
                                             'Tasmania': 'TAS',
                                             'Western Australia': 'WA',
                                             'Other Territories': 'OT'})
    
    # set index to lga name
    df = df.set_index(df.columns[0])
    
    # clean up names
    df.index = df.index.map(lambda x: re.sub('\s\(.*', '', x))
    
    # round up area size to 2 decimal places
    df = df.round(2)
    
    # output to clean csv file
    output_file = "clean_" + lga_csv
    df.to_csv(output_file, index=True)
    