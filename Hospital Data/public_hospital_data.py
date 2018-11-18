#!/usr/bin/python3

# Created and written by Nicholas Puglia, z5115237
# Cleaning and exploring hospital dataset

import pandas as pd
import numpy as np
import re

def print_dataframe(dataframe, print_column=True, print_rows=True):
    
    if print_column:
        print(",".join([column for column in dataframe]))

    if print_rows:
        for index, row in dataframe.iterrows():
            print(",".join([str(row[column]) for column in dataframe]))

def write_in_csv(dataframe, file):
	
	dataframe.to_csv(file, sep=',', encoding='utf-8', index=False)

#remove unwanted columns	
def clean_data(dataframe):

	columns_to_drop = ['Establishment ID',
						'Medicare Provider No.',  
						'asgc_ra', 
						'2012-13 Peer Group code', 
						'Provided data for NPHED', 
						'Provided data for NHMD', 
						'Provided data for NAPEDCD', 
						'Provided data for ESWT', 
						'Provided data for NNAPCD', 
						'IHPA funding designation']

	dataframe.drop(columns=columns_to_drop, axis=1, inplace=True)
	dataframe.fillna('Not Available', inplace=True)

	hospital_suburb = []
	hospital_postcode = []
	hospital_number_of_beds = []
	hospital_address = []

	for index, row in dataframe.iterrows():
		hospital_number_of_beds.append(int(row["Number of available beds"]))

	#filter address data into suburb and postcode
	for index, row in dataframe.iterrows():
		
		suburb_data = re.search('(\w+)\s+(\d{4})', row["Address Line 2"])
		address_data = row["Address Line 1"]
		address_data = " ".join(address_data.split())

		if suburb_data is not None:
			suburb = suburb_data.group(1)
			suburb = suburb.lower()
			suburb = suburb.capitalize()
			postcode = suburb_data.group(2)

		address = address_data + " " + suburb + " " + postcode
		hospital_suburb.append(suburb)
		hospital_postcode.append(postcode)
		hospital_address.append(address)

	#print(hospital_suburb)

	dataframe['Suburb'] = hospital_suburb
	dataframe['Postcode'] = hospital_postcode
	dataframe['Address'] = hospital_address
	
	dataframe.drop(columns='Number of available beds', axis=1, inplace=True)
	dataframe['Number of available beds'] = hospital_number_of_beds

	dataframe.drop(columns='Address Line 2', axis=1, inplace=True)
	dataframe.drop(columns='Address Line 1', axis=1, inplace=True)

	#reorder the dataframe columns
	dataframe = dataframe[['State', 'Hospital name', 'Address', 'Suburb', 'Postcode', 'Local Hospital Network identifier', 'Local Hospital Network', 'Remoteness area', 'Number of available beds', 'Peer Group Name']]
	dataframe.columns = ['State', 'Hospital_name', 'Address', 'Suburb', 'Postcode', 'Local_Hospital_Network_identifier', 'Local_Hospital_Network', 'Remoteness_area', 'Number_of_available_beds', 'Peer_Group_Name']
	write_in_csv(dataframe, 'clean_hospital_data.csv')

	return dataframe

'''
def count_hospitals_per_state(dataframe):

	dataframe = dataframe['State'].value_counts()
	#print(dataframe)
	return dataframe

def count_hospitals_per_local_network(dataframe):

	dataframe = dataframe['Local_Hospital_Network'].value_counts()
	#print(dataframe)
	return dataframe


def count_hospitals_by_remoteness(dataframe):

	dataframe = dataframe['Remoteness_area'].value_counts()
	#print(dataframe)
	return dataframe


def count_hospitals_by_peer_group(dataframe):

	dataframe = dataframe['Peer_Group_Name'].value_counts()
	#print(dataframe)
	return dataframe


def list_hospital_by_state(dataframe, state):

	dataframe = dataframe.loc[dataframe['State'] == state]
	#print(dataframe)
	return dataframe

def find_hospital_by_local_network(dataframe, network):

	dataframe = dataframe.loc[dataframe['Local_Hospital_Network'] == network]
	#print(dataframe)
	return dataframe

def find_hospital_by_local_network_id(dataframe, network_id):

	dataframe = dataframe.loc[dataframe['Local_Hospital_Network_identifier'] == network_id]
	#print(dataframe)
	return dataframe

def find_hospital_by_remoteness(dataframe, remoteness):

	dataframe = dataframe.loc[dataframe['Remoteness_area'] == remoteness]
	#print(dataframe)
	return dataframe


#sort dataset by number of beds
def number_of_beds(dataframe):

	dataframe = dataframe.sort_values('Number of available beds', ascending=False)
	#print_dataframe(dataframe.head(5))
	return dataframe

'''
if __name__ == '__main__':

	df = pd.read_csv('public_hospital_list.csv', skiprows=0)

	df = clean_data(df)
'''
	print(count_hospitals_per_state(df))

	print(number_of_beds(df))

	print(count_hospitals_per_local_network(df))

	print(count_hospitals_by_remoteness(df))

	print(count_hospitals_by_peer_group(df))

	print(list_hospital_by_state(df, 'NSW'))

	print(find_hospital_by_local_network(df, 'Sydney'))

	print(find_hospital_by_remoteness(df, 'Major Cities'))

	print(find_hospital_by_local_network_id(df, 504))

	list hospitals by number of beds in a specific state
	df1 = list_hospital_by_state(number_of_beds(df), 'QLD')
	print(df1.to_string())
'''


