#!/usr/bin/python3

# Cleaning and exploring public school datasets
# written by Hansen Feraldy z5083930

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pandas import read_excel

def print_dataframe(dataframe, print_column=True, print_rows=True):
    
    if print_column:
        print(",".join([column for column in dataframe]))

    if print_rows:
        for index, row in dataframe.iterrows():
            print(",".join([str(row[column]) for column in dataframe]))

def write_in_csv(dataframe, file):
	
	dataframe.to_csv(file, sep=',', encoding='utf-8', index=False)

# clean SA data	
def clean_data_sa(dataframe):

	columns_to_drop = ['Site Number', 
						'Latitude', 
						'Longitude', 
						'Category Name', 
						'Subtype Name', 
						'Department Type', 
						'Email Address', 
						'Principal/Manager', 
						'Education Portfolio', 
						'Phone Number',
						'Address']

	dataframe.drop(columns=columns_to_drop, axis=1, inplace=True)
	dataframe.fillna('N/A', inplace=True)

	dataframe.rename(columns={'Site Name':'Name'}, inplace=True)
	dataframe.rename(columns={'Type Name':'School Level'}, inplace=True)

	write_in_csv(dataframe, 'clean_sa_data.csv')

	return dataframe

# clean WA data
def clean_data_wa(dataframe):

	columns_to_drop = ['Code', 
						'Postal Street', 
						'Postal Suburb', 
						'Postal State',
						'Postal Postcode', 
						'Latitude', 
						'Longitude', 
						'Courier Code', 
						'Phone', 
						'Education Region', 
						'Broad Classification',
						'Low Year',
						'High Year',
						'KIN',
						'PPR',
						'Street',
						'State',
						'Total Students',
						'Y01','Y02','Y03','Y04','Y05','Y06','UPR','Y07','Y08','Y09','Y10','Y11','Y12','USE','Unnamed: 36']						


	dataframe.drop(columns=columns_to_drop, axis=1, inplace=True)
	dataframe["Classification Group"].fillna("Not Available", inplace=True)

	dataframe.rename(columns={'School Name':'Name'}, inplace=True)
	dataframe.rename(columns={'Classification Group':'School Level'}, inplace=True)
	dataframe = dataframe[:-1]

	write_in_csv(dataframe, 'clean_wa_data.csv')
	return dataframe

# clean VIC data
def clean_data_vic(dataframe):

	columns_to_drop = ['Education_Sector', 
						'Entity_Type', 
						'School_No', 
						'School_Status',
						'Address_Line_1',
						'Address_Line_2', 
						'Postal_Address_Line_1', 
						'Postal_Address_Line_2', 
						'Postal_Town', 
						'Postal_State', 
						'Postal_Postcode',
						'Full_Phone_No',
						'LGA_ID',
						'LGA_Name',
						'Address_State',
						'X','Y']						

	dataframe.drop(columns=columns_to_drop, axis=1, inplace=True)
	dataframe.fillna('N/A', inplace=True)

	dataframe.rename(columns={'School_Name':'Name'}, inplace=True)
	dataframe.rename(columns={'Address_Town':'Suburb'}, inplace=True)
	dataframe.rename(columns={'School_Type':'School Level'}, inplace=True)
	dataframe.rename(columns={'Address_Postcode':'Postcode'}, inplace=True)

	write_in_csv(dataframe, 'clean_vic_data.csv')
	return dataframe

# clean QLD data
def clean_data_qld(dataframe):

	columns_to_drop = ['School Code', 
						'Latitude', 
						'Longitude', 
						'Centre Type', 
						'Independent Public School', 
						'Actual Floor Number', 
						'Actual Building Name', 
						'Actual Street Number', 
						'OIC Title', 
						'Postal Address 1',
						'Postal Address 2',
						'Postal Address 3',
						'Postal Address Post Code',
						'Band',
						'Phone Number',
						'Restrict Contact to Outside Teaching Hours',
						'Fax Number',
						'Internet Site',
						'Official Low Year Level',
						'Official High Year Level',
						'DoE Geographic Region',
						'Federal Electorate',
						'State Electorate',
						'Local Government Area',
						'Host Centre Name',
						'Host Centre Code',
						'Host Centre Type',
						'Actual Street Name']						

	dataframe.drop(columns=columns_to_drop, axis=1, inplace=True)
	dataframe.fillna('N/A', inplace=True)

	dataframe.rename(columns={'Actual Suburb/Town':'Suburb'}, inplace=True)
	dataframe.rename(columns={'Actual Postcode':'Postcode'}, inplace=True)
	dataframe.rename(columns={'School Name':'Name'}, inplace=True)

	write_in_csv(dataframe, 'clean_qld_data.csv')
	return dataframe

# clean NSW data
def clean_data_nsw(dataframe):

	columns_to_drop = ['School_code', 
						'AgeID', 
						'Phone', 
						'School_Email', 
						'Fax', 
						'Indigenous_pct', 
						'LBOTE_pct', 
						'ICSEA_value', 
						'Selective_school', 
						'Opportunity_class',
						'School_specialty_type',
						'School_subtype',
						'Support_classes',
						'Preschool_ind',
						'Distance_education',
						'Intensive_english_centre',
						'School_gender',
						'Late_opening_school',
						'Date_1st_teacher',
						'LGA',
						'Electorate',
						'Fed_electorate',
						'Operational_directorate',
						'Principal_network',
						'FACS_district',
						'Local_health_district',
						'AECG_region',
						'ASGS_remoteness',
						'Latitude',
						'Longitude',
						'Assets unit',
						'SA4',
						'Healthy canteen',
						'Date_extracted',
						'Street',
						'Student_number']						

	dataframe.drop(columns=columns_to_drop, axis=1, inplace=True)
	dataframe.fillna('N/A', inplace=True)

	dataframe.rename(columns={'School_name':'Name'}, inplace=True)
	dataframe.rename(columns={'Town_suburb':'Suburb'}, inplace=True)
	dataframe.rename(columns={'Level_of_schooling':'School Level'}, inplace=True)

	write_in_csv(dataframe, 'clean_nsw_data.csv')
	return dataframe

if __name__ == '__main__':

	# process South Australian state Data
	sa_df = pd.read_excel('sa_school_list.xlsx', skiprows=2)
	sa_df = clean_data_sa(sa_df)

	#process West Australian state Data
	wa_df = pd.read_excel('wa_school_list.xlsx')
	wa_df = clean_data_wa(wa_df)

	#process Victoria state Data
	vic_df = pd.read_csv('victoria_school_list.csv')
	vic_df = clean_data_vic(vic_df)

	#process Queensland state Data
	qld_df = pd.read_excel('queensland_school_list.xlsx') 
	qld_df = clean_data_qld(qld_df)

	#process NSW State Data
	nsw_df = pd.read_csv('nsw_school_list.csv')
	nsw_df = clean_data_nsw(nsw_df)

	#merege all 5 dataframes
	"""
	merge_df = pd.concat([wa_df,vic_df], ignore_index=True)
	merge_df = pd.concat([merge_df,sa_df], ignore_index=True)
	merge_df = pd.concat([merge_df,qld_df], ignore_index=True)
	merge_df = pd.concat([merge_df,nsw_df], ignore_index=True)

	write_in_csv(merge_df, 'clean_merged_data.csv')
	"""

	#process 
	#print(merge_df)

	
