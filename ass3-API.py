################################################
# Assignment 3 COMP9321                        #
# Written by group 2b-or-not-2b                #
################################################

# import statements
from flask import Flask
from flask_cors import CORS
from flask_restplus import Resource, Api
from pymongo import MongoClient
import requests

# Machine Learning Model
import pandas as pd
from sklearn import linear_model

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})
api = Api(app, default="Demographics API",
               title="Demographics Dataset",
               description="Service to retrieve data on Australia's demographics.")

##############################################################################
# username: comp9321                                                         #
# password: comp9321                                                         #
# uri     : mongodb://comp9321:comp9321@ds243501.mlab.com:43501/hieumaiunswdb#
##############################################################################
#MONGODB_URI = "mongodb://comp9321:comp9321@ds243501.mlab.com:43501/hieumaiunswdb"

##### SCHOOL MLAB CONNECTION #####
MONGODB_URI = "mongodb://comp9321:comp9321@ds131313.mlab.com:31313/school-database"
myclient = MongoClient(MONGODB_URI, connectTimeoutMS=30000)
mydb = myclient.get_database("school-database")

##### HOSPITAL MLAB CONNECTION #####
MONGODB_URI_HOSPITAL = "mongodb://2b-or-not-2b:2b-or-not-2b@ds151382.mlab.com:51382/mydatabase"
myclient_hospital = MongoClient(MONGODB_URI_HOSPITAL, connectTimeoutMS=30000)
mydb_hospital = myclient_hospital.get_database("mydatabase")

##### POPULATION MLAB CONNECTION #####
# connect to MongoDB
client = MongoClient('mongodb://user:pass123@ds129003.mlab.com:29003/population-database')
db = client['population-database']

##### LGA MLAB CONNECTION #####
client_lga = MongoClient('mongodb://user:pass123@ds137263.mlab.com:37263/localarea-database')
db_lga = client_lga['localarea-database']

##### SCHOOL API #####
@api.route('/school/<state_id>')
@api.param('state_id', 'State name i.e. NSW')
class School(Resource):
    @api.response(200, "List of schools returned sucessfully")
    @api.response(404, "State not found in the collection")
    @api.doc(description="Retrieve list of all schools in the state given")
    def get(self,state_id):
        query = get_school_by_state(state_id)
        if not query:
            return{
                "error":"State not found in collection"
            },404
        else:
            return{
                "school_list": query['School_List']
            },200

@api.route('/school/<state_id>/<suburb>')
@api.param('state_id', 'State name i.e. NSW')
@api.param('suburb', 'Suburb name i.e. Kingsford')
class School_by_state(Resource):
    @api.response(200, "Count of schools by suburb returned sucessfully")
    @api.response(404, "State not found in the collections")
    @api.doc(description="Retrieve count of school in a particular suburb")
    def get(self,state_id,suburb):
        counter = count_school_by_suburb(state_id, suburb)
        if not counter:
            return{
                "error":"Collection_id not found"
            },404
        else:
            return{
                "state_id": state_id,
                "suburb": suburb,
                "count": counter

            },200

@api.route('/school/<state_id>/<postcode>')
@api.param('state_id', 'State name i.e. NSW')
@api.param('postcode', 'Postcode i.e. 2020')
class School_by_postcode(Resource):
    @api.response(200, "Count of schools by postcode returned sucessfully")
    @api.response(404, "State not found in the collections")
    @api.doc(description="Retrieve count of school in a particular postcode")
    def get(self,state_id,postcode):
        counter = count_school_by_post_code(state_id, postcode)
        if not counter:
            return{
                "error":"Collection_id not found"
            },404
        else:
            return{
                "state_id": state_id,
                "postcode": postcode,
                "count": counter
            },200

@api.route('/school/<state_id>/data')
@api.param('state_id', 'State name i.e. NSW')
class School_data(Resource):
    @api.doc(description="Retrieve count of Primary and Secondary schools available in a state")
    @api.response(200, "School data succesfully returned")
    @api.response(404, "State not found in the collection")
    def get(self,state_id):
        query = get_school_by_state(state_id)
        if not query:
            return{
                "error":"State not found in collection"
            },404
        else:
            return{
                "state_id": state_id,
                "Total_Primary_School":query['Total_Primary_School'],
                "Total_Secondary_School":query['Total_Secondary_School'],
            },200

##### POPULATION API #####
@api.route('/population/<state_id>/<local_area>')
@api.param('state_id', 'State name i.e. NSW')
@api.param('local_area', 'Local government area name i.e. Sydney, Randwick, etc')
class LocalPopulation(Resource):
    @api.response(200, "Population data returned sucessfully")
    @api.response(400, "Invalid local government area entered")
    @api.response(404, "State not found in the collections")
    @api.doc(description="Retrieve all populations of a certain local government area for the entire period (2001-2017")
    def get(self, state_id, local_area):
        # retrieve list of collections
        collection_list = db.collection_names()
        collection_list.remove('system.indexes')
        collection_list.sort()

        # check if valid state
        collection = db[collection_list[0]]
        valid = collection.find_one({"state_id": state_id})
        if not valid:
            return { "message": "State not found in the collections!" }, 404

        # find the population history for this local area
        population_history = []
        for year in collection_list:
            collection = db[year]
            query = collection.find_one({ "state_id": state_id, "local_area": local_area })
            if not query:
                return { "message": "Invalid local government area entered" }, 400

            pop_hist = { "year": query['year'],
                         "population_count": query['local_population'][0]['all_ages'] }
            population_history.append(pop_hist)

        ret = { "state_id": query['state_id'],
                "local_area": query['local_area'],
                "population_history": population_history }
        return ret, 200

@api.route('/population/<state_id>/<population_year>/data')
@api.param('state_id', 'State name i.e. NSW')
@api.param('population_year', 'Year between 2001 and 2017')
class Population(Resource):
    @api.response(200, "Population data returned sucessfully")
    @api.response(400, "Invalid state entered")
    @api.response(404, "Year not found in the collection")
    @api.doc(description="Retrieve all local government area populations of a certain year and state")
    def get(self, state_id, population_year):
        # retrieve list of collections
        collection_list = db.collection_names()
        collection_list.remove('system.indexes')

        # check if selected year is available to get population data
        if (population_year not in collection_list):
            return { "message": "Year not found in collection!" }, 404

        # find the correct document in the collection with the selected state/territory and year
        collection = db[population_year]
        query = collection.find({"state_id" : state_id, "year" : population_year})
        if query.count() == 0:
            return { "message": "Invalid State"}, 400

        result = []
        for doc in query:
            ret = { "state_id": doc['state_id'],
                    "year": doc['year'],
                    "local_area": doc['local_area'],
                    "local_population": doc['local_population']}
            result.append(ret)

        return { "result": result }, 200

@api.route('/population/<state_id>/<population_year>/<local_area>')
@api.param('state_id', 'State name i.e. NSW')
@api.param('population_year', 'Year between 2001 and 2017')
@api.param('local_area', 'Local government area name i.e. Sydney, Rankwick, etc')
class PopulationArea(Resource):
    @api.response(200, "Population data returned sucessfully")
    @api.response(400, "Invalid state or local government area entered")
    @api.response(404, "Year not found in the collection")
    @api.doc(description="Retrieve all populations of a certain year, state and local government area")
    def get(self, state_id, population_year, local_area):
        # retrieve list of collections
        collection_list = db.collection_names()
        collection_list.remove('system.indexes')

        # check if selected year is available to get population data
        if (population_year not in collection_list):
            return { "message": "Year not found in collection!" }, 404

        # find the specific document with the select year, state/territory and lga
        collection = db[population_year]
        query = collection.find_one({"state_id" : state_id, "year" : population_year, "local_area": local_area})
        if not query:
            return { "message": "Invalid state and/or local area"}, 400

        ret = { "state_id": query['state_id'],
                "year": query['year'],
                "local_area": query['local_area'],
                "local_population": query['local_population']}
        return ret, 200

@api.route('/population/history')
class PopulationHistory(Resource):
    @api.response(200, "Population history data returned sucessfully")
    @api.response(404, "State not found in the collection!")
    @api.doc(description="Get population history for Australia from starting year (2001) to ending year (2017)")
    def get(self):
       # retrieve list of collections
       collection_list = db.collection_names()
       collection_list.remove('system.indexes')
       collection_list.sort()

       # create a table to store the population history sorted by each state/territory
       table = [{ "state_id": "NSW", "population_history": [] },
                { "state_id": "QLD", "population_history": [] },
                { "state_id": "VIC", "population_history": [] },
                { "state_id": "ACT", "population_history": [] },
                { "state_id": "NT", "population_history": [] },
                { "state_id": "WA", "population_history": [] },
                { "state_id": "SA", "population_history": [] },
                { "state_id": "TAS", "population_history": [] },
                { "state_id": "OT", "population_history": [] }
               ]
       for year in range(2001,2018):
           collection = db[str(year)]
           pipeline = [ { "$unwind": "$local_population" },
                        { "$group": { "_id": "$state_id", "totalPop": { "$sum": "$local_population.all_ages"} } }
                      ]
           result = list(collection.aggregate(pipeline))

           for item in result:
               tup = { "state_id": item['_id'], "population_history": { "year": year, "current_pop": item['totalPop'] } }
               related_dict = next((doc for doc in table if doc['state_id'] == tup['state_id']))
               related_dict['population_history'].append(tup['population_history'])

       return { "result": table }, 200

@api.route('/population/history/<state_id>')
@api.param('state_id', 'State name i.e. NSW')
class PopulationHistory_State(Resource):
    @api.response(200, "Population history data returned sucessfully")
    @api.response(404, "State not found in the collection!")
    @api.doc(description="Get population history for a specific state from starting year (2001) to ending year (2017)")
    def get(self, state_id):
       # fetch the population history for the entire period (2001 to 2017)
       # filter out the other states/territories except for the selected/chosen one
       url = 'http://127.0.0.1:5000/population/history'
       req = requests.get(url)
       req_table = req.json()
       table = next((item for item in req_table['result'] if item['state_id'] == state_id), None)

       # return result and check if empty response then the state must be non existant
       if table == None:
           return { "message": "State does not exist in the collection!" }, 404
       else:
           return { "result": table }, 200

@api.route('/population/growth/<state_id>')
@api.param('state_id', 'State name i.e. NSW')
class PopulationGrowth(Resource):
    @api.response(200, "Population growth data returned sucessfully")
    @api.response(404, "State not found in the collection!")
    @api.doc(description="Get population growth for a specific state from starting year (2001) to ending year (2017)")
    def get(self, state_id):
       # fetch the population history for the entire period (2001 to 2017)
       url = 'http://127.0.0.1:5000/population/history'
       req = requests.get(url)
       req_table = req.json()
       table = req_table['result']
       table = [tup for tup in table if tup['state_id'] == state_id]

       # check if state/territory exists
       if not table:
           return { "message:": "State does not exist in the colletion!" }, 404

       pop_history = table[0]['population_history']

       # calculate the growth rate between the current year and the previous years starting from 2001
       growth_hist = []
       for year in range(2001, 2018):
           if (year == 2001):
               growth_rate = 0.00
               growth = { "year": year, "growth_rate": growth_rate }
               growth_hist.append(growth)
           else:
               prev_item = next((item for item in pop_history if item['year'] == year-1))
               curr_item = next((item for item in pop_history if item['year'] == year))

               prev_pop = prev_item['current_pop']
               curr_pop = curr_item['current_pop']

               growth_rate = (curr_pop/prev_pop - 1)*(100)
               growth = { "year": year, "growth_rate": growth_rate }
               growth_hist.append(growth)

       ret = { "state_id": state_id,
               "growth_history": growth_hist }
       return ret, 200

@api.route('/population/prediction/<state_id>/<prediction_year>')
@api.param('state_id', 'State name i.e. NSW')
@api.param('prediction_year', 'Year to predict population')
class PopulationPrediction_State(Resource):
    @api.response(200, "Population data returned sucessfully")
    @api.response(404, "State not found in the collection!")
    @api.doc(description="Get population prediction for state, based on previous population data")
    def get(self, state_id, prediction_year):
       # fetch the population for all states/territories for the entire period (2001 to 2017)
       url = 'http://127.0.0.1:5000/population/history'
       req = requests.get(url)
       req_table = req.json()
       table = next((item for item in req_table['result'] if item['state_id'] == state_id), None)

       # if empty return then must be a non existant state
       if table == None:
           return { "message": "State does not exist in the collection!" }, 404

       # put the population historical data into the model to produce a prediction
       population_history = table['population_history']

       columns = ["year", "current_pop"]
       df = pd.DataFrame(data=population_history, columns=columns)

       pop_X_train = df.drop('current_pop', axis=1).values
       pop_y_train = df['current_pop'].values

       model = linear_model.LinearRegression()
       model.fit(pop_X_train, pop_y_train)
       ftr = pd.DataFrame({'Year': [prediction_year]})
       future = model.predict(ftr)
       predict_population = future[0]

       # put predictor result into a year and predicted_population format
       result_table = [{ "year": prediction_year,
                         "predict_population": predict_population
                      }]

       return { "result": result_table }, 200

##### LGA API #####
@api.route('/lga/size/<state_id>')
@api.param('state_id', 'State name i.e. NSW')
class LgaSize(Resource):
    @api.response(200, "Local area land sizes data returned sucessfully")
    @api.response(404, "State/Territory not found in the collection")
    @api.doc(description="Retrieve the local area land sizes by state")
    def get(self, state_id):
        # retrieve documents from the collection
        database = 'localarea_sizes'
        collection = db_lga[database]
        query = collection.find({ "state_id": state_id })

        # if not documents are returned, then it must be a non existant state
        if query.count() == 0:
            return { "message": "State is not found in collection" }, 404

        # put all the local government area document information in to a list
        lga_list = []
        for item in query:
            row = { "local_area": item['local_area'], "area_size": item['area_size'] }
            lga_list.append(row)

        # return the list and the state requested
        ret = {
                "state_id": state_id,
                "lga_list": lga_list
              }
        return ret, 200

##### HOSPITAL API #####
@api.route('/hospital/<string:state_id>')
@api.param('state_id', 'State name i.e. NSW')
class Hospitals(Resource):
    @api.response(200, "Hospitals in given state returned successfully")
    @api.response(400, "Input entered in not valid")
    @api.response(404, "State/Territory not found in the collection")
    @api.doc(description="Retrieve hospital information for specific states")
    def get(self, state_id):
        #check if the given input is a state
        states = ['NSW', 'VIC', 'QLD', 'SA', 'WA', 'ACT', 'NT', 'TAS']
        state_id = state_id.upper()
        if state_id not in states:
            return {
                "Error": "Input not valid"
            }, 400
        #get the list of hospitals with given state
        query = get_hospital_by_state(state_id)
        #if no data is found return an error otherwise return the list
        if not query:
            return{
                "Error": "State is not found in collection"
            },404
        else:
            return{
                "hospital_list": query
            },200

@api.route('/hospital/<string:state_id>/data')
@api.param('state_id', 'State name i.e. NSW')
class HospitalData(Resource):
    @api.response(200, "Hospital data in given state returned successfully")
    @api.response(400, "Input entered in not valid")
    @api.response(404, "State/Territory not found in the collection")
    @api.doc(description="Retrieve hospital data for specific states e.g. Number of available beds in each region")
    def get(self, state_id):
        #check if the given input is a state
        states = ['NSW', 'VIC', 'QLD', 'SA', 'WA', 'ACT', 'NT', 'TAS']
        state_id = state_id.upper()
        if state_id not in states:
            return {
                "Error": "Input not valid"
            }, 400
        #get the list of hospital data with given state
        hospital_data, region_beds, region_hospitals = get_hospital_data_by_state(state_id)
        #if no data is found return an error otherwise return the list
        if not hospital_data:
            return{
                "Error": "State is not found in collection"
            },404
        else:
            return{
                "hospital_data": hospital_data,
                "region_total_available_beds": region_beds,
                "region_total_hospitals": region_hospitals
            },200

##### SUPPORT FUNCTIONS FOR THE APIS #####
# list of hospitals by selected state
def get_hospital_by_state(state_id):
    #get the hospital data from mlab database
    collection = "Hospitals"
    c = mydb_hospital[collection]
    records = c.find_one()
    #new list to return hospitals in given state
    hospitals = []
    #for every hospital in the database, check if the state matches the given state and add it to the empty list
    for hospital in records['Hospital List']:
        if hospital['State'] == state_id:
        	hospitals.append(hospital)

    return hospitals

# list of regions in the state with total hospitals and number of beds in each region
def get_hospital_data_by_state(state_id):
    #get the hospital data from mlab database
    collection = "Hospitals"
    c = mydb_hospital[collection]
    records = c.find_one()
    #new list to return hospital data in given state
    hospital_data = []
    #counters for total beds and hospitals in a state
    bed_count = 0
    hospital_count = 0
    #dictionary for the beds and hospitals in each region within a state
    region_total_beds = {}
    region_total_hospitals = {}
    #for every hospital in the database check if the state matches the given state
    #create a new key in the dictionary as the region name
    for hospital in records['Hospital List']:
        if hospital['State'] == state_id:
            region_total_beds[hospital['Local_Hospital_Network']] = 0
            region_total_hospitals[hospital['Local_Hospital_Network']] = 0

    #for every hospital in the database check if the state matches the given state
    #add the number of available beds to the dictionary with the key as the region
    #do the same for number of hospitals in each region
    #increment the total state bed count by the number of available beds for the current hospital
    #increment the total state hospital count by 1
    for hospital in records['Hospital List']:
        if hospital['State'] == state_id:
            region_total_beds[hospital['Local_Hospital_Network']] += int(hospital['Number_of_available_beds'])
            region_total_hospitals[hospital['Local_Hospital_Network']] += 1

            bed_count += hospital['Number_of_available_beds']
            hospital_count += 1

    #convert the bed_count to int
    #append the total bed and hospital count to the empty list to return
    bed_count = int(bed_count)
    hospital_data.append("Total bed count: {}".format(bed_count))
    hospital_data.append("Hospital count: {}".format(hospital_count))

    return hospital_data, region_total_beds, region_total_hospitals

# list of schools by chosen state
def get_school_by_state(state_id):
	collection = "test-collection"
	c = mydb[collection]
    #query collections in the database where State_iD is equal to state_id
	query = {"State_ID": state_id}
	return c.find_one(query)

# count number of schools based on suburb
def count_school_by_suburb(state_id, suburb_name):
    collection = "test-collection"
    c = mydb[collection]
    counter = 0
    query = {"State_ID": state_id}
    #query collections in the database where State_iD is equal to state_id
    state_query = c.find_one(query)
    #retrieve the list of dictionary (School_List)
    school_list = state_query['School_List']

    # go through the list of dictionary where suburb name are matching and increment counter
    for item in school_list:
        if item['Suburb'] == suburb_name:
            counter += 1
    return counter

# count number of schools based on postcode
def count_school_by_post_code(state_id, post_code):
    collection = "test-collection"
    c = mydb[collection]
    counter = 0
    query = {"State_ID": state_id}
    #query collections in the database where State_iD is equal to state_id
    post_code_query = c.find_one(query)
    school_list = post_code_query['School_List']

    # go through the list of dictionary where postcodes are matching and increment counter
    for item in school_list:
        if item['Postcode'] == post_code:
            counter += 1

    return counter

if __name__ == '__main__':
    # start application
    app.run(debug=True)
