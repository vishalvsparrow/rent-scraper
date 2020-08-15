import requests
import random
from datetime import date
from datetime import datetime
import time
import json
import pandas as pd
#import numpy as np
import pandas_gbq
from google.oauth2 import service_account
#from oauth2client.service_account import ServiceAccountCredentials

## request for app engine
# import requests_toolbelt.adapters.appengine
# requests_toolbelt.adapters.appengine.monkeypatch()

class callBrookfield:
	'''
	apartment_list = \
	['1873535', '1873533', 
	'1873532', '1873530', '1873531', 
	'1873534', '9631161', '9631159', '9631162', '9631160', 
	'1873545', '1873543', '1873539', '6207783', '1873544', '1873540', '5247743', 
	'1873537', '1873536', '1873541', '1873542', '1873546', '1873538', '9631163', '9631171', 
	'9631169', '9631166', '9631168', '9631167', '9631164', '9631165', '9631170', '1873560', 
	'1873553', '1873552', '1873558', '1873555', '1873559', '1873556', '1873557', '1873547', 
	'1873549', '1873548', '1873554', '1873550', '1873551', '9631179', '9631172', '9631174', 
	'9631177', '9631173', '9631175', '9631176', '9631180', '9631178', '9631181']
	'''
	

	headers = {
		#'authority': 'leasing.realpage.com', 89iiqc 
		'accept': 'application/json, text/plain, */*'
		#'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko)\
		#Chrome/50.0.2661.102 Safari/537.36',
		#'referrer': 'https://baysidevillage.com/lease-online/'
		}

	floor_params = {'BpmId':'OLL.Shopping.Search.Apartment', 'SiteId': '3209177', 'User-Agent': ' Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Chrome/50.0.2661.102 Safari/537.36',
						'PmcId': '1050775'}


	home_url = 'https://baysidevillage.com/lease-online/'
	floor_plan_url = 'https://leasing.realpage.com/RP.Leasing.AppService.WebHost/ApartmentList/v1'

	## read the API response from a file saved locally during development
	def get_apartment_dict(self):
		with open('json_dump/apartment_list_dict.json') as f:
			apartment_list_dict = json.load(f)
		return(apartment_list_dict)

	## make calls to the actual URL
	def call_floor_plans(self, m_params, floor_plan_dict_t, floor_plan_list, floor_plan_url = floor_plan_url, m_headers = headers):
		
		## empty dictionary
		temp_floor_dict = dict()

		for floor in floor_plan_list[0:10]:

			m_params['FloorplanId'] = floor
			floor_plan_dict_t = floor_plan_dict[floor]
			
			print("\n\nthis is the request -->", m_params)

			floor_resp = s.get(url = m_floor_plan_url , headers= m_headers, params = m_params)
			
			floor_resp = json.loads(floor_resp.content)

			print("\n\nresponse -->", floor_resp)
			temp_floor_dict[floor] = {"request_d": m_params["MoveInDate"], "Name": floor_plan_dict_t["Name"],
										 "Bedrooms": floor_plan_dict_t["Bedrooms"], "Bathrooms": floor_plan_dict_t["Bathrooms"],
										 "Image2dUrl": floor_plan_dict_t["Image2dUrl"], "Squarefeet": floor_plan_dict_t["Squarefeet"],
										 "Units": floor_resp["Units"]}
            
            ## sleep for a random duration of time, max. 15 seconds
			time.sleep(5+random.randrange(10))

		return(temp_floor_dict)

	## output the dictionary during testing
	def test_floor_dict(self, floor_plan_dict):
		for floor in list(floor_plan_dict.keys())[0:5]:
			print(floor)
			## append information here
			floor_plan_dict[floor]['FloorplanId'] = floor
			print("\n")
			
		print(floor_plan_dict.values())
		return None


class authenticateUser:
	
	def __init__(self):
		## enter the name of your Google Cloud project service account key
		self.gcloud_service_key_name = ''
		
		
	
	ascii_lowercase = 'abcdefghijklmnopqrstuvwxyz'
	digits = '0123456789'

	#xyz = 'TjNBNjFGNUMyQzNCRkU3QzdDNDBBNzAzMzhFOEZBNDFCVVhHNzJGNTMzRUY2NkQ0OTNCODk3RjNDRkM2NDNFMTU0NDhnRjFBek1UVTVOak16TlRFd05UWXpPUT09NWxZQ0RpTQ=='
	#xyz_2 = 'NDNBNjFGNUMyQzNCRkU3QzdDNDBBNzAzMzhFOEZBNDFCaW5oNzJGNTMzRUY2NkQ0OTNCODk3RjNDRkM2NDNFMTU0NDhjdnRKSU1UVTVOak16TURJM01qSTBOdz09RlV0MEprdQ=='

	## general authentication, not being used anywhere in this code
	def authenticate(self, url, headers, session):

		response = session.get(url, headers)

		return response

	## authorize with bigquery using the service account key. Returns auth token
	def bq_auth(self):
		credentials = service_account.Credentials.from_service_account_file(self.gcloud_service_key_name)
		return(credentials)




	## generate and return a random string to supply in the GET parameters
	def randomUser(self, ascii_lowercase = ascii_lowercase, digits = digits):

		p1 = ''.join(random.choice(ascii_lowercase+digits) for x in range(8))
		p2 = ''.join(random.choice(ascii_lowercase+digits) for x in range(4))
		p3 = ''.join(random.choice(ascii_lowercase+digits) for x in range(4))
		p4 = ''.join(random.choice(ascii_lowercase+digits) for x in range(4))
		p5 = ''.join(random.choice(ascii_lowercase+digits) for x in range(12))

		return p1 + '-' + p2 + '-' + p3 + '-' + p4 + '-' + p5

class process_data:

	def __init__(self):
		## supply the table name and project name
		self.bq_table_name = ''
		self.test_table = ''
		self.gcloud_project_name = ''
	
	## write the API response to a JSON file 
	def write_local_floor_response(self, floor_response):
		
		try:
			with open('json_dump/json_floor_resp_dump.json', 'w') as f:
				json.dump(floor_response, f, indent = 4)
			print("floor_response written successfully")
			return 1
		except:
			print("error writing floor_response")
			return 0

	## read from the JSON file
	def read_local_floor_response(self):
		
		with open('json_dump/json_floor_resp_dump.json') as f:
			self.local_floor_resp_dict = json.load(f)
		return(self.local_floor_resp_dict)
		#except:
		#	print("error reading floor_response")
		#	return 0

	## create a Pandas dataframe from the JSON file
	def create_dataframe(self, floor_resp_dict):

		
		#print(self.floor_resp_dict)

		## create a list to hold all rows
		#row_list = []

		## create an empty dataframe
		finalDataFrame = pd.DataFrame()

		## create a dictionary on a single level

		for key, value in floor_resp_dict.items():
			
			temp_floor_dict = dict()
			## get values from dictionary
			temp_floor_dict['FloorplanId'] = key
			temp_floor_dict['planSquareFeet'] = value['Squarefeet']
			temp_floor_dict['planBedrooms'] = value['Bedrooms']
			temp_floor_dict['planBathrooms'] = value['Bathrooms']
			temp_floor_dict['planName'] = value['Name']
			temp_floor_dict['planSquareFeet'] = value['Squarefeet']
			temp_floor_dict['request_d'] = value['request_d']
			temp_floor_dict['planImage'] = value['Image2dUrl']
			temp_floor_dict['planAvailableUnits'] = len(value['Units'])

			#print(key)
			#print(len(value['Units']))
			#print(value['Name'])
			#print(len(value['Units']))
			#print('\n')
			'''
			for d in value['Units']:
				try:
					print(d['UnitNumber'])
					print(datetime.fromtimestamp(float(d['AvailableDate'][6:19])/1000).strftime("%Y-%m-%d"))

				except Exception as e:
					raise e
			'''

			## if there are units available for a floor plan, add them incrementally to a data-frame
			
			if not len(value['Units']) == 0:

				for d in value['Units']:

					temp_floor_dict['unitId'] = d['Id']
					temp_floor_dict['unitNumber'] = d['UnitNumber']
					temp_floor_dict['unitBuildingNumber'] = d['BuildingNumber']
					temp_floor_dict['unitFloor'] = d['Floor']
					temp_floor_dict['unitMinPrice'] = d['MinPriceRange']
					temp_floor_dict['unitMaxPrice'] = d['MaxPriceRange']
					#temp_floor_dict['unitAvailableDate'] = datetime.fromtimestamp(float(d['AvailableDate'][6:19])/1000.).strftime("%Y-%m-%d")
					temp_floor_dict['unitAvailableDate'] = d['AvailableDate'][6:19]
					temp_floor_dict['unitDeposit'] = d['Deposit']
					temp_floor_dict['unitIsFeatured'] = d['IsFeatured']
					temp_floor_dict['unitHasConcession'] = d['HasConcession']

					#print(temp_floor_dict)

					finalDataFrame = finalDataFrame.append(temp_floor_dict, ignore_index = True)


			else:
				finalDataFrame = finalDataFrame.append(temp_floor_dict, ignore_index = True)
				#print(temp_floor_dict)

		#print(finalDataFrame)
		return(finalDataFrame)

	## write the dataframe to bigquery
	def write_bigquery(self, finaldataFrame, bq_cred):


		#pandas_gbq.to_gbq(finaldataFrame, self.test_table, self.gcloud_project_name, if_exists = 'append', credentials = bq_cred)
		finaldataFrame.to_gbq(self.bq_table_name, self.gcloud_project_name, if_exists = 'append', credentials = bq_cred)
		#finaldataFrame.to_gbq(self.test_table, self.gcloud_project_name, if_exists = 'replace', credentials = bq_cred)
		print("write success on", date.today().strftime("%m/%d/%Y"))

	 
## create a Session object
s = requests.Session()

auth = authenticateUser()
brook = callBrookfield()

m_floor_plan_url = brook.floor_plan_url

## read the floor plan dict from json
floor_plan_dict = brook.get_apartment_dict()

## create a list to randomly shuffle
floor_plan_list = list(floor_plan_dict.keys())
random.shuffle(floor_plan_list)

## add parameters
m_params = brook.floor_params
m_params['ClientSessionID'] = auth.randomUser()
m_params['MoveInDate'] = date.today().strftime("%m/%d/%Y")

#brook.test_floor_dict(floor_plan_dict)

#print(m_params)
#print(m_headers)
#print(m_floor_plan_url)

#home_response = s.get(url = home_url , headers= m_headers)

## call floor response

floor_response = brook.call_floor_plans(floor_plan_url=m_floor_plan_url, 
							 m_params=m_params, floor_plan_dict_t=floor_plan_dict, floor_plan_list=floor_plan_list)

print("\nFinal floor response-->", floor_response)

## Class instance for process_data
p = process_data()

## write floor response data to local
#p.write_local_floor_response(floor_response)

## read floor response data

#p.read_local_floor_response()

## create a local dataframe

#m_final_df = p.create_dataframe(p.local_floor_resp_dict)

## pass floor_response to create_dataframe

m_final_df = p.create_dataframe(floor_response)

print("dataframe columns", m_final_df.columns)

print("Authenticating with bigquery")
## Authenticate with bq
bq_cred = auth.bq_auth()

#m_final_df.to_csv('temp_csv.csv')
#dummy_df = pd.DataFrame()
#dummy_df = dummy_df.append({"name": "Vishal9", "age":26}, ignore_index=True)
#dummy_df['name'] = dummy_df['name'].astype('str')
#dummy_df['age'] = dummy_df['age'].astype('str')

#dummy_df.columns = np.char.decode(dummy_df.columns.values.astype(str), encoding='UTF-8', errors='ignore')

#print(dummy_df.columns.to_series().groupby(dummy_df.dtypes).groups)
#print(dummy_df)

#dummy_df['name'] = dummy_df['name'].map(lambda x: x.encode('unicode-escape'))
#print(pd.api.types.infer_dtype(dummy_df.columns, skipna = False))
print("writing to bigquery")
## Write to bigquery
#p.write_bigquery(dummy_df, bq_cred)
p.write_bigquery(m_final_df, bq_cred)


#json_dump = json.dumps(json_floor_array)

#with open('json_floor_resp_dump.json', 'w', encoding = 'utf-8') as f:
#	json.dump(json_dump, f, ensure_ascii = False, indent = 4)

# note: a response with multiple units. Continue working on this
'''
{'Units': [{'Id': '488', 'UnitNumber': '407', 'BuildingId': None, 'BuildingNumber': '5', 'ImageUrl': None, 'ImageAltText': None, 'MinPriceRange': 2085, 'MaxPriceRange': 2330, 'AvailableDateString': 'Now', 'AvailableDate': '/Date(1588050000000-0500)/', 'HasConcession': False, 'Floor': 4, 'Deposit': 600, 'IsFeatured': False, 'SiteId': '3209177', 'Squarefeet': 460}, {'Id': '9', 'UnitNumber': '109', 'BuildingId': None, 'BuildingNumber': '1', 'ImageUrl': None, 'ImageAltText': None, 'MinPriceRange': 1905, 'MaxPriceRange': 2150, 'AvailableDateString': 'Aug 09', 'AvailableDate': '/Date(1596949200000-0500)/', 'HasConcession': False, 'Floor': 1, 'Deposit': 600, 'IsFeatured': False, 'SiteId': '3209177', 'Squarefeet': 460}, {'Id': '554', 'UnitNumber': '404', 'BuildingId': None, 'BuildingNumber': '6', 'ImageUrl': None, 'ImageAltText': None, 'MinPriceRange': 1980, 'MaxPriceRange': 2225, 'AvailableDateString': 'Aug 09', 'AvailableDate': '/Date(1596949200000-0500)/', 'HasConcession': False, 'Floor': 4, 'Deposit': 600, 'IsFeatured': False, 'SiteId': '3209177', 'Squarefeet': 460}, {'Id': '401', 'UnitNumber': '109', 'BuildingId': None, 'BuildingNumber': '5', 'ImageUrl': None, 'ImageAltText': None, 'MinPriceRange': 1915, 'MaxPriceRange': 2160, 'AvailableDateString': 'Aug 09', 'AvailableDate': '/Date(1596949200000-0500)/', 'HasConcession': False, 'Floor': 1, 'Deposit': 600, 'IsFeatured': False, 'SiteId': '3209177', 'Squarefeet': 460}, {'Id': '134', 'UnitNumber': '202', 'BuildingId': None, 'BuildingNumber': '2', 'ImageUrl': None, 'ImageAltText': None, 'MinPriceRange': 1940, 'MaxPriceRange': 2185, 'AvailableDateString': 'Aug 16', 'AvailableDate': '/Date(1597554000000-0500)/', 'HasConcession': False, 'Floor': 2, 'Deposit': 600, 'IsFeatured': False, 'SiteId': '3209177', 'Squarefeet': 460}, {'Id': '356', 'UnitNumber': '322', 'BuildingId': None, 'BuildingNumber': '4', 'ImageUrl': None, 'ImageAltText': None, 'MinPriceRange': 1950, 'MaxPriceRange': 2195, 'AvailableDateString': 'Aug 16', 'AvailableDate': '/Date(1597554000000-0500)/', 'HasConcession': False, 'Floor': 3, 'Deposit': 600, 'IsFeatured': False, 'SiteId': '3209177', 'Squarefeet': 460}, {'Id': '444', 'UnitNumber': '223', 'BuildingId': None, 'BuildingNumber': '5', 'ImageUrl': None, 'ImageAltText': None, 'MinPriceRange': 1885, 'MaxPriceRange': 2135, 'AvailableDateString': 'Aug 26', 'AvailableDate': '/Date(1598418000000-0500)/', 'HasConcession': False, 'Floor': 2, 'Deposit': 600, 'IsFeatured': False, 'SiteId': '3209177', 'Squarefeet': 460}, {'Id': '201', 'UnitNumber': '208', 'BuildingId': None, 'BuildingNumber': '3', 'ImageUrl': None, 'ImageAltText': None, 'MinPriceRange': 1970, 'MaxPriceRange': 2215, 'AvailableDateString': 'Aug 28', 'AvailableDate': '/Date(1598590800000-0500)/', 'HasConcession': False, 'Floor': 2, 'Deposit': 600, 'IsFeatured': False, 'SiteId': '3209177', 'Squarefeet': 460}, {'Id': '168', 'UnitNumber': '410', 'BuildingId': None, 'BuildingNumber': '2', 'ImageUrl': None, 'ImageAltText': None, 'MinPriceRange': 1985, 'MaxPriceRange': 2295, 'AvailableDateString': 'Sep 19', 'AvailableDate': '/Date(1600491600000-0500)/', 'HasConcession': False, 'Floor': 4, 'Deposit': 600, 'IsFeatured': False, 'SiteId': '3209177', 'Squarefeet': 460}, {'Id': '576', 'UnitNumber': '111', 'BuildingId': None, 'BuildingNumber': '7', 'ImageUrl': None, 'ImageAltText': None, 'MinPriceRange': 1905, 'MaxPriceRange': 2170, 'AvailableDateString': 'Oct 06', 'AvailableDate': '/Date(1601960400000-0500)/', 'HasConcession': False, 'Floor': 1, 'Deposit': 600, 'IsFeatured': False, 'SiteId': '3209177', 'Squarefeet': 460}, {'Id': '277', 'UnitNumber': '422', 'BuildingId': None, 'BuildingNumber': '3', 'ImageUrl': None, 'ImageAltText': None, 'MinPriceRange': 1985, 'MaxPriceRange': 2250, 'AvailableDateString': 'Oct 07', 'AvailableDate': '/Date(1602046800000-0500)/', 'HasConcession': False, 'Floor': 4, 'Deposit': 600, 'IsFeatured': False, 'SiteId': '3209177', 'Squarefeet': 460}, {'Id': '34', 'UnitNumber': '205', 'BuildingId': None, 'BuildingNumber': '1', 'ImageUrl': None, 'ImageAltText': None, 'MinPriceRange': 1950, 'MaxPriceRange': 2215, 'AvailableDateString': 'Oct 21', 'AvailableDate': '/Date(1603256400000-0500)/', 'HasConcession': False, 'Floor': 2, 'Deposit': 600, 'IsFeatured': False, 'SiteId': '3209177', 'Squarefeet': 460}, {'Id': '51', 'UnitNumber': '222', 'BuildingId': None, 'BuildingNumber': '1', 'ImageUrl': None, 'ImageAltText': None, 'MinPriceRange': 1930, 'MaxPriceRange': 2165, 'AvailableDateString': 'Oct 31', 'AvailableDate': '/Date(1604120400000-0500)/', 'HasConcession': False, 'Floor': 2, 'Deposit': 600, 'IsFeatured': False, 'SiteId': '3209177', 'Squarefeet': 460}], 'PmsId': None, 'ResponseStatus': {'ErrorCode': None, 'Message': None, 'StackTrace': None, 'Errors': None}, 'EventsResponse': [], 'PhasedToken': None, 'ErrorCategory': 'None', 'SiteId': None, 'PmcId': None, 'LogSequence': 0, 'BpmSequence': 0, 'BpmId': None, 'ClientSessionID': None, 'RequestStartUtc': '/Date(-62135596800000-0000)/'}
'''