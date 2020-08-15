import json

with open("json_dump/r1.json") as f:
	d1 = json.load(f)

with open("json_dump/r2.json") as f:
	d2 = json.load(f)


def get_list(*d):

	col_list = list()
	
	d_floors = list()
	d_floors_dict = dict()
	
	for item in d:
		d_list = item["Workflow"]["ActivityGroups"][0]["GroupActivities"][0]["Floorplans"]
		#print(item)
		for m_dict in d_list:
			#return(m_dict)
			if m_dict["MarketingId"] not in d_floors:
				d_floors.append(m_dict["MarketingId"])
				d_floors_dict[m_dict["MarketingId"]] = {"Name":m_dict["Name"], "Bedrooms": m_dict["Bedrooms"], "Bathrooms": m_dict["Bathrooms"],
														"Image2dUrl": m_dict["Image2dUrl"], "Squarefeet": m_dict["Squarefeet"]}

	return(d_floors_dict)

def get_diff(*args, **kwargs):
	for arg in args:
		print(len(arg))
		#return(len(arg))

print(get_list(d1))
dump_dict = get_list(d1)
with open ('json_dump/apartment_list_dict.json', 'w') as f:
	json.dump(dump_dict, f)
	

#print(get_list_len(d3))
#get_diff(d1, d2, d3)
#print(d1)