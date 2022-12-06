from elasticsearch import Elasticsearch

ELASTIC_PASSWORD = "YqPxlMdOLUdyOBd7vAiYt0T4"
CLOUD_ID = "WDPS2022_Group34:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ4OWJhNGUzYjA4ZDQ0Y2YwYmQyZmQ1YjZiNDdiMjdlNSQxOTYzZTJiNmNmZTY0NjBlYTFlOWVmYTkzNDRmYjYyMQ=="
client = Elasticsearch(cloud_id = CLOUD_ID, basic_auth = ("elastic", ELASTIC_PASSWORD))
print(client.info())
def get_entity_candidates(entity):
	# entity input should be a dict with 4 keys (name, label, start, end)
	
	try:
		response = client.search(
			size=15, 
			index="wikidata_1", 
			request_cache=True, 
			body={"query": {"query_string": {"query": entity['name'],}}})

		candidates = []

		for hit in response['hits']['hits']:
			#DEBUGGING
			#print(f"\n\n hit: \n{hit}\n")
			#break

			candidate = {"id":hit["_id"], "name": hit['_source']['column2'] , "wikidata_link": hit['_source']['column3']}
			candidates.append(candidate)

		return candidates

	except Exception as e:
		print(f"Error in elastic search run:\n {e}")
		return []

# Hit Example:
"""{'_index': 'wikidata_1', 
	'_id': '3HG254QBt1nlyh_STzi8', 
	'_score': 7.8189745, 
	'_source': {'column1': 121792593, 
				'column3': '<http://www.wikidata.org/entity/Q90>', 
				'column2': 'Paris, France'}} """








#def rank_candidates()