from elasticsearch import Elasticsearch
import urllib
import json


ELASTIC_PASSWORD = "Y2bIljYOMsXUi17a52UUECAT"
CLOUD_ID = "WDPS_Group34_2022:ZXVyb3BlLXdlc3Q0LmdjcC5lbGFzdGljLWNsb3VkLmNvbTo0NDMkYzg0MGMzZmNlMDM2NDZjMjliNzI2ZWM3NDA4NjdhZGMkYTQyNjc3NjNmYWIyNDdiYmJkMGMxNzkwOGNmZjgxMGM="
client = Elasticsearch(cloud_id = CLOUD_ID, basic_auth = ("elastic", ELASTIC_PASSWORD))
print(client.info())

def get_entity_candidates(entity):
	# entity input should be a dict with at minumum the keys "name" and "label"
	# name will be used to query wikidata dump -> returns candidate matches
	
	try:
		response = client.search(
			size=15, 
			index="wikidata_1", 
			request_cache=True, 
			body={"query": {"query_string": {"query": entity['name'],}}})

		candidates = []

		for hit in response['hits']['hits']:
			candidate = {"id":hit["_id"], "name": hit['_source']['column2'] , "wikidata_link": hit['_source']['column3']}
			candidates.append(candidate)

		return candidates

	except Exception as e:
		print(f"Error in elastic search run:\n {e}")
		return []


def get_all_candidates(entities):
    entities_and_candidates = {} 
    j = 0
    for entity in entities:
        candidates = get_entity_candidates(entity) # Get candidates for current entity
        if candidates == []:
            continue #if no candidate matches found in wikidata, discard this entity
        entities_and_candidates[j] = {'name': entity['name'], 
                                    'ent_info': [entity['label'], entity['start'], entity['end']], 
                                    'candidates':candidates}
        j+=1

    return entities_and_candidates


def get_wikipedia_link(wikidata_id):
	_id = wikidata_id
	url = None
	with urllib.request.urlopen("https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&props=sitelinks&ids="+_id+"&sitefilter=enwiki") as url:
		try:
			data = json.load(url)
			#print(f"json data: {data}\n")
			title = data["entities"][_id]["sitelinks"]["enwiki"]["title"]
			title = title.replace(" ", "_")
			wiki_url = "https://en.wikipedia.org/wiki/{}".format(title)

		except Exception as e:
			print(f"Failed to load json data for QID: {_id}, reason: \n {e}")
			return None

	return wiki_url


def add_wikipedia_links(entities_and_matches):
	seen_links = []
	remove_entries = [] #remove entities that fail to acquire wikilink
	for key in entities_and_matches.keys():
		try:
			wikidata_url = entities_and_matches[key]['match']['wikidata_link']
			wikidata_id =  wikidata_url.rsplit('/', 1)[1].strip(">")
			wikipedia_url = get_wikipedia_link(wikidata_id)
			if not wikipedia_url: #if url not found for entity, discard it
				remove_entries.append(key)
				continue
			if wikipedia_url not in seen_links: #Prevent storing duplicate entities
				entities_and_matches[key]['match']['wikipedia_link'] = wikipedia_url
				seen_links.append(wikipedia_url)
			else:
				remove_entries.append(key)
			#print(f"Entity: {entities_and_matches[key]['name']}, Acquired wikipedia_link: {wikipedia_url}\n")
		except Exception as e:
			remove_entries.append(key)
			print(f"Error in add_wikipedia_links. reason: {e}\n")

	for key in remove_entries:
		del entities_and_matches[key]

	return entities_and_matches


# Hit Example:
"""{'_index': 'wikidata_1', 
	'_id': '3HG254QBt1nlyh_STzi8', 
	'_score': 7.8189745, 
	'_source': {'column1': 121792593, 
				'column3': '<http://www.wikidata.org/entity/Q90>', 
				'column2': 'Paris, France'}} """

