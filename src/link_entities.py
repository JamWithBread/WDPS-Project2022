from elasticsearch import Elasticsearch
import urllib
import json

#Establish connection with elasticsearch cloud server
def create_es_client():
	ELASTIC_PASSWORD = "s2cLbL0FpX5a3wIx2QvVJEqZ"
	CLOUD_ID = "WDPS_G34:ZXVyb3BlLXdlc3Q0LmdjcC5lbGFzdGljLWNsb3VkLmNvbTo0NDMkNGEyMTJmNmFiMmNkNDFiMDhmZTQyYzJkM2Q3MGRiZDckN2NiOTMxNzExZTg4NDU1NmJiMzZkYTg3ZTlmZWFmNTY="
	client = Elasticsearch(cloud_id = CLOUD_ID, http_auth = ("elastic", ELASTIC_PASSWORD))
	return client

client = create_es_client()

def get_client_info():
	return client.info()


def get_entity_candidates(entity,verbose):
	# entity input should be a dict with at minumum the following keys: "name", "label"
	# name will be used to query wikidata dump -> returns candidate matches
	
	try:
		response = client.search(
			size=10, 
			index="wikidata_1", 
			request_cache=True, 
			body={"query": {"query_string": {"query": entity['name'],}}})

		candidates = []

		for hit in response['hits']['hits']:
			candidate = {"id":hit["_id"], "name": hit['_source']['column2'] , "wikidata_link": hit['_source']['column3']}
			candidates.append(candidate)

		return candidates

	except Exception as e:
		if verbose:
			print(f"Error in elastic search run:\n {e}")
		return []


def get_all_candidates(entities,verbose):
	#To return a new dictionary containing all identified entities and their potential candidates
    entities_and_candidates = {} 
    j = 0
    for entity in entities:
        candidates = get_entity_candidates(entity,verbose) # Get candidates for current entity
        if candidates == []:
            continue #if no candidate matches found in wikidata, discard this entity
        entities_and_candidates[j] = {'name': entity['name'], 
                                    'ent_info': [entity['label'], entity['start'], entity['end']], 
                                    'candidates':candidates}
        j+=1

    return entities_and_candidates


def get_wikipedia_link(wikidata_id,verbose):
	#Takes a wikidata id (qid) as input and finds correspoding wikipedia title on wikidata.org. 
	# Combines title with generic wikepida url to return wikipedia link
	_id = wikidata_id
	url = None
	with urllib.request.urlopen("https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&props=sitelinks&ids="+_id+"&sitefilter=enwiki") as url:
		try:
			data = json.load(url)
			title = data["entities"][_id]["sitelinks"]["enwiki"]["title"]
			title = title.replace(" ", "_")
			wiki_url = "https://en.wikipedia.org/wiki/{}".format(title)

		except Exception as e:
			if verbose:
				print(f"Failed to load json data for QID: {_id}, reason: \n {e}")
			return None

	return wiki_url


def add_wikipedia_links(entities_and_matches,verbose):
	# Adds wikipedia link key value pair for all (top ranked) candidates in entities_and_matches dictionary
	seen_links = [] #If a link has been seen already, it is a duplicate. Discard duplicates
	remove_entries = [] #remove entities that fail to acquire wikilink
	for key in entities_and_matches.keys():
		try:
			wikidata_url = entities_and_matches[key]['match']['wikidata_link']
			wikidata_id =  wikidata_url.rsplit('/', 1)[1].strip(">")
			wikipedia_url = get_wikipedia_link(wikidata_id,verbose)
			if not wikipedia_url: #if url not found for entity, discard it
				remove_entries.append(key)
				continue
			if wikipedia_url not in seen_links: #Prevent storing duplicate entities
				entities_and_matches[key]['match']['wikipedia_link'] = wikipedia_url
				seen_links.append(wikipedia_url)
			else:
				remove_entries.append(key)
		
		except Exception as e:
			remove_entries.append(key)
			if verbose:
				print(f"Error in add_wikipedia_links. reason: {e}\n")

	for key in remove_entries:
		del entities_and_matches[key]

	return entities_and_matches


