# python3 starter_code.py data/warcs/sample.warc.gz
import sys
import gzip
sys.path.append("/app/assignment/assignment-code/src")

from html_to_text_prod import warc_html_killer
from spacy_prod import spacy_extract_entities
from wikimapper import WikiMapper
from link_entities import get_entity_candidates

KEYNAME = "WARC-TREC-ID"

# The goal of this function process the webpage and returns a list of labels -> entity ID
def find_entities(payload,i):
    if payload == '':
        return

    # The variable payload contains the source code of a webpage and some
    # additional meta-data.  We first retrieve the ID of the webpage, which is
    # indicated in a line that starts with KEYNAME.  The ID is contained in the
    # variable 'key'

    key = None
    for line in payload.splitlines():
        if line.startswith(KEYNAME):
            key = line.split(': ')[1]
            break

    # Problem 1: The webpage is typically encoded in HTML format.
    # We should get rid of the HTML tags and retrieve the text. How can we do it?
    text = warc_html_killer(payload)
    
    # Problem 2: Let's assume that we found a way to retrieve the text from a
    # webpage. How can we recognize the entities in the text?

    ents = spacy_extract_entities(text)

    # Problem 3: We now have to disambiguate the entities in the text. For
    # instance, let's assume that we identified the entity "Michael Jordan".
    # Which entity in Wikidata is the one that is referred to in the text?

    # For each entity, get candidate entities from wikidata, rank and select top match
    #print(ents)
    entities_and_candidates = {} #{i:{'name':None, "ent_info":None, 'candidates':[]}}
    i = 0
    for entity in ents:
        entities_and_candidates[i] = {'name': entity['name'], 
                                    'ent_info': [entity['label'], entity['start'], entity['end']], 
                                    'candidates':None}
        candidates = get_entity_candidates(entity)
        if candidates == []:
            print(f"\n\n entity: {entity} - NO CANDIDATE MATCHES \n\n")
        entities_and_candidates[i]['candidates'] = candidates
        i+=1

    for key in entities_and_candidates.keys():
        print(f"entity # {key}\n")
        print(entities_and_candidates[key],"\n\n")


    #Only keep entities that could be found in wikimedia, get wikipedia links for these entities

    # mapper = WikiMapper("index_enwiki-latest.db")
    # for key in entities.keys():
    #     if entities[key][3] == None:
    #         del entities[key]
    #     else: # Map wikidata id to wikipedia page title. Append wikipedia link to dictionary
    #         qid = entities[key][2]
    #         title = mapper.id_to_titles(qid)[0]
    #         wikipedia_link = "https://en.wikipedia.org/wiki/" + title
    #         entities[key].append(wikipedia_link)

    # if i < 5:
    #     for key in entities.keys():
    #         print(f"Entitiy: {entitites[key]},\nwikipedia link: {entities[key][-1]}")


    # A simple implementation would be to create a dictionary with all the
    # labels of the entities in Wikipedia. You may want to contact also some
    # external KBs (like Wikidata) to get some extra knowledge, or find a way
    # to leverage the context in the webpage. For instance, if you know that the
    # webpage refers to persons then you can query the knowledge base to filter
    # out all the entities that are not persons...

    # Obviously, more sophisticated implementations that the one suggested
    # above are more than welcome :-)

    # For now, we are cheating. We are going to return the labels that we stored
    # in sample-labels-cheat.txt Instead of doing that, you should process the
    # text to identify the entities. Your implementation should return the
    # discovered disambiguated entities with the same format so that I can
    # check the performance of your program.

    cheats = dict((line.split('\t', 2) for line in open('data/sample-entities-cheat.txt').read().splitlines()))
    for label, wikipedia_id in cheats.items():
        if key and (label in payload):
            yield key, label, wikipedia_id


# The goal of this function is to find relations between the entities
def find_relations(payload, entities):
    if payload == '':
        return

    key = None
    for line in payload.splitlines():
        if line.startswith(KEYNAME):
            key = line.split(': ')[1]
            break

    # A simple solution would be to extract the text between two previously
    # extracted entitites, and then determine if it is a valid relation

    # Optionally, we can try to determine whether the relation mentioned in the
    # text refers to a known relation in Wikidata.

    # Similarly as before, now we are cheating by reading a set of relations
    # from a file. Clearly, this will report the same set of relations for each page
    tokens = [line.split('\t') for line in open('data/sample-relations-cheat.txt').read().splitlines()]
    for label, subject_wikipedia_id, object_wikipedia_id, wikidata_rel_id in tokens:
        if key:
            yield key, subject_wikipedia_id, object_wikipedia_id, label, wikidata_rel_id


def split_records(stream):
    payload = ''
    for line in stream:
        if line.strip() == "WARC/1.0":
            yield payload
            payload = ''
        else:
            payload += line
    yield payload

if __name__ == '__main__':
    try:
        _, INPUT = sys.argv
    except Exception as e:
        print('Usage: python3 starter-code.py INPUT')
        sys.exit(0)

    with gzip.open(INPUT, 'rt', errors='ignore') as fo:
        i = 0 #For debugging / tracking what document # we're at in the warc file
        for record in split_records(fo):
            print(f"i: {i}\n")
            i+=1
            entities = find_entities(record,i)
            
            for key, label, wikipedia_id in entities:
                print("ENTITY: " + key + '\t' + label + '\t' + wikipedia_id)
            relations = find_relations(record, entities)
            for key, s, o, label, wikidata_id in relations:
                print("RELATION: " + key + '\t' + s + '\t' + o + '\t' + label + '\t' + wikidata_id)

