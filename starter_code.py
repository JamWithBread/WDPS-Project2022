# python3 starter_code.py data/warcs/sample.warc.gz


##################################################################################
####### Before running, run 'bash requirements.txt' in container cmd line ########
##################################################################################

import sys, argparse
import gzip
import multiprocessing
sys.path.append("src")

from html_to_text_prod import warc_html_killer
from spacy_prod import spacy_extract_entities
from link_entities import get_all_candidates, add_wikipedia_links, get_client_info
from rank_candidates import get_top_candidates
from relation_extraction import find_relations
import time

KEYNAME = "WARC-TREC-ID"
URLNAME = "WARC-Target-URI"

# The goal of this function process the webpage and returns a list of labels -> entity ID
def find_entities(key,text,i):
    t00 = time.time()
    if text == '':
        return

    # The variable payload contains the source code of a webpage and some
    # additional meta-data.  We first retrieve the ID of the webpage, which is
    # indicated in a line that starts with KEYNAME.  The ID is contained in the
    # variable 'key'

    # Problem 1: The webpage is typically encoded in HTML format.
    # We should get rid of the HTML tags and retrieve the text. How can we do it?
    

    #text = warc_html_killer(payload)
    
    # Problem 2: Let's assume that we found a way to retrieve the text from a
    # webpage. How can we recognize the entities in the text?
    ents = spacy_extract_entities(text)

    # Problem 3: We now have to disambiguate the entities in the text. For
    # instance, let's assume that we identified the entity "Michael Jordan".
    # Which entity in Wikidata is the one that is referred to in the text?

    # Problem 3.1: For each entity, get candidate entities from wikidata, rank and select top match
    entities_and_candidates = get_all_candidates(ents,verbose)

    #Problem 3.2: Rank and select top candidate for each entity
    matched_entities = get_top_candidates(entities_and_candidates)

    #problem 3.3: Get wikipedia link for selected candidate
    matched_entities = add_wikipedia_links(matched_entities,verbose)
    verboseprint(f"Find entities finished - Total time: {round(time.time()-t00,2)}s\n")

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

    if not matched_entities: #If document contained no text
        return

    for item in matched_entities.keys():
        entry = matched_entities[item]
        label = entry['name']
        wikipedia_id = entry['match']['wikipedia_link']
        yield key, label, wikipedia_id


    # cheats = dict((line.split('\t', 2) for line in open('data/sample-entities-cheat.txt').read().splitlines()))
    # for label, wikipedia_id in cheats.items():
    #     if key and (label in payload):
    #         print(f"ley: {key}, label: {label}, wikipedia_id: {wikipedia_id}\n")
    #         yield key, label, wikipedia_id



# # The goal of this function is to find relations between the entities
# def find_relations(payload, entities):
#     if payload == '':
#         return

#     key = None
#     for line in payload.splitlines():
#         if line.startswith(KEYNAME):
#             key = line.split(': ')[1]
#             break

#     # A simple solution would be to extract the text between two previously
#     # extracted entitites, and then determine if it is a valid relation

#     # Optionally, we can try to determine whether the relation mentioned in the
#     # text refers to a known relation in Wikidata.

#     # Similarly as before, now we are cheating by reading a set of relations
#     # from a file. Clearly, this will report the same set of relations for each page
#     tokens = [line.split('\t') for line in open('data/sample-relations-cheat.txt').read().splitlines()]
#     for label, subject_wikipedia_id, object_wikipedia_id, wikidata_rel_id in tokens:
#         if key:
#             yield key, subject_wikipedia_id, object_wikipedia_id, label, wikidata_rel_id

def process_record(tup):
    i, record = tup

    key = None
    web_url = None
    for line in record.splitlines():
        if line.startswith(KEYNAME):
            key = line.split(': ')[1]
        if line.startswith(URLNAME):
            web_url = line.split(': ')[1]
            break

    verboseprint(f"\n\nFinding entities for doc #{i}. web url: {web_url}")

    text = warc_html_killer(record)
    verboseprint(f"document # {i}: \n {text}\n\n\n")
    # text = "Amsterdam is the capital and largest city in the European country of the Netherlands. \
    #     Amsterdam is famous for its canals and dikes.\
    #      Unlike in capitals of most other countries, the national government, parliament, government ministries, supreme court, royal family and embassies are not in Amsterdam, but in The Hague.\
    #          Located in the Dutch province of North Holland, Amsterdam is colloquially referred to as the \"Venice of the North\".\
    #              The only diplomatic offices present in Amsterdam are consulates. The city hosts two universities (the University of Amsterdam and the Free University Amsterdam) and an international airport \"Schiphol Airport"

    
    entities = find_entities(key, text, i)
    

    relations = find_relations(text, list(entities))
    for _, label, wikipedia_id in entities:
        print("ENTITY: " + '\t' + label + '\t' + wikipedia_id)
    
    for key, s, o, label, wikidata_id in relations:
        print("RELATION: " + key + '\t' + s + '\t' + o + '\t' + label + '\t' + wikidata_id)


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
        parser = argparse.ArgumentParser(prog = "python3 starter_code.py")
        parser.add_argument('data', help = 'path to data file (warc)')
        parser.add_argument('--verbose','-v', help = "use '-v True' or '--verbose True' for more descriptive output and error msgs", default = False)
        args = parser.parse_args()
        INPUT = args.data
        verbose = args.verbose
        if verbose:
            def verboseprint(*args):
                for arg in args:
                    print(arg)
        else:   
            verboseprint = lambda *a: None 


    except Exception as e:
        print(f'Usage: python3 starter-code.py INPUT. error: {e}')
        sys.exit(0)

    #Create Connection to Elasticsearch cloud service
    verboseprint(f"\nElasticsearch client info:\n",get_client_info())

    multi = False
    open("test.txt", "w")
    with gzip.open(INPUT, 'rt', errors='ignore') as fo:   
        if multi:
            pool = multiprocessing.Pool() 
            pool.imap(process_record, enumerate(split_records(fo)))
        else: 
            for i, record in enumerate(split_records(fo)):
                #DEBUGGING
                if i == 100:
                    break
                print(f"i: {i}\n")
                process_record((i,record))


