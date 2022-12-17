# python3 starter_code.py data/warcs/sample.warc.gz

## TODO: add unlinkable entity cutoff score --> returns 'NIL'
# TODO: There are some pages that are nearly identical to another, after processing one skip the rest ????
        #ie WARC-Target-URI: http://cheapcosthealthinsurance.com/2012/02/06/what-is-breast-the-cancer/
# TODO: Program is fairly slow, main bottleneck is elasticsearch candidate generation (queries)

import sys
import gzip
import multiprocessing
import tqdm



sys.path.append("src")

from html_to_text_prod import warc_html_killer
from spacy_prod import spacy_extract_entities
#from wikimapper import WikiMapper
from link_entities import get_all_candidates, add_wikipedia_links
from rank_candidates import get_top_candidates
from relation_extraction import find_relations
import time

KEYNAME = "WARC-TREC-ID"
URLNAME = "WARC-Target-URI"

# The goal of this function process the webpage and returns a list of labels -> entity ID
def find_entities(key, text,i):
    t00 = time.time()
    print(f"\n\nFinding entities for doc {i}/1466")
    if text == '':
        return

    # The variable payload contains the source code of a webpage and some
    # additional meta-data.  We first retrieve the ID of the webpage, which is
    # indicated in a line that starts with KEYNAME.  The ID is contained in the
    # variable 'key'

    

    
    # Problem 2: Let's assume that we found a way to retrieve the text from a
    # webpage. How can we recognize the entities in the text?
    ents = spacy_extract_entities(text)

    # Problem 3: We now have to disambiguate the entities in the text. For
    # instance, let's assume that we identified the entity "Michael Jordan".
    # Which entity in Wikidata is the one that is referred to in the text?

    # Problem 3.1: For each entity, get candidate entities from wikidata, rank and select top match
    entities_and_candidates = get_all_candidates(ents)

    # for entry in entities_and_candidates.keys():
    #     print(f"entity # {entry}\n")
    #     print(entities_and_candidates[entry],"\n\n")

    #Problem 3.2: Rank and select top candidate for each entity
    matched_entities = get_top_candidates(entities_and_candidates)

    #problem 3.3: Get wikipedia link for selected candidate
    t0 = time.time()
    matched_entities = add_wikipedia_links(matched_entities)
    print(f"Find entities finished - Total time: {round(time.time()-t00,2)}s\n")

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
    if not matched_entities:
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




def split_records(stream):
    payload = ''
    for line in stream:
        if line.strip() == "WARC/1.0":
            yield payload
            payload = ''
        else:
            payload += line
    yield payload


def process_record(tup):
    i, record = tup

    key = None
    for line in record.splitlines():
        if line.startswith(KEYNAME):
            key = line.split(': ')[1]
            break

    text = warc_html_killer(record)
    text = "Amsterdam is the capital and largest city in the European country of the Netherlands. Amsterdam is famous for its canals and dikes. Unlike in capitals of most other countries, the national government, parliament, government ministries, supreme court, royal family and embassies are not in Amsterdam, but in The Hague. Located in the Dutch province of North Holland, Amsterdam is colloquially referred to as the \"Venice of the North\". The only diplomatic offices present in Amsterdam are consulates. The city hosts two universities (the University of Amsterdam and the Free University Amsterdam) and an international airport \"Schiphol Airport"

    
    entities = find_entities(key, text, i)
    

    relations = find_relations(text, list(entities))
    for _, label, wikipedia_id in entities:
        print("ENTITY: " + '\t' + label + '\t' + wikipedia_id)
    
    for key, s, o, label, wikidata_id in relations:
        print("RELATION: " + key + '\t' + s + '\t' + o + '\t' + label + '\t' + wikidata_id)

if __name__ == '__main__':
    try:
        _, INPUT = sys.argv
    except Exception as e:
        print('Usage: python3 starter-code.py INPUT')
        sys.exit(0)
    multi = False

    with gzip.open(INPUT, 'rt', errors='ignore') as fo:   
        if multi:
            pool = multiprocessing.Pool() 
            tqdm.tqdm(pool.imap(process_record, enumerate(split_records(fo))))
        else: 
            for i, record in enumerate(split_records(fo)):
                #DEBUGGING
                if i == 1:
                    break
                print(f"i: {i}\n")
                process_record((i,record))
        
            

