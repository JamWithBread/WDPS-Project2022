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
def find_entities(key, text, verbose):
    t00 = time.time()
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
    entities_and_candidates = get_all_candidates(ents,verbose)

    #Problem 3.2: Rank and select top candidate for each entity
    matched_entities = get_top_candidates(entities_and_candidates)

    #problem 3.3: Get wikipedia link for selected candidate
    matched_entities = add_wikipedia_links(matched_entities,verbose)
    if verbose:
        print(f"Find entities finished - Total time: {round(time.time()-t00,2)}s\n")


    if not matched_entities: #If document contained no text
        return

    for item in matched_entities.keys():
        entry = matched_entities[item]
        label = entry['name']
        wikipedia_id = entry['match']['wikipedia_link']
        yield key, label, wikipedia_id



def process_record(i, record, verbose):

    key = None
    key = "clueweb12-0000tw-00-00007"
    web_url = None
    for line in record.splitlines():
        if line.startswith(KEYNAME):
            key = line.split(': ')[1]
        if line.startswith(URLNAME):
            web_url = line.split(': ')[1]
            break
    
    if verbose:
        print(f"\n\nFinding entities for doc #{i}. web url: {web_url}")

    text = warc_html_killer(record)

    if verbose:
        print(f"document # {i}: \n {text}\n\n\n")
    # text = "Amsterdam is the capital and largest city in the European country of the Netherlands. \
    #     Amsterdam is famous for its canals and dikes.\
    #      Unlike in capitals of most other countries, the national government, parliament, government ministries, supreme court, royal family and embassies are not in Amsterdam, but in The Hague.\
    #          Located in the Dutch province of North Holland, Amsterdam is colloquially referred to as the \"Venice of the North\".\
    #              The only diplomatic offices present in Amsterdam are consulates. The city hosts two universities (the University of Amsterdam and the Free University Amsterdam) and an international airport \"Schiphol Airport"
    
    entities = find_entities(key, text, verbose)
    
    ent_list = []
    for key, label, wikipedia_id in entities:
        ent_list.append(label)
        print(key + '\t' + "ENTITY" + '\t' + label + '\t' + wikipedia_id)

    relations = find_relations(text, ent_list, key)
    
    for key, subject_wikipedia_id, object_wikipedia_id, label in relations:
        print(key + '\t' + "RELATION" + '\t' + label + '\t' + subject_wikipedia_id + '\t' + object_wikipedia_id )


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

    except Exception as e:
        print(f'Usage: python3 starter-code.py INPUT. error: {e}')
        sys.exit(0)

    #Create Connection to Elasticsearch cloud service
    if verbose:
        print(f"\nElasticsearch client info:\n",get_client_info())

    multi = True
    with gzip.open(INPUT, 'rt', errors='ignore') as fo: 
        records_enum = []
        for i, record in enumerate(split_records(fo)):
            records_enum.append([i, record, verbose])
       
        if multi:
            pool = multiprocessing.Pool() 
            pool.starmap(process_record, records_enum[:100])
        else: 
            for i, record in enumerate(split_records(fo)):
                #DEBUGGING
                if i == 3:
                    break
                print(f"i: {i}\n")
                process_record(i,record, verbose)

