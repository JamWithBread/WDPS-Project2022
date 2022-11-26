import sys
import gzip
sys.path.append("/app/assignment/assignment-code/src")

from html_to_text_prod import warc_html_killer
from spacy_prod import spacy_extract_entities

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
    entities = pacy_extract_entities(text)
    for key in entities.keys():
        print(f"Entity: {key},\n    Idxs and label: {entities[key]}")
    # Problem 3: We now have to disambiguate the entities in the text. For
    # instance, let's assume that we identified the entity "Michael Jordan".
    # Which entity in Wikidata is the one that is referred to in the text?

    # A simple implementation would be to create a dictionary with all the
    # labels of the entities in Wikipedia. You may want to contact also some
    # external KBs (like Wikidata) to get some extra knowledge, or find a way
    # to leverage the context in the webpage For instance, if you know that the
    # webpage refers to persons then you can query the knowledge base to filter
    # out all the entities that are not persons...

    # Obviously, more sophisticated implementations that the one suggested
    # above are more than welcome :-)

    # For now, we are cheating. We are going to return the labels that we stored
    # in sample-labels-cheat.txt Instead of doing that, you should process the
    # text to identify the entities. Your implementation should return the
    # discovered disambiguated entities with the same format so that I can
    # check the performance of your program.

    #DEBUGGING
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
        i = 0
        for record in split_records(fo):
            if i < 3:
                print(f"i: {i}\n")
                entities = find_entities(record,i)
                i+=1
            #DEBUGGING
                for key, label, wikipedia_id in entities:
                    print("ENTITY: " + key + '\t' + label + '\t' + wikipedia_id)
                relations = find_relations(record, entities)
                for key, s, o, label, wikidata_id in relations:
                    print("RELATION: " + key + '\t' + s + '\t' + o + '\t' + label + '\t' + wikidata_id)

