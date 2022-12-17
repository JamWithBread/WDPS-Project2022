from spacy.matcher import Matcher 
import spacy
import itertools
import nltk

# The goal of this function is to find relations between the entities
def find_relations(payload, entities):
    if payload == '':
        return
    nltk.download('punkt')
    properties = open('src/properties.json')
    key = None
    # for line in payload.splitlines():
    #     if line.startswith(KEYNAME):
    #         key = line.split(': ')[1]
    #         break

    # A simple solution would be to extract the text between two previously
    # extracted entitites, and then determine if it is a valid relation
    nlp = spacy.load('en_core_web_sm')
    matcher = Matcher(nlp.vocab)
    sentences = nltk.sent_tokenize(payload)

    #define the pattern 
    pattern = [{'DEP':'ROOT'}, 
                    {'DEP':'nsubj','OP':"?"},
                    {'DEP':'agent','OP':"?"},  
                    {'POS':'dobj','OP':"?"}] 

    matcher.add("matching_1",[pattern])

    for sentence in sentences:
        k = [w for w in entities if w in sentence]
        if len(k) >= 2:
            doc = nlp(sentence)
            entity = doc.noun_chunks
            print(entity)
    
    # for subset in itertools.combinations(entities, 2):
    #     start = payload.find(subset[0][1])
    #     end = payload.find(subset[1][1], start)
        
    #     if start < end:
    #         substr = payload[start + len(subset[0][1]):end]
    #     else:
    #         substr = payload[end + len(subset[1][1]):start]
        
    #     if '.' not in substr and len(substr) != 0:
    #         print(substr)
            

    #         doc = nlp(substr)
    #         matches = matcher(doc)
    #         k = len(matches) - 1
    #         print(matches)
            
    #         span = doc[matches[k][1]:matches[k][2]]
    #         if len(span.text) != 0:
    #             print("Ent1"+subset[0][1])
    #             print("Rel"+span.text)
    #             print("Ent2"+subset[1][1]) 
        
    
    


    # Optionally, we can try to determine whether the relation mentioned in the
    # text refers to a known relation in Wikidata.

    # Similarly as before, now we are cheating by reading a set of relations
    # from a file. Clearly, this will report the same set of relations for each page
    tokens = [line.split('\t') for line in open('data/sample-relations-cheat.txt').read().splitlines()]
    for label, subject_wikipedia_id, object_wikipedia_id, wikidata_rel_id in tokens:
        if key:
            yield key, subject_wikipedia_id, object_wikipedia_id, label, wikidata_rel_id