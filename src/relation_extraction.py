import spacy
import itertools
import nltk
import textacy
import numpy as np
nltk.download('punkt', quiet = True)

# The goal of this function is to find relations between the entities
def find_relations(payload, entities, key):
    relations = []
    if payload == '':
        return
    
    properties = open('src/properties.json')
    
    # A simple solution would be to extract the text between two previously
    # extracted entitites, and then determine if it is a valid relation
    nlp = spacy.load('en_core_web_sm')
    sentences = nltk.sent_tokenize(payload)

    #define the pattern 
    # X is/are (the) NOUN
    pattern_1 = [{'POS': 'PROPN'},
        {'POS': 'AUX'},
        {'POS': 'DET', 'OP': '*'},
        {'POS': 'NOUN'}]

    # in/of/to Y
    pattern_2 = [{'POS': 'ADP'},
           {'POS': 'DET', 'OP': '*'},
           {'POS': 'PROPN'}]
        
    # located in/ 
    pattern_3 = [{'POS': 'VERB'},
        {'POS': 'ADP'}]

    # Look for relations in each sentence.
    for sentence in sentences:
        k = [w for w in entities if w in sentence]

        # There must be two or more entities in the sentence.
        if len(k) >= 2:
            relation = []
            doc = nlp(sentence)

            # match of type AUX NOUN ADP
            matches_1 = textacy.extract.matches.token_matches(doc, pattern_1)  
            matches_2 = textacy.extract.matches.token_matches(doc, pattern_2)

            for match in matches_1:
                for ent in k:
                    if ent in match.text:
                        relation.append(match.text.replace(ent, ''))
                        relation.append(ent)
                        break
            
            for match in matches_2:
                for ent in k:
                    if ent in match.text:
                        relation.append(ent)
                        break

            if len(relation) == 3:
                relations.append(relation)

            # match of type AUX VERB ADP
            matches_3 = textacy.extract.matches.token_matches(doc, pattern_3)
            ent_indices = np.array([sentence.find(w) for w in k])

            for match in matches_3:
                match_loc = sentence.find(match.text)
                relation = []
                relation.append(match.text)
                
                # Closest entity before match.
                val = max(ent_indices[ent_indices < match_loc], default=-1)

                # If there are no entities before match
                if val == -1:
                    # Get two closest entities after match
                    val = min(ent_indices[ent_indices > match_loc], default=-1)
                    if val == -1:
                        break
                    i, = np.where(ent_indices == val)
                    val = min(ent_indices[ent_indices > val], default=-1)
                    if val == -1:
                        break
                    j, = np.where(ent_indices == val)
                    relation.append(k[j[0]])
                    relation.append(k[i[0]])
                else:
                    # Closest entity before and after match
                    i, = np.where(ent_indices == val)
                    relation.append(k[i[0]])
                    val = min(ent_indices[ent_indices > match_loc], default=-1)
                    if val == -1:
                        break
                    i, = np.where(ent_indices == val)
                    relation.append(k[i[0]])

            
                if len(relation) == 3:
                    relations.append(relation)

    
    # Optionally, we can try to determine whether the relation mentioned in the
    # text refers to a known relation in Wikidata.

    # Similarly as before, now we are cheating by reading a set of relations
    # from a file. Clearly, this will report the same set of relations for each page
    tokens = relations

    for label, subject_wikipedia_id, object_wikipedia_id in tokens:
        yield key, subject_wikipedia_id, object_wikipedia_id, label