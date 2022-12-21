import spacy
import itertools
import nltk
import textacy
import numpy as np
nltk.download('punkt', quiet = True)

# From a list of matches retrieve the longest matches.
def string_set(string_list):
    return set(i for i in string_list 
               if not any(i in s for s in string_list if i != s))


# The goal of this function is to find relations between the entities
def find_relations(payload, ent_dict, key):
    relations = []
    if payload == '':
        return
    
    properties = open('src/properties.json')
    entities = ent_dict.keys()
    # A simple solution would be to extract the text between two previously
    # extracted entitites, and then determine if it is a valid relation
    nlp = spacy.load('en_core_web_sm')
    sentences = nltk.sent_tokenize(payload)

    # V | VP | VW*P
    # V = verb particle? adv?
    # W = (noun | adj | adv | pron | det)
    # P = (prep(adp) | particle | inf. marker)
    patterns = [[{'POS': 'VERB'}, {'POS': 'PART', 'OP': '?'}, {'POS': 'ADV', 'OP': '?'}], 
                [{'POS': 'VERB'}, {'POS': 'PART', 'OP': '?'}, {'POS': 'ADV', 'OP': '?'}, {'POS': 'ADP'}],
                [{'POS': 'VERB'}, {'POS': 'PART', 'OP': '?'}, {'POS': 'ADV', 'OP': '?'}, {'POS': 'PART'}],
                [{'POS': 'AUX'}, {'POS': 'PART', 'OP': '?'}, {'POS': 'ADV', 'OP': '?'}],
                [{'POS': 'AUX'}, {'POS': 'PART', 'OP': '?'}, {'POS': 'ADV', 'OP': '?'}, {'POS': 'ADP'}],
                [{'POS': 'AUX'}, {'POS': 'PART', 'OP': '?'}, {'POS': 'ADV', 'OP': '?'}, {'POS': 'PART'}]]

    v1 = [{'POS': 'VERB'}, {'POS': 'PART', 'OP': '?'}, {'POS': 'ADV', 'OP': '?'}]
    v2 = [{'POS': 'AUX'}, {'POS': 'PART', 'OP': '?'}, {'POS': 'ADV', 'OP': '?'}]
    w = [{'POS': 'NOUN', 'OP': '*'}, {'POS': 'ADJ', 'OP': '*'}, {'POS': 'ADV', 'OP': '*'}, {'POS': 'PRON', 'OP': '*'}, {'POS': 'DET', 'OP': '*'}]
    
    # Create an approximation of W* by repeating the options.
    for i in range(20):
        v1 = v1 + w
        v2 = v2 + w

    # Add VW*P patterns.
    patterns.append(v1 + [{'POS': 'ADP'}])
    patterns.append(v2 + [{'POS': 'ADP'}])
    patterns.append(v1 + [{'POS': 'PART'}])
    patterns.append(v2 + [{'POS': 'PART'}])

    # Look for relations in each sentence.
    for sentence in sentences:
        k = [w for w in entities if w in sentence]

        # There must be two or more entities in the sentence.
        if len(k) >= 2:
            relation = []
            doc = nlp(sentence)

            # match of type AUX VERB ADP
            matches = textacy.extract.matches.token_matches(doc, patterns)
            ent_indices = np.array([sentence.find(w) for w in k])
            

            match_strings = []
            starts = []
            ends = []
            for i in matches:
                starts.append(i.start)
                ends.append(i.end)
                match_strings.append(i.text)

            # If matches are next to eachother combine them.
            for i, end in enumerate(ends):
                if end in starts:
                    indices = [i for i, x in enumerate(starts) if x == end]
                    for j in indices:
                        match_strings.append(match_strings[i] + " " + match_strings[j])

            # Get the longest different matches.
            longest_matches = string_set(list(match_strings))

            # For each match find the closest entities.
            for match in longest_matches:
                match_loc = sentence.find(match)
                relation = []
                relation.append(match)
                
                # Closest entity before match.
                val = max(ent_indices[ent_indices < match_loc], default=-1)

                # If there are no entities before match.
                if val == -1:
                    continue
                else:
                    # Closest entity before and after match.
                    i, = np.where(ent_indices == val)
                    relation.append(k[i[0]])
                    val = min(ent_indices[ent_indices > match_loc], default=-1)
                    if val == -1:
                        break
                    i, = np.where(ent_indices == val)
                    relation.append(k[i[0]])

                # If there is a complete relation save it.
                if len(relation) == 3:
                    relations.append(relation)

    for label, subject_wikipedia_id, object_wikipedia_id in relations:
        yield key, ent_dict[subject_wikipedia_id], ent_dict[object_wikipedia_id], label