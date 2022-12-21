import re, math
import nltk
nltk.download('wordnet',quiet=True)
nltk.download('stopwords',quiet=True)
nltk.download('omw-1.4',quiet=True)
from collections import Counter
from nltk.corpus import stopwords
from nltk.stem.porter import *
from nltk.corpus import wordnet as wn

stop = stopwords.words('english')

WORD = re.compile(r'\w+')
stemmer = PorterStemmer()


def get_cosine(vec1, vec2):
    intersection = set(vec1.keys()) & set(vec2.keys())
    numerator = sum([vec1[x] * vec2[x] for x in intersection])

    sum1 = sum([vec1[x]**2 for x in vec1.keys()])
    sum2 = sum([vec2[x]**2 for x in vec2.keys()])
    denominator = math.sqrt(sum1) * math.sqrt(sum2)

    if not denominator:
        return 0.0
    else:
        return float(numerator) / denominator

def text_to_vector(text):
    words = WORD.findall(text)
    a = []
    for i in words:
        for ss in wn.synsets(i):
            a.extend(ss.lemma_names())
    for i in words:
        if i not in a:
            a.append(i)
    a = set(a)
    w = [stemmer.stem(i) for i in a if i not in stop]
    return Counter(w)

def get_similarity(a, b):
    a = text_to_vector(a.strip().lower())
    b = text_to_vector(b.strip().lower())

    return get_cosine(a, b)

def get_top_candidates(cand_dict):
    # This function should be passed candidates_and_entities dict from starter_code.py
    # Returns entity and most likely cadidate match (highest scoring match)
    entities_and_match = {}
    for key in cand_dict:
        top_score = {'candidate': {}, 'score': 0}
        entity = cand_dict[key]["name"]
        for match in cand_dict[key]["candidates"]:
            candidate = match["name"]
            sim_score = get_similarity(entity,candidate)
            #print(f"Similarity score: {sim_score}, entity: {entity}, cand: {candidate}\n")
            if sim_score > top_score['score']:
                top_score['candidate'] = match
                top_score['score'] = sim_score

        if top_score['score'] < 0.3: #if similarity score below 0.3 threshold, entity is considered unlinkable
            continue

        top_cand = top_score['candidate']

        entities_and_match[key] = {'name': entity, 
                                   'ent_info' : cand_dict[key]['ent_info'],
                                   'match': top_score['candidate']
                                    }
    #print(f"\n\nentities_and_match:\n {entities_and_match}\n")                                
    return entities_and_match


