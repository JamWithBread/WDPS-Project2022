#Sspacy.py

#before using in container:
#	python3 -m pip install spacy
#	python3 -m spacy download en_core_web_sm

import spacy

#returns a dict of entities, where they appear in the text and their type
#	example:
#	{"Robin Cannon": [5154, 5166, 'PERSON']}

def spacy_extract_entities(text):
	nlp = spacy.load("en_core_web_sm")
	doc = nlp(text)
	entities = {}

	for ent in doc1.ents:
		if ent.label_ not in ["CARDINAL", "DATE", "QUANTITY", "TIME", "ORDINAL", "MONEY", "PERCENT", "QUANTITY"]:
			entities[ent.text.replace('\t', ' ')] = [ent.start_char, ent.end_char, ent.label_]
			
	return entities

