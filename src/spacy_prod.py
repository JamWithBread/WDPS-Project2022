import spacy

#returns a dict of entities, where they appear in the text and their type
#	example:
#	{"Robin Cannon": [5154, 5166, 'PERSON']}

def spacy_extract_entities(text):
	nlp_model = spacy.load("en_core_web_sm")

	doc = nlp_model(text)
	entities = []

	for ent in doc.ents:
		if ent.label_ not in ["CARDINAL", "DATE", "QUANTITY", "TIME", "ORDINAL", "MONEY", "PERCENT", "QUANTITY"]:
			entity = {"name": ent.text.replace('\t', ' '), 'label':ent.label_, 'start':ent.start_char, 'end':ent.end_char}
			entities.append(entity)
			
	return entities

