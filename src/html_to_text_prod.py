from bs4 import BeautifulSoup
import re

# before using in container:
#	python3 -m pip install bs4

KEYHTML= "<!DOCTYPE html"

def warc_html_killer(content):
	# Code Adapted from stackoverflow user: PeYoTIL
	# -> to be applied to a html content from single warc file entry 
	text = ""
	read = False
	for line in content.splitlines():
		if line.startswith(KEYHTML):
			read =  True
		if read == True:
			text += line

	soup = BeautifulSoup(text, features="html.parser")

	# kill all script and style elements
	for script in soup(["script", "style", "aside"]):
	    script.extract()    # rip it out

	# get text
	text = soup.get_text()
	# break into lines and remove leading and trailing space on each
	lines = (line.strip() for line in text.splitlines())
	# break multi-headlines into a line each
	chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
	# drop blank lines
	text = '\n'.join(chunk for chunk in chunks if chunk)

	#Remove any html characters that html.parser missed
	text = re.sub("[^\u4e00-\u9fa5^\s\.\!\:\-\@\#\$\(\)\_\,\;\?^a-z^A-Z^0-9]","",text)

	return text



