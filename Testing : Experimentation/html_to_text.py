#html2text.py

#Testing html to text package
sample_path = "samples/sample_amazon_obama.txt"

import html2text

with open(sample_path, 'r') as file:
	sample = file.read()

sample = html2text.html2text(sample)

output = open("/Users/jerenolsen/Desktop/WDPS_local_testing/HTML_to_text_testing/output.txt", 'w')
output.write(sample)
#print(sample)