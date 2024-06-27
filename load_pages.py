from porterStemmer import PorterStemmer

porter=PorterStemmer()

def hashmap_pagerank():
	hmap_pr = {}

	f = open('part-00001', 'r')

	for line in f:
		row = line.split('\t')
		hmap_pr[row[0]] = row[1].rstrip('\n')
	print("PAGE RANK DICT:", hmap_pr)
	return hmap_pr

def hashmap():
	words = {}

	f = open('000000_0', 'r')

	for line in f:
		row = line.split('\t')
		if len(row) > 1:
			words.setdefault(row[0], []).append(row[1][:-1])
	print("WORDS: ", words)
	return words

inverted_index = hashmap()	
	
def search_query(value):

	words = []

	for word in value:
		words.append(word)
		word =	porter.stem(word, 0, len(word)-1)

	if len(value):		
		result = []
		i = 0
		for word in value:

			if word in inverted_index:
				result.append([inverted_index[word], words[i]])
				i += 1

		return result

	return []
