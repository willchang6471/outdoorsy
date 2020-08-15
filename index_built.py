from collections import defaultdict
import os
import re


def index_builder():

    words_docs =[]
    files = os.listdir('dataset')
    files.sort(key=int)


    words_docs =[]
    inverted_index_set = defaultdict(set)
    inverted_index = defaultdict(list)
    inverted_index_lookup = []

    for file in files:
        f = open("dataset/" + file, "r", encoding="utf-8", errors='surrogateescape')

        for word in f.read().split(' '):
            text = re.search(r'\b(\w+)\b', word)
            if text:
                words_docs.append((text.group(1), file))

    word_id_dict = {}
    f2 = open("output/word_id", "r")

    for word in f2.read().split('\n'):
        token = word.split()
        if token:
            word_id_dict.update({token[0]:token[1]})

    for word, doc in words_docs:
        if word in word_id_dict.keys():
            inverted_index_set[word_id_dict[word]].add(int(doc))

    for id, index_set in inverted_index_set.items():
        for index in sorted(index_set):
            inverted_index[int(id)].append(index)

    inverted_index = sorted(inverted_index.items(), key = lambda x:x[0])
    print(inverted_index)

    # Write inverted_index to output
    for id, doc_ids in inverted_index:
        inverted_index_lookup.append(str(id)+" "+ ",".join(map(str, doc_ids))+"\n")

    with open("output/inverted_index", "w") as output:
        for inverted_index in inverted_index_lookup:
            output.write(inverted_index)








