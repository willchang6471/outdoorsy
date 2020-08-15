import os
import re


def dict_builder():
    files = os.listdir('dataset')
    files.sort(key=int)

    words_set = set()
    word_id_lookup = []

    for file in files:
        f = open("dataset/"+file, "r", encoding="utf-8", errors ='surrogateescape')

        for word in f.read().split(' '):
            text = re.search(r'\b(\w+)\b', word)
            if text:
                # words_docs.append((text.group(1), 1))
                words_set.add(text.group(1))

    for word_id, word in enumerate(words_set):
        word_id_lookup.append(str(word)+" "+str(word_id)+"\n")


    print(word_id_lookup)

    with open("output/word_id", "w") as output:
        for word_id in word_id_lookup:
            output.write(word_id)




