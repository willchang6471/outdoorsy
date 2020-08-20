import re
import sys
sys.path.insert(0, '.')
from collections import defaultdict
from pyspark import SparkContext, SparkConf


def word_spliter(word):

    text = re.search(r'\b(\w+)\b', word)
    if text:
        return str(text.group(1))

def dict_builder():
    conf = SparkConf().setAppName("dict")
    sc = SparkContext(conf = conf)
    textFiles = sc.textFile("dataset/*")
    words_rdd = textFiles.flatMap(lambda line: line.split(' '))
    words_map = words_rdd.map(word_spliter).distinct().zipWithIndex()
    words_map.saveAsTextFile("output/spark_word_id")
    
    words_map=words_map.collect()
    word_id = {}
    for line in words_map:
        word_id[line[0]] = line[1]


    textFiles = sc.wholeTextFiles("dataset")

    words_doc = textFiles.flatMap(lambda fc: ((fc[0].split('/')[-1], word) for word in fc[1].split(' ')))
    words_doc = words_doc.map(lambda wd: (wd[0], word_spliter(wd[1])))

    words_reduce = words_doc.map(lambda wd: (wd[1], [wd[0]])).groupByKey()
    words_reduce = words_reduce.collect()

    inverted_set = defaultdict(set)
    inverted_list = defaultdict(list)
    for w, docs in words_reduce:
        for doc in docs:
            for d in doc:
                inverted_set[w].add(int(d))

    for w, index_set in inverted_set.items():
        for index in sorted(index_set):
            inverted_list[word_id[w]].append(index)

    inverted_list = sorted(inverted_list.items(), key=lambda x: x[0])
    print(inverted_list)

    # Write inverted_index to output
    inverted_index_lookup = []
    for id, doc_ids in inverted_list:
        inverted_index_lookup.append(str(id) + " " + ",".join(map(str, doc_ids)) + "\n")

    with open("output/spark_inverted_index", "w") as output:
        for inverted_index in inverted_index_lookup:
            output.write(inverted_index)



if __name__ == "__main__":
    dict_builder()
