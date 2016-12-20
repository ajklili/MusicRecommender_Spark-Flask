import sys
from operator import add
from pyspark import SparkContext
import unicodedata

def get_music_bubbles(ratings_filename):
    sc2 = SparkContext(appName="MusicBubble")
    lines = sc2.textFile(ratings_filename, 1)
    counts = line.flatMap(lambda x: x.split('')).map(
        lambda x: (x, 1)).reduceByKey(add)
    output = counts.collect()
    bubbles=[]
    for (word, count) in output:
        word_a = unicodedata.normalize('NFKD', word).encode('ascii', 'ignore')
        item = [word_a, count]
        bubbles.append(item,key=item[1])
    sc.stop()
    return bubbles