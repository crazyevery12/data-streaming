import sys
import findspark
findspark.init()

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    sc = SparkContext("local","Word count")

    words = sc.textFile("test.txt").flatMap(lambda line: line.split(" "))

    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    wordCounts.saveAsTextFile("C:/Users/ADMIN/Desktop/Big Data/data-stream/output")