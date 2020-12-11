import sys
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":

    sc = SparkContext("local", "Words Count")

    words = sc.textFile("./sample.txt").flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda a, b: (a+b))

    for i in words.collect():
        print(i)

    words.coalesce(1).saveAsTextFile("./words.txt")