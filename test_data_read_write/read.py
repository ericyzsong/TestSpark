#!/usr/bin/evn python

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('App')
sc = SparkContext(conf=conf)

# 每一行为RDD的一个元素
rdd = sc.textFile('/data/test.txt')
print(rdd.collect())

# 将文件夹中的文件读取进来，所有文件的按行分割组成RDD
rdd = sc.textFile('/data/test')
print(rdd.collect())
# 支持通配符
rdd = sc.textFile('/data/test/*.txt')
print(rdd.collect())

# 以文件路径为键，以文件内容为值（整个文本内容为值），组成RDD
rdd = sc.wholeTextFiles('/data/test')
print(rdd.collect())
