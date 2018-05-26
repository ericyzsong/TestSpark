#!/usr/bin/evn python

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('App')
sc = SparkContext(conf=conf)

rdd_dict = sc.parallelize([('a', 1), ('b', 2), ('c', 3), ('d', 4), ('e', 5), ('f', 1)])

# partitionBy只适用于元素为键值对的RDD，partitionFunc传入参数为元素的键
print(rdd_dict.partitionBy(2, lambda x: x > 'b').glom().collect())
