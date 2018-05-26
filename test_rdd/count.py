#!/usr/bin/evn python

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('App')
sc = SparkContext(conf=conf)

rdd_list = sc.parallelize([1, 2, 3, 4, 5, 5])
rdd_dict = sc.parallelize([('a', 1), ('b', 2), ('c', 3), ('a', 4), ('b', 5), ('a', 1)])

print(rdd_dict.count())
# 返回键和其对应的数量
print(rdd_dict.countByKey())
# 返回RDD中唯一的元素（注意是元素，不是键或者值）和其对应的数量
print(rdd_list.countByValue())
print(rdd_dict.countByValue())
