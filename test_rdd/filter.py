#!/usr/bin/evn python

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('App')
sc = SparkContext(conf=conf)

rdd_list = sc.parallelize([1, 2, 3, 4, 5])
rdd_dict = sc.parallelize([('a', 1), ('b', 2), ('c', 3), ('a', 4), ('b', 5)])

# rdd元素的类型不一样，调用filter时需要对f函数的参数进行针对性地处理，
# 如下面一个是x，一个时x[1]
print(rdd_list.filter(lambda x: x > 3).collect())
print(rdd_dict.filter(lambda x: x[1] > 3).collect())
