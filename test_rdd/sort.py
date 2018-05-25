#!/usr/bin/env python

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('App')
sc = SparkContext(conf=conf)

rdd_list = sc.parallelize([1, 2, 3, 4, 5], 3)
rdd_dict = sc.parallelize([('a', 1), ('b', 2), ('c', 3), ('d', 4), ('e', 5)], 3)

# keyfunc为自定义的排序函数，比如下一例中按-x排序
print(rdd_list.sortBy(keyfunc=lambda x: -x, ascending=True).collect())
print(rdd_dict.sortByKey(keyfunc=lambda x: x, ascending=False).collect())
