#!/usr/bin/env python

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('App')
sc = SparkContext(conf=conf)

rdd_list = sc.parallelize([1, 2, 3, 4, 5], 3)
rdd_dict = sc.parallelize([('a', 1), ('b', 2), ('c', 3), ('a', 4), ('b', 5)], 3)


def f(v1, v2):
    print('-', type(v1), type(v2), v1, v2)
    return v1 + v2


# fold和reduce功能一样，只是fold需要传入一个零值
print(rdd_list.fold(0, f))


def f2(e1, e2):
    print('-', type(e1), type(e2), e1, e2)
    return e1[0] + e2[0], e1[1] + e2[1]


# fold和reduce功能一样，只是fold需要传入一个零值
print(rdd_dict.fold(('', 0), f2))


def f3(v1, v2):
    print('-', type(v1), type(v2), v1, v2)
    return v1 + v2


# 和reduceByKey功能一样，只是foldByKey需要传入一个零值
print(rdd_dict.foldByKey(0, f3).collect())
