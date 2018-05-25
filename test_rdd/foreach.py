#!/usr/bin/env python

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('App')
sc = SparkContext(conf=conf)

rdd_list = sc.parallelize([1, 2, 3, 4, 5], 3)
rdd_dict = sc.parallelize([('a', 1), ('b', 2), ('c', 3), ('a', 4), ('b', 5)], 3)


def f(e):
    print('-', type(e), e)


print(rdd_list.collect())
# foreach返回值为None，只是调用f对元素进行迭代处理，其处理结果不改变传入RDD的值
rdd_list.foreach(f)
print(rdd_list.collect())


def f2(iter):
    for e in iter:
        print('-', type(e), e)
    print('-----')


print(rdd_dict.collect())
# 按元素分组进行迭代处理
rdd_list.foreachPartition(f2)
print(rdd_dict.collect())

