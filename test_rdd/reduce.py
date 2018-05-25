#!/usr/bin/evn python

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('App')
sc = SparkContext(conf=conf)

rdd_list = sc.parallelize([1, 2, 3, 4, 5])
rdd_dict = sc.parallelize([('a', 1), ('b', 2), ('c', 3), ('a', 4), ('b', 5)])


def f(x, y):
    print('-', x, y)
    return 0, x[1] + y[1]


print(rdd_dict.reduce(f))
# fold和reduce功能一样，只是前者需要传入一个零值作为第一次和最后一次调用f时的第一个参数
print(rdd_dict.fold((0, 0), f))


def f2(x, y):
    print('-', x, y)
    return x + y


# 如果key是唯一的，则不调用f2
print(rdd_dict.reduceByKey(f2).glom().collect())
# reduceByKeyLocally和reduceByKey功能一样，只是前者返回的是Dict类型，后者是RDD类型
print(rdd_dict.reduceByKeyLocally(f2))
