#!/usr/bin/env python

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('App')
sc = SparkContext(conf=conf)

rdd_list = sc.parallelize([1, 2, 3, 4, 5], 3)
rdd_dict = sc.parallelize([('a', 1), ('b', 2), ('a', 3), ('b', 4), ('a', 5)], 3)

# map将定义的函数作用于RDD的每一个元素，具体操作由函数体定义
print(rdd_list.map(lambda x: x * 2).glom().collect())
print(rdd_dict.map(lambda x: (x[0] * 2, x[1] * 2)).glom().collect())


def f(x):
    print('*', x)
    return x[0] * 2, x[1] * 2


# flatMap要求函数返回结果为可迭代的对象，且其返回由迭代器的所有内容构成的新的RDD（非元组，map返回由元组构成的RDD）
print(rdd_list.flatMap(lambda x: (x * 2,)).glom().collect())
print(rdd_dict.flatMap(lambda x: (x[0] * 2, x[1] * 2)).glom().collect())
print(rdd_dict.flatMap(f).glom().collect())

# 只适用于元素为键值对的RDD，传入值为键值对的值
print(rdd_dict.mapValues(lambda x: x * x).glom().collect())
# flatMapValues的处理函数要返回可迭代对象
print(rdd_dict.flatMapValues(lambda x: (x * x,)).glom().collect())


def f2(x):
    l = []
    for xi in x:
        l.append(xi)
        print('-', xi)
    print('-----')
    return l


# mapPartitions按RDD的分组进行调用f，而非按元素调用f，
# f传入的参数为RDD中组的元素的一个迭代器，且f需要返回一个可迭代的对象
print(rdd_list.mapPartitions(f2).glom().collect())
print(rdd_dict.mapPartitions(f2).glom().collect())


def f3(idx, x):
    l = []
    for xi in x:
        l.append(xi)
        print('-', idx, xi)
    print('-----')
    return l


# 和mapPartitions函数功能一样，只不过f函数第一个参数为组的索引
print(rdd_dict.mapPartitionsWithIndex(f3).glom().collect())
# 和mapPartitionsWithIndex函数完全一样，不建议使用
print(rdd_dict.mapPartitionsWithSplit(f3).glom().collect())
