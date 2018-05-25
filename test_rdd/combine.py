#!/usr/bin/env python

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('App')
sc = SparkContext(conf=conf)

rdd_list = sc.parallelize([1, 2, 3, 4, 5], 3)
rdd_dict = sc.parallelize([('a', 1), ('a', 2), ('a', 3), ('b', 4), ('b', 5)], 3)


# 当第一次出现一个键时，调用该函数来创建那个键对应的组合器的初始值
# 这一过程会在每个分区中第一次出现各个键时发生，而不是在整个RDD中第一次出现一个键时发生
def createCombiner(value):
    print('createCombiner', value)
    return 1, value
    # return value


# 如果这是一个在处理当前分区之前已经遇到的键，则调用该函数将该键的组合器对应的当前值与这个新的值进行组合
def mergeValue(value, newValue):
    print('mergeValue', value, newValue)
    return value[0]+1, value[1] + newValue
    # return value + newValue


# 如果有两个或者更多的分区都有对应同一个键的组合器，则调用该函数将各个分区的结果进行组合
def mergeCombiners(value, newValue):
    print('mergeCombiners', value, newValue)
    return value[0] + newValue[0], value[1] + newValue[1]
    # return value + newValue


print(rdd_dict.glom().collect())
# combineByKey可以让用户返回与输入数据的类型不同的返回值
print(rdd_dict.combineByKey(createCombiner, mergeValue, mergeCombiners).collect())
print(rdd_dict.combineByKey(createCombiner, mergeValue, mergeCombiners).glom().collect())
