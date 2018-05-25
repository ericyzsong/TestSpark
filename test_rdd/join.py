#!/usr/bin/env python

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('App')
sc = SparkContext(conf=conf)

rdd_dict = sc.parallelize([('a', 1), ('b', (2, 20)), ('c', 3), ('d', 4), ('e', 5)])
rdd_dict2 = sc.parallelize([('a', 10), ('b', (20, 200)), ('c', 30), ('f', 60), ('g', 70)])

# join只连接在两个RDD中都存在的键，且返回的RDD中值的类型为元组
print(rdd_dict.join(rdd_dict2).glom().collect())
# leftOuterJoin连接时以左边RDD的键为基准，若右边RDD不存在该键，则对应值为None
print(rdd_dict.leftOuterJoin(rdd_dict2).glom().collect())
# leftOuterJoin连接时以右边RDD的键为基准，若左边RDD不存在该键，则对应值为None
print(rdd_dict.rightOuterJoin(rdd_dict2).glom().collect())
