#!/usr/bin/env python

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('App')
sc = SparkContext(conf=conf)

rdd_list = sc.parallelize(range(1, 11), 3)

print(rdd_list.glom().collect())
# 每个RDD都有固定数目的分区，分区数决定了在RDD上执行操作时的并行度
# 对数据进行重新分区是代价相对比较大的操作，重新分区不改变原RDD的内容
print(rdd_list.repartition(5).glom().collect())

# coalesce是repartition的优化版
# 第二个参数表示：是否减少分区中元素的数量，以便将元素均匀分布在输出分区上。否则，重新分区可能会产生高度倾斜的分区
print(rdd_list.coalesce(5, True).glom().collect())
print(rdd_list.coalesce(5, False).glom().collect())

print(rdd_list.getNumPartitions())
