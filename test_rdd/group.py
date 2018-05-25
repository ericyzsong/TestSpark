#!/usr/bin/evn python

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('App')
sc = SparkContext(conf=conf)

rdd_list = sc.parallelize([1, 2, 3, 4, 5])
rdd_dict = sc.parallelize([('a', 1), ('b', 2), ('c', 3), ('a', 4), ('b', 5)], 3)

print(rdd_list.glom().collect())
print(rdd_dict.glom().collect())

# 按照定义的排序规则进行排序，并返回已True和False为键、以内容为值的迭代器为值的RDD
rdd_group = rdd_list.groupBy(lambda x: x > 3)
print(type(rdd_group))
for kv in rdd_group.top(1, bool):  # kv类型为元组
    print('-', kv)
    for vi in kv[1]:
        print(kv[0], vi)

# 返回值为List，键的值类型为可迭代对象
rdd_group = rdd_dict.groupByKey().glom().collect()
print(type(rdd_group))
for kv in rdd_group:  # kv类型为列表，列表中元素类型为元组
    print('-', kv)
    if len(kv) != 0:
        for k, v in kv:
            for vi in v:
                print(k, vi)

rdd_dict2 = sc.parallelize([('a', 10), ('b', 20), ('c', 30)])
# 将两个RDD按键合并
print(rdd_dict.cogroup(rdd_dict2).collect())

rdd_dict3 = sc.parallelize([('a', 100), ('b', 200), ('c', 300)])
rdd_dict4 = sc.parallelize([('a', 1000), ('b', 2000), ('c', 3000)])
# 将两个或多个RDD按键合并
print(rdd_dict.groupWith(rdd_dict2, rdd_dict3, rdd_dict4).collect())
