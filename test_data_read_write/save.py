#!/usr/bin/env python

import os
import shutil
import json
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('App')
sc = SparkContext(conf=conf)

rdd_list = sc.parallelize([1, 2, 3, 4, 5], 3)
rdd_dict = sc.parallelize([('a', 1), ('a', 2), ('a', 3), ('b', 4), ('b', 5)], 3)

# Spark将传入的路径作为目录对待，会在那个目录下输出多个文件，这样Spark就可以从多个节点上并行输出了
# 在这个方法中，不能控制数据的哪一部分输出到哪个文件中，不过有些输出格式支持控制
if os.path.exists('/data/list'):
    shutil.rmtree('/data/list')

rdd_list.saveAsTextFile('/data/list')

if os.path.exists('/data/dict'):
    shutil.rmtree('/data/dict')

rdd_dict.saveAsTextFile('/data/dict')

# 文件每一行必须是完整的json字符串或数组
rdd = sc.textFile('/data/*.json')
# 若不是，完整的json字符串或数组不全在一行，需要将多行转换成单一一行
# rdd = sc.parallelize([rdd.fold('', lambda x, y: x.strip() + y.strip())])

rdd = rdd.map(lambda x: json.loads(x))


def f(x):
    print('-', type(x), x)
    return x['firstName']


def f2(x):
    print('*', type(x), x)
    return json.dumps(x)


if os.path.exists('/data/json'):
    shutil.rmtree('/data/json')

rdd.filter(f).map(f2).saveAsTextFile('/data/json')
