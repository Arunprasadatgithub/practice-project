import os
import sys
import logging

os.environ['hadoop_home']="C:/hadoop"
os.path
print (os.environ.get('hadoop_home'))

from pyspark import SparkConf,SparkContext,HiveContext
from pyspark.sql import SparkSession

# Variables

sc=''

"""
Spark session creation usinf spark builder
Spark version 2.2
Author : Arun prasad D
version : 0.1
"""


def sparkcontext_config():
    global sc
    sc=SparkSession.builder \
            .master("local")\
            .appName("practice")\
            .enableHiveSupport()\
            .getOrCreate()
    # sc = SparkConf().setAppName("wordcount")\
    #     .setMaster("local")
    # sc = SparkContext(conf=conf)
    return sc

def logger_conf():
    logger=logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    file_handler=logging.FileHandler("error.log")
    formater=logging.Formatter("%(asctime)s:%(filename)s:%(levelname)s:%(message)s")
    file_handler.setFormatter(formater)
    logger.addHandler(file_handler)
    return logger


