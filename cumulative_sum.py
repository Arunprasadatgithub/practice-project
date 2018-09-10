import sqlite3
import sys
import os
import operator
from collections import OrderedDict
import pandas as pd
import numpy as np
from Practice.pyspark_conf import sparkcontext_config,logger_conf
from pyspark.sql.functions import *
# import pyspark.sql.functions
from pyspark.sql.types import *
from pyspark.sql.window import Window


value=0

logger=logger_conf()

def sum(values):
    try:
        global value
        value=value+values
        return value
    except:
            print(sys.exc_info())

if __name__=='__main__':
    try:
         spark = sparkcontext_config()
         a=[(1, 100), (1, 200), (3, 400), (4, 500), (5, 600),(3,540),(1,300),(1,2)]
         # a_rdd=spark.parallelize(a)

         df=spark.createDataFrame(a,['days','sales'])
         print(df.rdd.getNumPartitions())
         sales=df.select('sales')
         sales_sum=udf(sum,IntegerType())
         df2 = df.withColumn("cum",sales_sum(df.sales))
         # df2.write.format("json").mode("overwrite").save('E:\df2.json')
         print(df2.show())
         df2.registerTempTable('sales_tb')
         new_df=spark.sql("""select days,{1}+1 as daily_sales from {0} where {2}>500""".format('sales_tb','sales','cum'))
         print(new_df.show())
         rank_df=df2.withColumn("rank",dense_rank().over(Window.partitionBy("days").orderBy(desc("cum"))))
         print(rank_df.show())
         # df3 = spark.read.json('C:/Users/Arun prasad/PycharmProjects/Sample/venv/Practice/df2.json/part-00000-88287c46-6d19-488b-9b86-a31189622b14-c000.json')
         # print(df3.show())

    except:
        print(sys.exc_info())