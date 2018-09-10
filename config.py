import os
import sys

os.environ['hadoop_home']="C:/hadoop"

print (os.environ.get('hadoop_home'))


from pyspark import SparkConf,SparkContext

def main(sc):
    file= sc.parallelize("a b c c",1)
    rdd=file.flatMap(lambda x:x.split(" "))
    rdd1=rdd.map(lambda w:(w,1))
    rdd2=rdd1.reduceByKey(lambda a,b:a+b)
    print (rdd2.collect())


if __name__ =="__main__":


    conf = SparkConf().setAppName("wordcount")\
        .setMaster("local")
    sc = SparkContext(conf=conf)
    main(sc)






