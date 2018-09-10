import os
import sys

os.environ['hadoop_home']="C:/hadoop"

print (os.environ.get('hadoop_home'))


from pyspark import SparkConf,SparkContext
from pyspark.serializers import MarshalSerializer
def main(sc):
    file= sc.parallelize("a,b,c,c,d,c,f,t,t")
    print(file.glom().collect())
    file =file.repartition(3)
    print(file.toDebugString())
    try:
        sc.dumps(my_object)
    except:
        print("Unable to serialize the object",sys.exc_info()[0])
    print(file.getNumPartitions())
    rdd0=file.flatMap(lambda x:x.split(","))
    rdd1=rdd0.map(lambda w:(w,1))
    rdd2=rdd1.reduceByKey(lambda a,b:a+b)
    print(rdd2.toDebugString())
    print (rdd2.collect())
    d=rdd2.glom().collect()
    f=open("E:/b.txt",'a')
    f.write(str(d)+'\n')



if __name__ =="__main__":


    conf = SparkConf().setAppName("wordcount")\
        .setMaster("local")
    sc = SparkContext(conf=conf,serializer=MarshalSerializer())
    main(sc)





