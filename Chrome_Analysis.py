import sqlite3
import sys
import os
import operator
from collections import OrderedDict
import pandas as pd
import numpy as np
from Practice.pyspark_conf import sparkcontext_config,logger_conf


url_dict={}
invalid_urls={}
report=''
logger=logger_conf()

def getdata():
    data_path=os.path.expanduser('~')+"\\AppData\\Local\\Google\\Chrome"
    files=os.listdir(data_path)
    historydb=os.path.join(data_path,"History")
    return historydb

def connection(database):
    if(database!=''):
        try:
            conn=sqlite3.connect(database)
            cursor=conn.cursor()
            #sql="select name from sqlite_master where type = 'table'";
            #sql="SELECT * FROM  visits;"# WHERE urls.id = visits.url;"
            sql = "SELECT urls.url, urls.visit_count FROM urls, visits WHERE urls.id = visits.url;"
            cursor.execute(sql)
            names=list(map(lambda x:x[0],cursor.description))
            print ("names:",names)
            result=cursor.fetchall()
            cursor.close()
            logger.info("connection successful")
            return result
        except:
            logger.error("Connection error:",sys.exc_info())

def parse_URL(urls):
    try:
        parsed_url=urls.split('//')
        if len(parsed_url)>1:
            url=parsed_url[1].split('/')
            return url[0]
        else:

            for urls in invalid_urls:
                invalid_urls[urls]+=1
            else:
                invalid_urls[urls]=1

    except:
        logger.error('parsed_url:',parsed_url)
        logger.error('urls:',urls)
        logger.error('Error:',sys.exc_info())

def url_count(url):
    try:

        #if url_dict.get(url)!='':
        if url in url_dict:
            url_dict[url]+=1
        else:
            url_dict[url]= 1

        return url_dict
    except:
        logger.error(sys.exc_info())



if __name__=='__main__':

    try:
        database=getdata()
        results=connection(database)

        urls=[parse_URL(result[0]) for result in results]
        for url in urls:
          report=url_count(url)

        sites_count_sorted = OrderedDict(sorted(report.items(), key=operator.itemgetter(1), reverse=True))
        df=pd.DataFrame(list(sites_count_sorted.items()),columns=['site','count'])
        print(sites_count_sorted)
        sc=sparkcontext_config()
        logger.info("sparkcontext",sc)
        spark_df=sc.createDataFrame(list(sites_count_sorted.items()),['site','count'])
        print(spark_df.show())

        # for site,count in sites_count_sorted.items():
        #    print(site,':',count)
        #print('Report:',report)
        for a,b in invalid_urls.items():
            logger.info("********** Encountered Invalid URLs **********")
            logger.info("{}{}".format(a,b))
    except:
        logger.error(sys.exc_info())

else:
    logger.warning("Main not found")
    raise Exception("Main not found")