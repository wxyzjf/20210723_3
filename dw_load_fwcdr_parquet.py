[mapr@bigdatamr10 script]$ cat dw_load_fwcdr_parquet.py
from __future__ import print_function

from pyspark.sql import SparkSession

from pyspark.sql import Row

from pyspark.sql.types import *

from datetime import datetime,timedelta

from pyspark import SparkContext,SparkConf

import sys

yesday = (datetime.now()-timedelta(days=2)).strftime('%Y%m%d')
print(yesday)


indir = "maprfs:///HDS_VOL_HIVE/FWCDR/start_date=" + yesday + "/*fwcdr_ldr*"
outdir = "maprfs:///HDS_VOL_HIVE/FWCDR_PQ"
print(indir)
print(outdir)


#conf = SparkConf().setAppName("fwcdr_256").setAll([('spark.executor.instances',25),('spark.executor.memory','10G'),('spark.executor.memoryOverhead','2G')])
#conf = SparkConf().setAppName("fwcdr").setAll([('spark.executor.memory','240G'),\
#                                                   ('spark.driver.maxResultSize','4G'),\
#                                                   ('spark.driver.memory','240G'),\
#                                                   ('spark.sql.shuffle.partitions',100),\
#                                                   ('spark.default.parallelism',100),\
#                                                   ('spark.eventLog.dir','/app/HDSDATA/SPK_HS_LOG/'),\
#                                                   ('spark.executor.cores','30')\
#                                                        ])

conf = SparkConf().setAppName("fwcdr").setAll([('spark.executor.instances',30),\
                                                   ('spark.executor.memory','10G'),\
                                                   ('spark.driver.maxResultSize','4G'),\
                                                   ('spark.driver.memory','240G'),\
                                                   ('spark.default.parallelism',200),\
                                                   ('spark.sql.shuffle.partitions',200)\
                                                   ])

sc = SparkContext(conf=conf)
spark = SparkSession(sc)

#sc._jsc.hadoopConfiguration().setInt("dfs.blocksize",1024*1024*256)
#sc._jsc.hadoopConfiguration().setInt("parquet.block.size",1024*1024*256)

print(datetime.now())


mySchema = StructType([
            StructField("rowkey",StringType(), True),
            StructField("ts",StringType(), True),
            StructField("srcip",StringType(), True),
            StructField("dstip",StringType(), True),  
            StructField("add",StringType(), True)
            ])


#fwcdr = spark.read.schema(mySchema).csv("maprfs:///HDS_VOL_HIVE/FWCDR/start_date=20200803/*fwcdr_ldr*",sep='|')

fwcdr = spark.read.schema(mySchema).csv(indir,sep='|')

fwcdr.createOrReplaceTempView("fwcdr")

df = spark.sql("select replace(substr(ts,1,10),'-','') as trx_date,\
                       int(substr(rowkey,9,3)) as pkey,\
                       int(substr(rowkey,9,3)) as srcip_3rd,\
                       cast(concat(lpad(substring_index(srcip,'.',1),3,'0'),\
                              lpad(substring_index(substring_index(srcip,'.',2),'.',-1),3,'0'),\
                              lpad(substring_index(substring_index(srcip,'.',3),'.',-1),3,'0'),\
                              lpad(substring_index(srcip,'.',-1),3,'0')) as long) as srcip_num,\
                       ts,srcip,dstip,\
                       substring_index(add,'-',1) as send_vol,\
                       substring_index(substring_index(add,'-',2),'-',-1) as recv_vol,\
                       substring_index(add,'-',-1) as srv\
                from fwcdr")\
          .repartition('pkey')\
          .write.partitionBy('trx_date','pkey').save(outdir,mode='append',compression='gzip')

          #.sortWithinPartitions("pkey","srcip_num") 
        
       # .repartition('pkey','srcip_num')\

#df.printSchema()
#df.show(10)


print(datetime.now())
