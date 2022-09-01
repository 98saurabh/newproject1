from pyspark.sql import *
from pyspark.sql.functions import *
import configparser
from configparser import ConfigParser
conf=Configparser()
conf.read(r"C:\\bigdata\\spark-3.1.2-bin-hadoop3.2\\conf\\config.txt")
#here r is raw data or regex its not error
host=conf.get("cred","host")
user=conf.get("cred","user")
pwd=conf.get("cred","pass")
data=conf.get("input","data")
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
#data="C:\\bigdata\\sparkdataset\\datasets\\10000Records.csv"
#above data location is writen in folr so no need to write address
df=spark.read.format("csv").option("header","true").option("inferSchema","True").option("sep",",").load(data)

import re

cols=[re.sub('[^a-zA-Z0-9]',"",c.lower()) for c in df.columns]
ndf = df.toDF(*cols)

ndf=spark.write.mode("overwrite").option("url",host).option("user","user")\
    .option("password","pwd")\
    .option("dbtable","testsaurabh")\
    .option("driver","com.mysql.jdbc.Driver").save()

ndf.show()