from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
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
#tabs=['dept','EMP','abc','banktab','DEPt']
qry="(select TABLE_NAME from information_schema.TABLESs where TABLE_SCHEMA='sravanthidb') AAA"
#in above qry we want only table_name bcos
# it is input to tabs bcos we have to display all this tables
#first reading data as below
df1=spark.read.format("jdbc").option("url",host).option("user",user)\
     .option("password",pwd)\
     .option("dbtable",qry)\
     .option("driver","com.mysql.jdbc.Driver").load()
#now convert df1 to list
tabs=[x[0] for x in df1.collect() if df1.count()>0]
#host="jdbc:mysql://sravanthidb.c7nqndsntouw.us-east-1.rds.amazonaws.com:3306/sravanthidb"
#to display all tables using for loop
for i in tabs:
     df=spark.read.format("jdbc").option("url",host).option("user",user)\
     .option("password",pwd)\
     .option("dbtable",i)\
     .option("driver","com.mysql.jdbc.Driver").load()

df.show()
spark--submit --master local --deploy-mode client alltables.py
