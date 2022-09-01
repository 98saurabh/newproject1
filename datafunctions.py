from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").config("spark.sql.secssion.timeZone", "EST").getOrCreate()
import sys
path=sys.argv[1]
#THIS config("spark.sql.secssion.timeZone", "EST") HELPS TO GET USA TIME ZONE EST MEANS USA TIME ZONE
#data="C:\\bigdata\\sparkdataset\\datasets\\donations.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(path)
#spark by default able to understand "yyyy-mm-dd" format only
#but in original data u have dd-m-yyyy so this data format convert to spark understandable format
#to_date convert input date format to yyyy-mm-dd format

res=df.withColumn("dt",to_date(col("dt"),"d-m-yyyy"))\
    .withColumn("today",current_date())\
    .withColumn("ts",current_timestamp())\
    .withColumn("dtdiff",datediff(col("today"),col("dt")))\
    .withColumn("dtadd",date_add(col("dt"),-100))\
    .withColumn("lastdt",date_format(last_day(col("dt")),"yyyy-MM-dd-EEE"))\
    .withColumn("nextday",next_day(col("today"),"fri"))\
    .withColumn("dtformat",date_format(col("dt"),"dd/MMMM/yy/EEEE/zzz"))\
    .withColumn("monlstfri",next_day(date_add(last_day(col("today")),-7),"Fri"))\
    .withColumn("dayofweek",dayofweek("dt"))\
    .withColumn("dayofmon",dayofmonth("dt"))\
    .withColumn("dayofyr",dayofyear("dt"))\
    .withColumn("monbet",months_between(current_date(),col("dt")))\
    .withColumn("floor",floor(col("monbet")))\
    .withColumn("ceil",ceil(col("monbet")))\
    .withColumn("round",round(col("monbet")).cast(IntegerType()))\
    .withColumn("dttrunc",date_trunc("year",col("dt")))\
    .withColumn("weekofyear",weekofyear(col("dt")))
#First create udf to get expected date format
#def nums(days):
 #   yr=days%365
  #  mn=
   # days
    #full=f"%yr years %mon months %days days"

#dayofweek means from sunday how many days completed if sun 1, mon 2, sat 7
#dayofmon  from month 1 to how many days completed
#dayofyear  from jan1 to specified date how many days completed
#last_day returns months last day
#date_add(col("dt"),-100) and date_sub(df.dt,100) both are same
#from dt date you want to know date after 100 days

#to get diff between two date
res.printSchema()
res.show(truncate=False)

    #this will give sec min hrs also
#here you get indias or your computers date
#Its very imp based on original client data all default time based on us time only
# TO GET USA DATE ADD config("spark.sql.secssion.timeZone", "EST") THIS TO ABOVE LINE
#
#acc to input data you have to change date format
#res.printSchema()
#res.show(truncate=False)

#spark--submit --master local --deploy-mode client datafunctions.py


