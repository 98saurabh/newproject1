from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").config("spark.sql.secssion.timeZone", "EST").getOrCreate()
from pyspark.sql.types import *
#spark = SparkSession.builder.master("local[*]").appName("test").config("spark.sql.secssion.timeZone", "EST").getOrCreate()
data="C:\\bigdata\\sparkdataset\\datasets\\donations.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
def daystoyrmndays(nums):
    yrs=int(nums/365)
    mon=int((nums % 365) / 30)
    days=int((nums % 365) % 30)
    result = yrs, "years" , mon , "months", days, "days"
    st = ''.join(map(str,result))
    return st
   # here initially it is tuple format then it convert to string
 # now convert function to udf
udffunc = udf(daystoyrmndays)
#StringType is optional , udffunc is udf name
res=df.withColumn("dt",to_date(col("dt"),"d-m-yyyy"))\
    .withColumn("today",current_date())\
    .withColumn("ts",current_timestamp())\
    .withColumn("dtdiff",datediff(col("today"),col("dt")))\
    .withColumn("daystoyrmon", udffunc(col("dtdiff")))

res.printSchema()
res.show(truncate=False)
#spark--submit --master local --deploy-mode client datapattern.py
