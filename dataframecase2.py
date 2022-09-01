from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data="C:\\bigdata\\sparkdataset\\datasets\\bank-full.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","True").option("sep",";").load(data)
#bydefault sep sets a separator for each field i.e ,  but file is separated by ; so we have to mention sep
#sep option used to specify delimiter, If you not mention inferSchema then you get all datatype as string
#if u not mention like this 1000+1000 if int .. 2000 if string..  u will get 10001000
#res=df.select(col("age"),col("marital"),col("balance")).where((col("age")>60) | (col("marital")!="married")&(col("balance")>=40000))
#Inabove case I want to only particular columns
# this process is sql friendly
df.createOrReplaceTempView("tab")
#createOrReplaceTempView it register this dataframe as a table its very useful to run sql
#res=spark.sql("select * from tab where age>50 and balance>50000")
#res=spark.sql("select marital, sum(balance) AS  sumbal from tab group by(marital)")
#res=df.groupBy(col("marital")).agg(sum(col("balance")).alias("smb")).orderBy(col("smb").desc())
res=df.groupBy(col("marital")).agg(count("*").alias("cnt"),sum(col("balance")).alias("smb"))\
    .where(col("balance")>=avg(col("balance")).orderBy(col("smb").desc())
res.show()
res.printSchema()



#.where(col("balance")>=avg(col("balance"))