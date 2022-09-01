from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="C:\\bigdata\\sparkdataset\\datasets\\10000Records.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","True").option("sep",",").load(data)
#you have to remove special character before processing
# data is too large so by default it display top20 records only and if any field having
# more than 20 character its truncated and shows ...
#df.show(5,truncate=False)
import re  # re .. replace except all small letters,capitalletter,
# and number except those any other symbols
num=int(df.count()) # here you have to count records
cols=[re.sub('[^a-zA-Z0-9]',"",c.lower()) for c in df.columns]
#^a-zA-Z0-9 except this if you found any special character then remove that
#removing space
#c.lower() means you want all letters small
ndf=df.toDF(*cols)
#toDF means rename all column headings and convert rdd to dataframe * means all
ndf.show(20,truncate=False)
#truncated means dont show full records it is by default true to see full records
# make it false
ndf.printSchema()

#data processing programming friendly(dataframe api)
res=ndf.groupBy(col("gender")).agg(count(col("*")).alias("cnt"))

res.show()
