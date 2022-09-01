from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
#sc = spark.sparkContext
data="C:\\bigdata\\sparkdataset\\datasets\\donations1.csv"
#1)
#df=spark.read.format("csv").load(data)
#in above case header is coming with data but we dont want header in data
#2)
#df=spark.read.format("csv").option("header","true").load(data)
#now we can get correct column names , this first line is considered as schema
# if you mention header true, first line of data considered as columns
#df.printSchema()
#df.show()

#4)
data="C:\\bigdata\\sparkdataset\\datasets\\donations1.csv"
#now you want to resolve heading problem from previously performed steps
rdd=spark.sparkContext.textFile(data)
skip=rdd.first()  # select first line
odata=rdd.filter(lambda x:x!=skip)#odata means data without any comment
# i.e on first line we write some information is removed and we have required data
#by default we are considering it is rdd from this we dont want first linehence we are writing x!=skip
#if you have any mal rcords like first line second line wrong clean that data using rdd or udf
#df=spark.read.csv(odata,header=True)
df=spark.read.csv(odata,header=True,inferSchema=True)
#inferSchema automatically data is converted into its data types
df.printSchema()
df.show(5)

