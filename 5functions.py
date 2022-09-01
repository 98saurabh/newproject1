from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="C:\\bigdata\\sparkdataset\\datasets\\us-500.csv"
#df=spark.read.format("csv").option("header","true").option("inferSchema","True").load(data)
#ndf=df.groupBy(df.state).agg(count("*").alias("cnt"),collect_list(df.first_name)).orderBy(col("cnt").desc())
#ndf=df.groupBy(df.state).agg(count("city").alias("cnt"),collect_set(df.city).alias("name" )).orderBy(col("cnt").desc())
#collect_set means remove duplicate elements . above add city under count so we get count of that column
#and collect_set is performed on it
#ndf=df.withColumn("address1",when(col("address").contains("#"),"***").otherwise(col("state")))\
 #     .withColumn("address2",regexp_replace(col("address"),"#","-"))

#ndf1=df.withColumn("substr",substring(col("email"),0,5)) \
#   .withColumn("emails",substring_index(col("email"),"@",-1))\
#  .withColumn("usernames",substring_index(col("email"),"@",1))\

#ndf=ndf1.groupBy(col("emails")).count().orderBy(col("count").desc())

# we selected 0 to 5 char. from column email
#1: from 0th position to position before @
#-1: from last position to position after @
#ndf.show()
#ndf.printSchema()


#ndf=df.withColumn("state",when(col("state")=="NY","NweYork").otherwise(col("state")))
#with is used whe you want to replace any value in column
#you can use when condition many times
#ndf.show(truncate=False)
#ndf.printSchema()


#you can also give column name like df.state
#if you want to give alias name then you must use agg(count("*"))

#ndf=df.withColumn("fullname",concat_ws(" ",df.first_name, df.last_name,df.state))\

#   .withColumn("phone1",regexp_replace(col("phone1"),"-","").cast(LongType()))\
#    .withColumn("phone2",regexp_replace(col("phone2"),"-","").cast(LongType()))\
#    .drop("email","city","county","address")\
#    .withColumnRenamed("first_name","fname").withColumnRenamed("last_name","lname")\

#changing column name

# dropping unwanted column
#when we have too many columns and dont want some columns then use

# here insted of int we taking cast(LongType) bcos phone no is int type but bcos no. is
# long so it crosses limit of phone no




#regexp_replace  remove - and"" means we dont mention any value hence phone1 values come together

#concat_ws means with space,
#withcolumn used to add new column(if column not exist) or update(if already column exist)
#lit(value) used to add something dummy value
#ndf.show()
#ndf.printSchema()

df=spark.read.format("csv").option("header","true").option("inferSchema","True").load(data)
#to join two columns with teh help of sql query
df.createOrReplaceTempView("tab")
qry="""with tmp as (select * , concat_ws('_', first_name, last_name) fullname,
substring_index(email,'@',-1 ) mail from tab)
select mail, count(*) cnt from tmp group by mail order by cnt desc
"""
#ndf=spark.sql(qry)
#create your own function if not available in spark
def func(st):
    if(st=="NY"):
        return"30% off"
    elif(st=="CA"):
        return "40% off"
    elif(st=="OH"):
        return "50% off"
    else:
        return "500/- off"
#by default spark unable to understand python function
# so convert python/scala/java function to udf(spark able to understand udf)
uf=udf(func)
#user defined function convert to sql function
spark.udf.register("offer",uf)
ndf=spark.sql("select *, offer(state) todayoffers from tab")

#now call function
#ndf=df.withColumn("offer",uf(col("state")))
ndf.show()
#ndf.printSchema()
