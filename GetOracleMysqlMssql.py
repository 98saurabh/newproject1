from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("testing").getOrCreate()
host="jdbc:mysql://sravanthidb.c7nqndsntouw.us-east-1.rds.amazonaws.com:3306/sravanthidb"
qry="(select * from banktab where age>60) t"
#t is one temperory variable you can give any name
#all result is stored in temperory table t
df=spark.read.format("jdbc").option("url",host).option("user","myuser")\
    .option("password","mypassword")\
    .option("dbtable",qry)\
    .option("driver","com.mysql.jdbc.Driver").load()
res=df
#dbtable means what are the table you want to read
#as you mention url in option it automatically consider
# you can also mention pwd and uname in options even you mention url
res.show()