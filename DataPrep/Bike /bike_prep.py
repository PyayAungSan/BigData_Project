from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

df_bike = spark.read.format('csv').options(header='true', inferschema='true').load('citi_bike.csv')

df_bike.registerTempTable('bike')

df1 = spark.sql("select * from bike where starttime like '%/%'")
df2 = spark.sql("select * from bike where starttime like '%-%'")

df3 = df1.withColumn('starttime',to_timestamp('starttime', 'MM/dd/yyyy HH:mm:ss').alias('starttime'))

df4 = df3.withColumn('stoptime',to_timestamp('stoptime', 'MM/dd/yyyy HH:mm:ss').alias('stoptime'))

result = df2.union(df4)

result.registerTempTable('bike2')

result2 = spark.sql("SELECT year(starttime) as year,month(starttime) as month, count(*) as count from bike2 group by year,month order by year,month")

result2.select("*").write.save("bike_monthly.csv", format="csv")