from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer, Imputer
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import mean
from pyspark.sql.types import IntegerType
import pandas as pd
import matplotlib.pyplot as plt

#conf = SparkConf().set('spark.jars','include/snowflake-jdbc-3.13.14.jar,include/spark-snowflake_2.12-2.10.0-spark_3.2.jar')
spark = SparkSession.builder.appName("pyspark_snowflake").master("local[*]").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").config("spark.kryoserializer.buffer.max", "2047MB").getOrCreate()

#spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.enablePushdownSession(spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())
#spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.disablePushdownSession(spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())

sfOptionsCurated = {
 "sfURL" : "NP61049.ap-southeast-1.snowflakecomputing.com",
 "sfAccount" : "NP61049",
 "sfUser" : "shailesh24",
 "sfPassword" : "Minsha@23",
 "sfDatabase" : "ANALYTICS",
 "sfSchema" : "CURATED",
 "sfWarehouse" : "COMPUTE_WH",
 "sfRole" : "ACCOUNTADMIN",
}

sfOptionsCleansed = {
 "sfURL" : "NP61049.ap-southeast-1.snowflakecomputing.com",
 "sfAccount" : "NP61049",
 "sfUser" : "shailesh24",
 "sfPassword" : "Minsha@23",
 "sfDatabase" : "ANALYTICS",
 "sfSchema" : "CLEANSED",
 "sfWarehouse" : "COMPUTE_WH",
 "sfRole" : "ACCOUNTADMIN",
}
 
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
# df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsRaw).option("query","select * from crm limit 100").load()
# df.show()
#df.withColumn("query", lit("Total number of devices week wise"))
#df.show()
df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select t.week_number, count(*) as count from ANALYTICS.CURATED.TELECOM_ANALYSIS t group by 1").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","devices_week").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("query","select ta.week_number, count(*) as count from analytics.curated.telecom_analysis ta where ta.system_status = 'ACTIVE' group by 1").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","devices_week_active").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select t.week_number, sum(t.revenue_usd) as revenue from ANALYTICS.CURATED.TELECOM_ANALYSIS t where t.gender='Male' group by 1").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","revenue_male_week").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select t.week_number, sum(t.revenue_usd) as revenue from ANALYTICS.CURATED.TELECOM_ANALYSIS t where t.gender='Female' group by 1").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","revenue_female_week").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select t.week_number, t.year_of_birth, sum(t.revenue_usd) as revenue from  ANALYTICS.CURATED.TELECOM_ANALYSIS t group by 1,2").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","revenue_age_week").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select t.week_number, t.value_segment, sum(t.revenue_usd) as revenue from ANALYTICS.CURATED.TELECOM_ANALYSIS t group by 1,2").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","revenue_value_segment_week").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select t.week_number, t.mobile_type, sum(t.revenue_usd) as revenue from ANALYTICS.CURATED.TELECOM_ANALYSIS t group by 1,2").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","revenue_mobile_type_week").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select ta.week_number,ta.brand_name ,sum(ta.revenue_usd) as revenue from analytics.curated.telecom_analysis ta group by 1,2").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","revenue_brand_name_week").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select ta.week_number,ta.os_name ,sum(ta.revenue_usd) as revenue from analytics.curated.telecom_analysis ta group by 1,2").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","revenue_os_name_week").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select ta.week_number,ta.os_vendor ,sum(ta.revenue_usd) as revenue from analytics.curated.telecom_analysis ta group by 1,2").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","revenue_os_vendor_week").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select t.week_number,t.os_name, count(*) as count from ANALYTICS.CURATED.TELECOM_ANALYSIS t group by 1,2").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","distribution_os_week").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select t.week_number,t.brand_name, count(*) as count from ANALYTICS.CURATED.TELECOM_ANALYSIS t group by 1,2").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","distribution_brand_week").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select t.week_number,t.mobile_type, count(*) as count from ANALYTICS.CURATED.TELECOM_ANALYSIS t group by 1,2").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","distribution_mobile_type_week").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select t.week_number, t.brand_name, max(t.revenue_usd) as max_revenue, min(t.revenue_usd) as min_revenue from ANALYTICS.CURATED.TELECOM_ANALYSIS t group by 1,2").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","max_min_revenue_brand").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select t.week_number, t.os_name, max(t.revenue_usd) as max_revenue, min(t.revenue_usd) as min_revenue from ANALYTICS.CURATED.TELECOM_ANALYSIS t group by 1,2").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","max_min_revenue_os").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select d.brand_name,c.year_of_birth from ANALYTICS.CLEANSED.DEVICE d join ANALYTICS.CLEANSED.CRM c on d.msisdn = c.msisdn where c.year_of_birth >= (2023 - 30) group by 1,2").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","distribution_brand_age_range").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select d.os_name,c.year_of_birth from ANALYTICS.CLEANSED.DEVICE d join ANALYTICS.CLEANSED.CRM c on d.msisdn = c.msisdn where c.year_of_birth >= (2023 - 30) group by 1,2").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","distribution_os_age_range").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select t.year_of_birth, sum(t.revenue_usd) as revenue from ANALYTICS.CURATED.TELECOM_ANALYSIS t group by 1").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","total_revenue_age").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select t.value_segment, sum(t.revenue_usd) as revenue from ANALYTICS.CURATED.TELECOM_ANALYSIS t group by 1").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","total_revenue_value_segment").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select t.mobile_type, sum(t.revenue_usd) as revenue from ANALYTICS.CURATED.TELECOM_ANALYSIS t group by 1").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","total_revenue_mobile_type").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select t.brand_name, sum(t.revenue_usd) as revenue from ANALYTICS.CURATED.TELECOM_ANALYSIS t group by 1").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","total_revenue_brand_name").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select t.os_name, sum(t.revenue_usd) as revenue from ANALYTICS.CURATED.TELECOM_ANALYSIS t group by 1").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","total_revenue_os_name").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select t.os_vendor, sum(t.revenue_usd) as revenue from ANALYTICS.CURATED.TELECOM_ANALYSIS t group by 1").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","total_revenue_os_vendor").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select d.os_name, count(*) as total from ANALYTICS.CLEANSED.DEVICE d group by 1").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","total_distribution_os_name").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select d.brand_name, count(*) as total from ANALYTICS.CLEANSED.DEVICE d group by 1").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","total_distribution_brand_name").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select c.mobile_type, count(*) as total from ANALYTICS.CLEANSED.CRM c group by 1").load()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","total_distribution_mobile_type").mode("overwrite").save()

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select 'devices' as type, count(*) as total from ANALYTICS.CLEANSED.DEVICE").load()
df.show()
df_1=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select 'active_devices' as type, count(*) as total from analytics.curated.telecom_analysis ta where ta.system_status = 'ACTIVE'").load()
df_1.show()
df_2=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select 'revenue_by_male' as type, sum(ta.revenue_usd) as total from analytics.curated.telecom_analysis ta where ta.gender = 'Male'").load()
df_2.show()
df_3=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("query","select 'revenue_by_female' as type, sum(ta.revenue_usd) as total from analytics.curated.telecom_analysis ta where ta.gender = 'Female'").load()
df_3.show()
df_4=df_3.union(df_1)
df_4.show()
df_4=df_4.union(df_2)
df_4.show()
df_4=df_4.union(df)
df_4.show()
df_4.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCurated).option("dbtable","overall_result").mode("overwrite").save()

pandas_df=df_4.toPandas()
print(pandas_df)
pandas_df['TOTAL']=pd.to_numeric(pandas_df['TOTAL'])
pandas_df.set_index('TYPE').plot()
# pandas_df.plot(y='TOTAL',figsize=(10,6),title='Total values',ylabel = 'TOTAL')
# plt.show()
