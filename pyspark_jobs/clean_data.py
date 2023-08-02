from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer, Imputer
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
from pyspark.sql.functions import mean
from pyspark.sql.types import IntegerType

#conf = SparkConf().set('spark.jars','include/snowflake-jdbc-3.13.14.jar,include/spark-snowflake_2.12-2.10.0-spark_3.2.jar')
spark = SparkSession.builder.appName("pyspark_snowflake").master("local[*]").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").config("spark.kryoserializer.buffer.max", "2047MB").getOrCreate()

#spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.enablePushdownSession(spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())
#spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.disablePushdownSession(spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())

sfOptionsRaw = {
 "sfURL" : "NP61049.ap-southeast-1.snowflakecomputing.com",
 "sfAccount" : "NP61049",
 "sfUser" : "shailesh24",
 "sfPassword" : "Minsha@23",
 "sfDatabase" : "ANALYTICS",
 "sfSchema" : "RAW",
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

# --------- CRM -------------

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsRaw).option("dbtable","CRM").load()
df.show()
df=df.withColumn('GENDER', F.regexp_replace("GENDER", '^[Mm].*', 'Male')).withColumn('GENDER', F.regexp_replace("GENDER", '^[Ff].*', 'Female')).withColumn('GENDER', F.regexp_replace("GENDER", '^(?![MmFf]).*', 'null')).fillna('null',subset=['GENDER'])
df=df.withColumn('YEAR_OF_BIRTH', F.regexp_replace("YEAR_OF_BIRTH", '^([^0-9]*)$', 'null')).fillna('null',subset=['YEAR_OF_BIRTH'])
df.show()
mean_val=df.select(mean(df.YEAR_OF_BIRTH)).collect()
print("mean_year_birth", mean_val)
mean_year_birth=mean_val[0][0]
df=df.withColumn('YEAR_OF_BIRTH', F.regexp_replace("YEAR_OF_BIRTH", 'null', str(int(mean_year_birth))))
df.show()
df=df.withColumn("YEAR_OF_BIRTH", df["YEAR_OF_BIRTH"].cast(IntegerType()))
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("dbtable","CRM").mode("overwrite").save()


# columns=df.columns
# string_columns=["GENDER", "MSISDN", "YEAR_OF_BIRTH", "SYSTEM_STATUS", "MOBILE_TYPE", "VALUE_SEGMENT"]
# # Create a list of StringIndexers for each string column
# indexers = [StringIndexer(inputCol=col, outputCol=col + "_index", handleInvalid="keep") for col in string_columns]

# # Create an Imputer to replace null values in numeric columns (if any)
# imputer = Imputer(inputCols=df.columns, outputCols=df.columns)

# # Combine the indexers and imputer into a single pipeline
# pipeline = Pipeline(stages=indexers + [imputer])

# # Fit and transform the DataFrame to replace null values
# df_transformed = pipeline.fit(df).transform(df)

# # Drop the original string columns and index columns to get the final DataFrame
# for col in string_columns:
#     df_transformed = df_transformed.drop(col).withColumnRenamed(col + "_index", col)

# df_transformed.show()

# ----------- REVENUE -----------

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsRaw).option("dbtable","REVENUE").load()
df.show()
df=df.withColumn('WEEK_NUMBER', F.regexp_replace("WEEK_NUMBER", '^([^0-9]*)$', 'null')).fillna('null',subset=['WEEK_NUMBER'])
mean_week_val=df.select(mean(df.WEEK_NUMBER)).collect()
print("mean_week_number", mean_week_val)
mean_week=mean_week_val[0][0]
df=df.withColumn('WEEK_NUMBER', F.regexp_replace("WEEK_NUMBER", 'null', str(int(mean_week))))

df=df.withColumn('REVENUE_USD', F.regexp_replace("REVENUE_USD", '^([^0-9]*)$', 'null')).fillna('null',subset=['REVENUE_USD'])
mean_revenue_val=df.select(mean(df.REVENUE_USD)).collect()
print("mean_revenue_val", mean_revenue_val)
mean_revenue=mean_revenue_val[0][0]
df=df.withColumn('REVENUE_USD', F.regexp_replace("REVENUE_USD", 'null', str(mean_revenue)))
df.show()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("dbtable","REVENUE").mode("overwrite").save()


# --------- DEVICE ---------

df=spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsRaw).option("dbtable","DEVICE").load()
df.show()
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptionsCleansed).option("dbtable","DEVICE").mode("overwrite").save()

