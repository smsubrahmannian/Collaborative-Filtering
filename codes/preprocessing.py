from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import monotonically_increasing_id
import sys

ip_address = sys.argv[1]
#Create SparkContext
conf = SparkConf()
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

df_review = sqlContext.read.format("com.mongodb.spark.sql.DefaultSource")\
    .option("uri","mongodb://"+ip_address+"/yelp.review").load()

df_tip = sqlContext.read.format("com.mongodb.spark.sql.DefaultSource")\
    .option("uri","mongodb://"+ip_address+"/yelp.tip").load()

df_review.persist()
df_tip.persist()

## Mapping

userMap = df_review.select('user_id').union(df_tip.select('user_id')).distinct()
userMap = userMap.withColumn("user_ix", monotonically_increasing_id())

bizMap = df_review.select('business_id').union(df_tip.select('business_id')).distinct()
bizMap = bizMap.withColumn("biz_ix", monotonically_increasing_id())



## Join Dataframe to the review table

df_review = df_review.select('user_id','business_id','stars')
df_review = df_review.join(userMap,on='user_id',how='left_outer').join(bizMap,on='business_id',how='left_outer')
ALS_baseline_df = df_review.select('user_ix','biz_ix','stars')

##Save table


userMap.write.parquet("s3://dknsyelp/userMap.parquet", mode='overwrite')
bizMap.write.parquet("s3://dknsyelp/bizMap.parquet", mode='overwrite')
ALS_baseline_df.write.parquet("s3://dknsyelp/ALS_baseline.parquet", mode='overwrite')




spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.0 --master yarn  --driver-memory 45G --driver-cores 4 --executor-cores 4 --executor-memory 45G preprocessing.py > /home/sooraj/preprocess_out.txt