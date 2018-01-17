from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import monotonically_increasing_id
import sys

ip_address = '54.245.171.238'
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

df_review_als = df_review.select('user_id','business_id','stars')
df_review_als = df_review_als.join(userMap,on='user_id',how='left_outer').join(bizMap,on='business_id',how='left_outer')
ALS_baseline_df = df_review_als.select('user_ix','biz_ix','stars')
df_tip_df = df_tip.join(userMap,on='user_id',how='left_outer').join(bizMap,on='business_id',how='left_outer')
## drop and Save table

userMap.write.parquet("s3://dknsyelp/userMap.parquet", mode='overwrite')
bizMap.write.parquet("s3://dknsyelp/bizMap.parquet", mode='overwrite')
ALS_baseline_df.write.parquet("s3://dknsyelp/ALS_baseline.parquet", mode='overwrite')
df_review.write.parquet("s3://dknsyelp/review.parquet", mode='overwrite')
df_tip_df.write.parquet("s3://dknsyelp/tip.parquet", mode='overwrite')

print 'Successful'

ALS_baseline_df.show()


