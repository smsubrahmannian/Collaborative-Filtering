from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.ml.feature import StringIndexer
import sys

ip_address = '54.214.104.75'
#Create SparkContext
conf = SparkConf()
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

df_review = sqlContext.read.format("com.mongodb.spark.sql.DefaultSource")\
    .option("uri","mongodb://"+ip_address+"/yelp.review").load()

df_tip = sqlContext.read.format("com.mongodb.spark.sql.DefaultSource")\
    .option("uri","mongodb://"+ip_address+"/yelp.tip").load()

rstdata = sc.textFile('./Collaborative-Filtering/data/restaurant_ids_final.txt').map(lambda x: (x,1))\
.toDF(['business_id','biz_ix']).select('business_id')

df_review.persist()
df_tip.persist()
rstdata.persist()

## Mapping

userMap = df_review.select('user_id').union(df_tip.select('user_id')).distinct()
indexer_userid = StringIndexer(inputCol="user_id", outputCol="user_ix")
userMap = indexer_userid.fit(userMap).transform(userMap)

bizMap = df_review.select('business_id').union(df_tip.select('business_id')).distinct()
indexer_biz = StringIndexer(inputCol="business_id", outputCol="biz_ix")
bizMap = indexer_biz.fit(bizMap).transform(bizMap)
bizMap = rstdata.join(bizMap,on='business_id',how='inner')


## Join Dataframe to the review table

df_review_als = df_review.select('user_id','business_id','stars')
df_review_als = df_review_als.join(userMap,on='user_id',how='left_outer').join(bizMap,on='business_id',how='inner')
ALS_baseline_df = df_review_als.select('user_ix','biz_ix','stars')
df_tip_df = df_tip.join(userMap,on='user_id',how='left_outer').join(bizMap,on='business_id',how='inner')
## drop and Save table

userMap.write.parquet("s3://dknsyelp/userMap.parquet", mode='overwrite')
bizMap.write.parquet("s3://dknsyelp/bizMap.parquet", mode='overwrite')
ALS_baseline_df.write.parquet("s3://dknsyelp/ALS_baseline.parquet", mode='overwrite')
df_review.write.parquet("s3://dknsyelp/review.parquet", mode='overwrite')
df_tip_df.write.parquet("s3://dknsyelp/tip.parquet", mode='overwrite')


print 'Successful'

ALS_baseline_df.show()


