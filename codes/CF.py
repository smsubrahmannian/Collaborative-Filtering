from pyspark import SparkContext,SparkConf
from pyspark.sql.types import *
from pyspark.sql.types import *
from pyspark.sql import Row,SQLContext
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator
from pyspark.ml.tuning import ParamGridBuilder
import shutil

conf = SparkConf()
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# Data pre-processing in Spark
# Loads parquet file located in AWS S3 into RDD Data Frame
parquetFile = sqlContext.read.parquet("s3://dknsyelp/ALS_baseline.parquet")

# Stores the DataFrame into an "in-memory temporary table"
parquetFile.registerTempTable("parquetFile")

# Run standard SQL queries against temporary table
ratings = sqlContext.sql("SELECT * FROM parquetFile")
train,valid = ratings.randomSplit([0.8,0.2])
train.cache()
valid.cache()


# # Model  Training

# coldstartStrategy will ensure that we have no nan value
als = ALS(maxIter=10, regParam=1, userCol="user_ix",nonnegative=True
          ,itemCol="biz_ix", ratingCol="stars", rank =10)
model = als.fit(train)


pred_trn = model.transform(train)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="stars",
                                predictionCol="prediction")
rmse = evaluator.evaluate(pred_trn)
print("RMSE of training data = " + str(rmse))

pred_vld = model.transform(valid).na.drop("all",subset=['prediction'])
rmse = evaluator.evaluate(pred_vld)
print("RMSE of validation data = " + str(rmse))


# # Model Tuning


# We need to tune the parameters: maxIter, regParam,rank to achieve better results
cv = CrossValidator().setEstimator(als).setEvaluator(evaluator).setNumFolds(5)
paramGrid = ParamGridBuilder().addGrid(als.rank,[8,12,15,20]).build()
cv.setEstimatorParamMaps(paramGrid)
cvmodel = cv.fit(train)
print "RMSE : " +  str(evaluator.evaluate(cvmodel.bestModel.transform(valid).na.drop("all",subset=['prediction'])))


# # Recommendations


# Generate top 10 movie recommendations for each user
#userRecs = model.recommendForAllUsers(10)
# Generate top 10 user recommendations for each movie
#movieRecs = model.recommendForAllItems(10)



#print userRecs.take(1)
print cvmodel.bestModel.params
#print movieRecs.take(1)

try:
    shutil.rmtree('./savedmodel/')
except:
    pass
cvmodel.bestModel.save('./savedmodel')
