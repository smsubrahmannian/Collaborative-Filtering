from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator
from pyspark.ml.tuning import ParamGridBuilder


sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

# Data pre-processing in Spark
path = ../data/
data = sc.textFile(path+'reviews.csv',4).map(lambda x:x.split(','))
header = data.first() #extract header
ratingsRDD = data.filter(lambda row: row != header).map(lambda p: Row(userId=int(p[0]), businessId=int(p[1]),
                                     rating=int(p[2])))
ratings = spark.createDataFrame(ratingsRDD)
train,valid = ratings.randomSplit([0.8,0.2])


train.cache()
valid.cache()


# # Model  Training

# coldstartStrategy will ensure that we have no nan value
als = ALS(maxIter=10, regParam=0.001, userCol="userId",nonnegative=True
          ,itemCol="businessId", ratingCol="rating",coldStartStrategy="drop", rank =10)
model = als.fit(train)


pred_trn = model.transform(train)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                predictionCol="prediction")
rmse = evaluator.evaluate(pred_trn)
print("RMSE of training data = " + str(rmse))

pred_vld = model.transform(valid)
rmse = evaluator.evaluate(pred_vld)
print("RMSE of validation data = " + str(rmse))


# # Model Tuning


# We need to tune the parameters: maxIter, regParam,rank to achieve better results
cv = CrossValidator().setEstimator(als).setEvaluator(evaluator).setNumFolds(5)
#ParamGridBuilder() – combinations of parameters and their values.
paramGrid = ParamGridBuilder().addGrid(als.regParam,[0.001,0.01,0.005,0.05,0.1]).addGrid(model.rank,[8,10,12,14]).build()
#setEstimatorParamMaps() takes ParamGridBuilder().
cv.setEstimatorParamMaps(paramGrid)
cvmodel = cv.fit(train)
print "Accuracy : " +  str(RegressionEvaluator().evaluate(cvmodel.bestModel.transform(valid)))


# # Recommendations


# Generate top 10 movie recommendations for each user
userRecs = model.recommendForAllUsers(10)
# Generate top 10 user recommendations for each movie
movieRecs = model.recommendForAllItems(10)



print userRecs.take(1)


# In[ ]:

print movieRecs.take(1)

