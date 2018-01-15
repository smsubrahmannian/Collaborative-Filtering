from pyspark import SparkContext,SparkConf
from pyspark.sql.types import *
from pyspark.sql.types import *
from pyspark.sql import Row,SQLContext
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator
from pyspark.ml.tuning import ParamGridBuilder


conf = SparkConf().setMaster("spark://ip-172-31-23-230.us-west-2.compute.internal:7077").setAppName("First_Attempt")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# Data pre-processing in Spark
path = 'file:///root/Collaborative-Filtering/data/'
data = sc.textFile(path+'reviews.csv',12).map(lambda x:x.split(','))
header = data.first() #extract header
ratingsRDD = data.filter(lambda row: row != header).map(lambda p: Row(userId=int(p[0]), businessId=int(p[1]),
                                     rating=int(p[2])))
ratings = sqlContext.createDataFrame(ratingsRDD)
train,valid = ratings.randomSplit([0.8,0.2])


train.cache()
valid.cache()


# # Model  Training

# coldstartStrategy will ensure that we have no nan value
als = ALS(maxIter=10, regParam=0.001, userCol="userId",nonnegative=True
          ,itemCol="businessId", ratingCol="rating", rank =10)
model = als.fit(train)


pred_trn = model.transform(train)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                predictionCol="prediction")
rmse = evaluator.evaluate(pred_trn)
print("RMSE of training data = " + str(rmse))

pred_vld = model.transform(valid).na.drop("all",subset=['prediction'])
rmse = evaluator.evaluate(pred_vld)
print("RMSE of validation data = " + str(rmse))


# # Model Tuning


# We need to tune the parameters: maxIter, regParam,rank to achieve better results
cv = CrossValidator().setEstimator(als).setEvaluator(evaluator).setNumFolds(5)
paramGrid = ParamGridBuilder().addGrid(als.regParam,[0.001,0.01,0.005,0.05,0.1]).addGrid(als.rank,[8,10,12,14]).build()
cv.setEstimatorParamMaps(paramGrid)
cvmodel = cv.fit(train)
print "RMSE : " +  str(evaluator.evaluate(cvmodel.bestModel.transform(valid).na.drop("all",subset=['prediction'])))


# # Recommendations


# Generate top 10 movie recommendations for each user
userRecs = model.recommendForAllUsers(10)
# Generate top 10 user recommendations for each movie
movieRecs = model.recommendForAllItems(10)



print userRecs.take(1)

print movieRecs.take(1)
