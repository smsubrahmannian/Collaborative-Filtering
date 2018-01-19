from pyspark import SparkContext,SparkConf
from pyspark.sql.types import *
from pyspark.sql import Row,SQLContext
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.feature import CountVectorizer, Tokenizer, StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.sql.functions import countDistinct, udf, col
from pyspark.mllib.linalg import Vectors, DenseMatrix
from sklearn.manifold import TSNE
import bokeh.plotting as bp
from bokeh.models import HoverTool, BoxSelectTool
from bokeh.plotting import figure, show, output_notebook
from bokeh.plotting import figure, output_file, save
from bokeh.plotting import figure, output_file, save
import matplotlib.pyplot as plt
from pylab import figure, axes, pie, title, show

sqlContext = SQLContext(sc)

parquetFile = sqlContext.read.parquet("s3://dknsyelp/ALS_baseline.parquet")

# Stores the DataFrame into an "in-memory temporary table"
parquetFile.registerTempTable("parquetFile")

# Run standard SQL queries against temporary table
ratings = sqlContext.sql("SELECT * FROM parquetFile")
train,valid = ratings.randomSplit([0.8,0.2])
train.cache()
valid.cache()

###RMSE v/s Iterations
range_iter = [10,20,30,40,50]
l = []
for i in range_iter:
    als = ALS(maxIter=i, regParam=1, userCol="user_ix", itemCol="business_ix", ratingCol="stars",coldStartStrategy="drop", rank=8)
    model = als.fit(train)
    predictions = model.transform(valid)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="stars",
                                predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    l.append(rmse)
    print("Root-mean-square error = " + str(rmse))

plt.ylabel('RMSE')
plt.xlabel('Iterations')
plt.plot(range_iter,l)
plt.savefig("s3://dknsyelp/RMSE_Iterations.png")

#Rmse v/s Data_percentage

data_iter = [0.25,0.5,0.75,0.99]
l = []
for i in data_iter:
    (data, reject_data) = ratings.randomSplit([i, 1-i])
    (train, test) = data.randomSplit([0.9, 0.1])
    als = ALS(maxIter=50, regParam=1, userCol="user_ix", itemCol="business_ix", ratingCol="stars",coldStartStrategy="drop", rank=8)
    model = als.fit(train)
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="stars",
                                predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    l.append(rmse)
    print("Root-mean-square error = " + str(rmse))
    
plt.ylabel('RMSE')
plt.xlabel('Ratings')
plt.plot(data_iter,l)
plt.savefig("s3://dknsyelp/Rmse_ratings.png")

#TIme v/s Data_percentage

import timeit
data_iter = [0.25,0.5,0.75,0.99]
l = []

for i in data_iter:
    (data, reject_data) = ratings.randomSplit([i, 1-i])
    (training, test) = data.randomSplit([0.9, 0.1])
    start = timeit.default_timer()
    als = ALS(maxIter=50, regParam=1, userCol="user_ix", itemCol="business_ix", ratingCol="stars",coldStartStrategy="drop", rank=8)
    model = als.fit(training)
    predictions = model.transform(test)
    l.append(start-stop)
    
    
plt.ylabel('Time')
plt.xlabel('Ratings')
plt.plot(data_iter,l)

plt.savefig("s3://dknsyelp/Time_ratings.png")


























