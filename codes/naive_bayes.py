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


# Create SparkContext
conf = SparkConf()
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


# Load data from mongo
df_tip = sqlContext.read.format("com.mongodb.spark.sql.DefaultSource").\
option("uri","mongodb://54.245.171.238/yelp.tip").load()
df_review = sqlContext.read.format("com.mongodb.spark.sql.DefaultSource").\
option("uri","mongodb://54.245.171.238/yelp.review").load()


# Clean data
df_review = df_review.select(df_review['text'], df_review['stars']).dropDuplicates()

# Display counts of reviews by stars
print 'counts of reviews by stars:'
df_review.groupBy(df_review.stars).agg(countDistinct(df_review.text)).show()

# Treat 1, 2, 3 star ratings as 'bad' reviews & 4, 5 star ratings as 'good' reviews
binning_udf = udf(lambda x: 1 if x > 3 else 0)
df_review = df_review.withColumn('label', binning_udf(col('stars')))
# convert label to integer type
df_review = df_review.withColumn("label", df_review["label"].\
                                 cast(IntegerType())).drop('stars')

# Split into training and validation sets
train_data, valid_data = df_review.randomSplit([0.8, 0.2])
train_data.cache()
valid_data.cache()


# Training the Naive Bayes model
# convert the input string to lowercase and split it by spaces
tokenizer = Tokenizer(inputCol="text", outputCol="words")
# create vocabulary (use binary=True for Binarized Multinomial Naive Bayes)
cv = CountVectorizer(inputCol="words", outputCol="features", binary=True)
nb = NaiveBayes(smoothing=1.0, modelType="multinomial")

# Create processing pipeline
pipeline = Pipeline(stages=[tokenizer, cv, nb])
model = pipeline.fit(train_data)
preds_valid = model.transform(valid_data)


#Evaluate the model. default metric : Area Under ROC..... areaUnderROC:0.609271576547
bceval = BinaryClassificationEvaluator()
print (bceval.getMetricName() +":" + str(bceval.evaluate(preds_valid)))

#Evaluate the model. metric : Area Under PR...... areaUnderPR:0.730182016574
bceval.setMetricName("areaUnderPR")
print (bceval.getMetricName() +":" + str(bceval.evaluate(preds_valid)))

#Evaluate the model. metric : F1 score...... f1:0.866084739578
mceval = MulticlassClassificationEvaluator(labelCol="label",
                                           predictionCol="prediction",
                                           metricName="f1")
print (mceval.getMetricName() +":" + str(mceval.evaluate(preds_valid)))

#Evaluate the model. metric : accuracy......  accuracy:0.867096347903
mceval.setMetricName("accuracy")
print (mceval.getMetricName() +":" + str(mceval.evaluate(preds_valid)))


#########
sc.stop()