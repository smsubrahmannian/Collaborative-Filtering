git p


# Create SparkContext
conf = SparkConf()
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


# Loads parquet file located in AWS S3 into RDD Data Frame
parquetFileTip = sqlContext.read.parquet("s3://dknsyelp/tip.parquet")
# Stores the DataFrame into an "in-memory temporary table"
parquetFileTip.registerTempTable("parquetFileTip")
# Run standard SQL queries against temporary table
df_tip = sqlContext.sql("SELECT * FROM parquetFileTip")

parquetFileReview = sqlContext.read.parquet("s3://dknsyelp/review.parquet")
parquetFileReview.registerTempTable("parquetFileReview")
df_review = sqlContext.sql("SELECT * FROM parquetFileReview")

# Clean data
df_review = df_review.select(df_review['text'], df_review['stars']).dropDuplicates()

# Display counts of reviews by stars
print 'counts of reviews by stars:'
df_review.groupBy(df_review.stars).agg(countDistinct(df_review.text)).show()

# Treat 1, 2, 3 star ratings as 'bad' reviews & 4, 5 star ratings as 'good' reviews
binning_udf = udf(lambda x: 1 if x > 3 else 0)
df_review = df_review.withColumn('label', binning_udf(col('stars')))
# convert label to integer type
df_review = df_review.withColumn("label", df_review["label"]\
                                 .cast(IntegerType())).drop('stars')
# Replace all punctuation
import string
import re
to_remove = string.punctuation + '0-9\\r\\t\\n'
to_remove = r"[{}]".format(to_remove)       # correct format for regex
my_regex = re.compile(to_remove)
# Replace instances of every element in to_remove with empty string
text_clean = udf(lambda text: my_regex.sub('', text))
df_review = df_review.withColumn('clean_text', text_clean(col('text')))\
    .drop('text').withColumnRenamed("clean_text", "text").cache()


# Split into training and validation sets
train_data, valid_data = df_review.randomSplit([0.8, 0.2])
train_data.cache()
valid_data.cache()


# Training the Naive Bayes model
def build_ngrams(n=3):
    # convert the input string to lowercase and split it by spaces
    tokenizer = [Tokenizer(inputCol="text", outputCol="words")]
    ngrams = [NGram(n=i, inputCol="words", outputCol="{0}_grams".format(i)) for i in range(1, n + 1)]
    # create vocabulary (use binary=True for Binarized Multinomial Naive Bayes)
    vectorizers = [CountVectorizer(inputCol="{0}_grams".format(i), outputCol="{0}_counts".format(i), binary=True) for i in range(1, n + 1)]
    # combine all n-grams
    assembler = [VectorAssembler(inputCols=["{0}_counts".format(i) for i in range(1, n + 1)], outputCol="features")]
    nb = [NaiveBayes(smoothing=1.0, modelType="multinomial")]
    return Pipeline(stages=tokenizer + ngrams + vectorizers + assembler + nb)


model = build_ngrams(n=2).fit(train_data)
preds_valid = model.transform(valid_data)


#Evaluate the model. default metric : Area Under ROC..... areaUnderROC:0.609
# with text_clean: 0.607
# with text_clean + build_ngrams(n=2): 0.612
bceval = BinaryClassificationEvaluator()
print (bceval.getMetricName() +":" + str(round(bceval.evaluate(preds_valid)), 3))

#Evaluate the model. metric : Area Under PR...... areaUnderPR:0.732
# with text_clean: 0.728
# with text_clean + build_ngrams(n=2): 0.729
bceval.setMetricName("areaUnderPR")
print (bceval.getMetricName() +":" + str(round(bceval.evaluate(preds_valid)), 3))

#Evaluate the model. metric : F1 score...... f1:0.865
# with text_clean: 0.858
# with text_clean + build_ngrams(n=2): 0.882
mceval = MulticlassClassificationEvaluator(labelCol="label",
                                           predictionCol="prediction",
                                           metricName="f1")
print (mceval.getMetricName() +":" + str(round(mceval.evaluate(preds_valid), 3)))

#Evaluate the model. metric : accuracy......  accuracy:0.866
# with text_clean: 0.859
# with text_clean + build_ngrams(n=2): 0.883
mceval.setMetricName("accuracy")
print (mceval.getMetricName() +":" + str(round(mceval.evaluate(preds_valid), 3)))


#########
sc.stop()