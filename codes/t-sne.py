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
als = ALS(maxIter=50, regParam=1, userCol="user_ix",nonnegative=True
          ,itemCol="biz_ix", ratingCol="stars", rank =8)

model = als.fit(ratings)

#Embeddings

a = model.itemFactors
b= a.sort("id")
b.show()

#Creating a dense matrix from embedding for businesses
values = (b.rdd.map(lambda x: (x.id, x.features)).sortByKey().flatMap(lambda (x, y): y).collect())

nrow = len(b.rdd.map(lambda x: x.features).first())
ncol = b.count()

dm = DenseMatrix(nrow, ncol, values)
dm.toArray().shape
z=dm.toArray().transpose()

#t-sne

tsne = TSNE(n_components=2)
X_tsne = tsne.fit_transform(z)

# creating data frame with t-sne results and business_id
e = sqlContext.createDataFrame(pd.DataFrame(X_tsne))
e_df = e.toPandas()
j=b.select("id")
j_df =j.toPandas()
result = pd.concat([e_df, j_df], axis=1,ignore_index=True)
result = pd.DataFrame(result)

output_notebook()
plot = bp.figure(plot_width=700, plot_height=600, title="Clustering of the restaurants",
    tools="pan,wheel_zoom,box_zoom,reset,hover,previewsave",
    x_axis_type=None, y_axis_type=None, min_border=1)
## plotting using bookeh

plot.scatter(x='a' , y='b', source = result)
hover = plot.select(dict(type=HoverTool))
hover.tooltips={"description":"@c"}
output_file("output_file_name.html")
show(plot)

