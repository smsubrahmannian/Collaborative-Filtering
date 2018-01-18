# Collaborative-Filtering

Here we are trying to implement Explicit Collaborative Filtering.

We have prepared a data pipeline to process input data.

We have a built recommender system.

s3 file storage has the following final preprocessed files in parquet format
1. ALS_baseline - user_ix, biz_ix, stars
2. bizMap - business_id, bis_ix
3. userMap- user_id,user_ix
4. tip- all columns of tip with user_ix and biz_ix
5. review - all columns of review with user_ix and biz_ix
