1. Recursively extracted all existing fields from the business collection (business.json) loaded in mongodb.
   Used variety.js for schema analysis (https://github.com/variety/variety)
   Saves the schema analysis result table in business_schema_analysis.txt.
   
2. Ran the queries in write_restaurant_ids.js to generate a list of business ids that are restaurants;
   saved this in all_restaurant_ids.txt.

3. Used clean_restaurant_ids.ipynb to clean all_restaurant_ids.txt 
   and saved final restaurant ids in data/restaurant_ids_final.txt.
   
   
Other:
   eda_mongo.js shows some exploratory querying of the business collection.
   category_list.csv shows a list of all unique categories in the business collection.