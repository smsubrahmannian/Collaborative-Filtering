use yelp

DBQuery.shellBatchSize = 5000000

// print all business ids which are have restaurants in their categories field
db.business.find({categories: {"$in": ["Restaurants"]}}, {business_id:1, _id:0})

// Get businesses that have the following restaurant related fields: (100315 resulting docs)
// This list of fields was selected by examining the business collection schema analysis output
// available in business_schema_analysis.txt.
db.business.find({$or: [{"attributes.RestaurantsPriceRange2": {$exists: true}}, 
{"attributes.RestaurantsTakeOut" : {$exists: true}}, 
{"attributes.RestaurantsGoodForGroups" : {$exists: true}},
{"attributes.RestaurantsDelivery" : {$exists: true}},
{"attributes.RestaurantsReservations" : {$exists: true}},
{"attributes.RestaurantsAttire" : {$exists: true}},
{"attributes.GoodForMeal" : {$exists: true}},
{"attributes.RestaurantsTableService" : {$exists: true}},
{"attributes.DogsAllowed" : {$exists: true}},
{"attributes.HappyHour" : {$exists: true}},
{"attributes.DriveThru" : {$exists: true}},
{"attributes.RestaurantsCounterService" : {$exists: true}}]}, {business_id:1, _id:0})