// preliminary EDA on the 'business' collection
// aim: to be able to identify restaurants out of all businesses

use yelp

// 156639 documents in the 'business' collection
db.business.count()

// # of docs that have the field "categories": 156639, i.e. all docs have categories field
db.business.find( { categories: { $exists: true } } ).count()

// # of docs with non-null categories: 156639, i.e. all docs
db.business.find({categories: {$ne: null}}).count()

// # of distinct terms appearing in "categories"
db.business.distinct("categories").length

// print all unique terms appearing in "categories"
db.business.distinct("categories")

// # of businesses which have the field "RestaurantsTakeOut": 54648
db.business.find( { "attributes.RestaurantsTakeOut": { $exists: true } } ).count()

// # of docs with non-null RestaurantsTakeOut: 54648, i.e. all docs where the field exists
db.business.find( { "attributes.RestaurantsTakeOut": { $ne: null } } ).count()

// # of docs where RestaurantsTakeOut = true: 49291
db.business.find( { "attributes.RestaurantsTakeOut": { $eq: true } } ).count()

// # of docs where RestaurantsTakeOut = false: 5357
// this confirms that only 'true' or 'false' values are present in RestaurantsTakeOut
db.business.find( { "attributes.RestaurantsTakeOut": { $eq: false } } ).count()

// # of categories that appear with "Restaurants": 651
db.business.aggregate([{"$match": {"categories": {"$in": ["Restaurants"]}}}, {$project: {categories:1, _id:0}}, {"$unwind": "$categories"}, {"$group": {"_id": "$categories"}}, { $group: { _id: null, count: { $sum: 1 } } }])

// categories that do not appear with "Restaurants"
db.business.aggregate([{"$match": {"categories": {"$nin": ["Restaurants"]}}}, {$project: {categories:1, _id:0}}, {"$unwind": "$categories"}, {"$group": {"_id": "$categories"}}])


db.business.aggregate({"$match": {"categories": {"$in": ["Restaurants"]}}})

// ------------  Write following 2 query results to file -------------


// find docs with "Restaurants" in categories  (51613 resulting docs)
db.business.find({categories: {"$in": ["Restaurants"]}}).count()



// Get businesses that have the following restaurant related fields: (100315 resulting docs)
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
{"attributes.RestaurantsCounterService" : {$exists: true}}]})
