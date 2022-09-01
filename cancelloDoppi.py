from pymongo import MongoClient

db = MongoClient()['DatMan']

duplicates = []

cursor = db.tweets.aggregate([
    {"$group": {
        "_id": {"id_str": "$id_str"},
        "dups": {"$addToSet": "$_id"},
        "count": {"$sum": 1}
    }},
    {"$match": {
        "count": {"$gt": 1}
    }}
],
    allowDiskUse=True
)


for doc in cursor:
    del doc['dups'][0]
    for dupId in doc['dups']:
        duplicates.append(dupId)


print(duplicates)


db.tweets.delete_many({"_id": {"$in": duplicates}})
