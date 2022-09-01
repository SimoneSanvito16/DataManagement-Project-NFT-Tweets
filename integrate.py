import pandas as pd
import tweepy as tw
import pandas as pd
import numpy as np
import pymongo
import json
from pymongo import MongoClient
import re
import gridfs
import json
from bson import json_util


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)


clientNFT = MongoClient('localhost', 27017)
db = clientNFT.DatMan
collectionNFT = db['nft']
collectionNFTTweets = db['nft_tweets']

cursor = collectionNFT.find()
list_cursor = list(cursor)
nftsDataframe = pd.DataFrame(list_cursor)
print(nftsDataframe)

i = 0
for i in (0, 3899):
    if(type(nftsDataframe['Owners'][i]) is str):
        print(nftsDataframe['Owners'][i])
        print(i)
    i = i+1


nftsDataframe = nftsDataframe.astype({"Collection": str})
nftsDataframe["Volume"] = nftsDataframe["Volume"].map(
    lambda x: x.replace('.', ''))
nftsDataframe["Volume"] = nftsDataframe["Volume"].map(
    lambda x: x.replace(',', '.'))
nftsDataframe["Volume"] = pd.to_numeric(nftsDataframe["Volume"])
nftsDataframe["Floor Price"] = nftsDataframe["Floor Price"].map(
    lambda x: '' if x == '---' else x)
nftsDataframe["Floor Price"] = nftsDataframe["Floor Price"].map(
    lambda x: '0.01' if x == '< 0.01' else x)
nftsDataframe["Floor Price"] = pd.to_numeric(nftsDataframe["Floor Price"])
nftsDataframe["Items"] = nftsDataframe["Items"].map(
    lambda x: x.replace('.', ''))
nftsDataframe["Items"] = pd.to_numeric(nftsDataframe["Items"])
# i = 3899 ha valore string per owners, mettiamo tutto in float
nftsDataframe['Owners'] = nftsDataframe['Owners'].astype(float)
nftsDataframe["Owners"] = nftsDataframe["Owners"].map(lambda x: int(x * 1000))

nftsDataframe

# NFT Grid
fs = gridfs.GridFS(db)
tot = 0
for i, record in nftsDataframe.iterrows():
    print("Documento: "+str(i))
    # print(record)
    # break
    document = {
        'Collection': record[1],
        'Volume': record[2],
        'Floor Price': record[3],
        'Owners': record[4],
        'Items': record[5],
    }
    i = i+1
    q = ''.join(e for e in record[1] if e.isalnum() or e == " ")
    q = q.replace(" ", '\s?')
    print(record[1])
    regx = re.compile(q, re.IGNORECASE)
    tweetsCursor = db.tweets.find({"$or": [{"full_text": regx},
                                           {"retweeted_status.full_text": regx},
                                           {"entities.urls": {"$elemMatch": {"expanded_url": regx}}}]})

    # for result in tweetsCursor:
    #    print(result['id_str'])
    # break
    # per gli oggetti troppo grandi

    isGrid = False
    tweets = list(tweetsCursor)
    document['TweetsNum'] = len(tweets)
    document['Tweets'] = tweets
    try:
        document['IsGridFile'] = isGrid
        collectionNFTTweets.insert_one(document)
    except:
        print("File troppo grosso")
        isGrid = True
        document['IsGridFile'] = isGrid
        s = json_util.dumps(tweets)
        res = fs.put(s, encoding="utf-8")
        document['Tweets'] = res
        collectionNFTTweets.insert_one(document)

    tot = tot + len(tweets)

    if(record[1] == "AKC Pets"):
        break


print(tot)


# per prendere le informazioni dei tweets con GridFS
decentraland = collectionNFTTweets.find({'Collection': "Decentraland"})
for id in decentraland:
    array = fs.get(id['Tweets']).read()

array = json.loads(array)
