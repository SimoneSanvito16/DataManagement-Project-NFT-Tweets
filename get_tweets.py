import os
import time
import tweepy as tw
import pandas as pd
import numpy as np
import pymongo
import json
from pymongo import MongoClient


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)

bearer_token = np.NaN
consumer_key = np.NaN
consumer_secret = np.NaN
access_token = np.NaN
access_token_secret = np.NaN


auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)


client = tw.Client(bearer_token=bearer_token, consumer_key=consumer_key, consumer_secret=consumer_secret,
                   access_token=access_token, access_token_secret=access_token_secret)

#nftsDataframe = pd.read_json("nft.json", orient="index")
#nomeNFT = nftsDataframe['Collection'].to_numpy()


def fromTweetsToDF(tweets):
    testo = list()
    id = list()
    for tweet in tweets.data:
        testo.append(tweet.text)
        id.append(tweet.id)

    data_vuoto = []
    df = pd.DataFrame(data_vuoto)

    df['id'] = id
    df['text'] = testo

    return df


def collectionDone(col):
    fileObject = open("dataDone.json", "r")
    jsonContent = fileObject.read()
    nftList = json.loads(jsonContent)

    if col in nftList:
        return True
    else:
        return False


def addCollection(col):
    fileObject = open("dataDone.json", "r")
    jsonContent = fileObject.read()
    nftList = json.loads(jsonContent)
    nftList.append(col)
    jsonString = json.dumps(nftList)
    jsonFile = open("dataDone.json", "w")
    jsonFile.write(jsonString)
    jsonFile.close()


def getTweets(query):
    q = ''.join(e for e in query if e.isalnum())
    q = q.replace("  ", ' ')
    tweets = client.search_recent_tweets(query=q, max_results=100)
    currentNFTdataset = fromTweetsToDF(tweets)
    print("Presi i tweet di "+query)
    print(tweets.meta)
    for _ in range(9):
        if 'next_token' in tweets.meta:
            tweets = client.search_recent_tweets(
                query=q, max_results=100, next_token=tweets.meta['next_token'])
            df = fromTweetsToDF(tweets)
            currentNFTdataset = currentNFTdataset.append(df, ignore_index=True)
        else:
            break

        print("Presi i tweet di "+query)
    return currentNFTdataset


i = 0
clientNFT = MongoClient('localhost', 27017)
dbNFT = clientNFT.DatMan
collectionNFT = dbNFT['nft']

dbTweets = clientNFT.DatMan
collectionTweets = dbTweets['tweets']


for document in list(collectionNFT.find({})):
    if collectionDone(document["Collection"]):
        print(document["Collection"] + " già inserita")
    else:
        try:

            dataframe = getTweets(document['Collection'])

            for index, row in dataframe.iterrows():
                try:
                    status = api.get_status(row['id'],  tweet_mode="extended")
                    res = collectionTweets.insert_one(status._json)
                except Exception as e:
                    print(e)

            addCollection(document['Collection'])

        except Exception as e:
            print(e)
            if str(e) == "429 Too Many Requests":

                print("TIMEOUT 15 minuti")

                time.sleep(15*60)

                dataframe = getTweets(document['Collection'])

                for index, row in dataframe.iterrows():
                    try:
                        status = api.get_status(
                            row['id'],  tweet_mode="extended")
                        res = collectionTweets.insert_one(status._json)
                    except Exception as e:
                        print(e)

                addCollection(document['Collection'])

            print("ERRORE")

        print("Creato collection numero: "+str(i))

    i += 1
