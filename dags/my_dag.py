from airflow import DAG
from sqlalchemy import create_engine
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from custome_fun import covert_numeric,reformat_data
from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
import configparser
from pymongo import MongoClient
import tweepy
from kafka import KafkaProducer
from datetime import datetime, timedelta
import csv
from autocorrect import Speller
from textblob import TextBlob
dag_path = os.getcwd()

from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tag import pos_tag
from nltk.tokenize import word_tokenize
import pickle

import re, string

import nltk

def remove_noise(tweet_tokens, stop_words = ()):

    cleaned_tokens = []

    for token, tag in pos_tag(tweet_tokens):
        token = re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+#]|[!*\(\),]|'\
                       '(?:%[0-9a-fA-F][0-9a-fA-F]))+','', token)
        token = re.sub("(@[A-Za-z0-9_]+)","", token)
        token = re.sub('RT @\w +: ', " ", token)
        token = re.sub("(@[A-Za-z0–9]+) | ([0-9A-Za-z \t]) | (\w+:\ / \ / \S+)", " ", token)

        token = token.strip("\n").strip(" ")

        if tag.startswith("NN"):
            pos = 'n'
        elif tag.startswith('VB'):
            pos = 'v'
        else:
            pos = 'a'

        lemmatizer = WordNetLemmatizer()
        token = lemmatizer.lemmatize(token, pos)

        if len(token) > 0 and token not in string.punctuation and token.lower() not in stop_words:
            cleaned_tokens.append(token.lower())
    return cleaned_tokens


def decontracted(phrase):
    # specific
    phrase = re.sub(r"won\'t", "will not", phrase)
    phrase = re.sub(r"can\'t", "can not", phrase)

    # general
    phrase = re.sub(r"n\'t", " not", phrase)
    phrase = re.sub(r"\'re", " are", phrase)
    phrase = re.sub(r"\'s", " is", phrase)
    phrase = re.sub(r"\'d", " would", phrase)
    phrase = re.sub(r"\'ll", " will", phrase)
    phrase = re.sub(r"\'t", " not", phrase)
    phrase = re.sub(r"\'ve", " have", phrase)
    phrase = re.sub(r"\'m", " am", phrase)
    return phrase

def clean_tweet(tweet):
    '''
    Utility function to clean tweet text by removing links, special characters
    using simple regex statements.
    '''
    spell = Speller(lang='en')
    return ' '.join(spell(i) for i in re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])| (\w+:\ / \ / \S+)", " ", tweet).split())


def get_tweet_sentiment(tweet):
    '''
    Utility function to classify sentiment of passed tweet
    using textblob's sentiment method
    '''
    # create TextBlob object of passed tweet text
    analysis = TextBlob(tweet)
    # set sentiment
    if analysis.sentiment.polarity > 0:
        return 'positive'
    elif analysis.sentiment.polarity == 0:
        return 'neutral'
    else:
        return 'negative'

def Mongoconnect():
    URL = f"mongodb://host.docker.internal/"
    url = f"mongodb://localhost:27017/"
    client = MongoClient(URL)
    db = client.admin
    # Issue the serverStatus command and print the results
    serverStatusResult = db.command("serverStatus")
    print(serverStatusResult)
    return client

def syntimantal_analysis():
    with open(f'{dag_path}/dags/kafka/model_pkl', 'rb') as f:
        classifier = pickle.load(f)
    client = Mongoconnect()
    mydb = client["Tweet_database"]

    collection_number = len([i for i in list(mydb.list_collection_names()) if "raw_data" in i])
    final_db = mydb.create_collection("senti_analysis"+str(collection_number))
    raw_data = mydb["raw_data_"+str(collection_number)]
    data = []
    f = open(f'{dag_path}/dags/kafka/result_data.csv', 'a+')
    writer = csv.DictWriter(f, fieldnames=["id","text","result","date","token"],extrasaction="ignore")

    for i in raw_data.find():
        review = ""
        i['text'] = token = decontracted(i['text'])
        cleaned_tweet = clean_tweet(i["text"])
        custom_tokens = remove_noise(word_tokenize(cleaned_tweet))
        polarity_result = get_tweet_sentiment(cleaned_tweet)
        if polarity_result != "neutral":
            nb_result = classifier.classify(dict([token, True] for token in custom_tokens))
            if polarity_result != nb_result:
                print(len(custom_tokens))
                nb_result = classifier.classify(dict([token, True] for token in custom_tokens))
                if len(custom_tokens) > 30:
                    review = nb_result
                else:
                    review = polarity_result
            else:
                review = polarity_result
        else:
            review = polarity_result

        row = {"id": i["id"], "text": i["text"], "result": review, "date": datetime.today(), "token": custom_tokens,
               "nb": nb_result, "pol": polarity_result}
        final_db.insert_one(row)
        writer.writerow(row)

    f.flush()
    f.close()
    client.close()












# with DAG(
#     dag_id = "Producer",
#     schedule_interval="@daily",
#     start_date = datetime(2021,11,1),
#     default_args={
#         "owner" : "airflow",
#         "retries":0,
#     },
#     catchup=False
#
# ) as dag2:
#     producer = BashOperator(
#     task_id='kafka_producer',
#     bash_command=f'python {dag_path}/dags/kafka/producer.py',
#     dag=dag2)
#
#
#
# with DAG(
#     dag_id = "Consumer",
#     schedule_interval="@daily",
#     start_date = datetime(2021,11,1),
#     default_args={
#         "owner" : "airflow",
#         "retries":0,
#     },
#     catchup=False
#
# ) as dag1:
#     consumer = BashOperator(
#         task_id='kafka_consumer',
#         bash_command=f'python {dag_path}/dags/kafka/Consumer_kafka.py',
#         dag=dag1)
#
#
#
# with DAG(
#     dag_id = "Analysis",
#     schedule_interval="@daily",
#     start_date = datetime(2021,11,1),
#     default_args={
#         "owner" : "airflow",
#         "retries":0,
#     },
#     catchup=False
#
# ) as dag4:
#     Sentimental = PythonOperator(
#         task_id="Sentimental",
#         python_callable= syntimantal_analysis
#     )

with DAG(
    dag_id = "Analysis",
    schedule_interval="@daily",
    start_date = datetime(2021,11,1),
    default_args={
        "owner" : "airflow",
        "retries":0,
    },
    catchup=False

) as dag5:
    Sentimental = PythonOperator(
        task_id="Sentimental1",
        python_callable= syntimantal_analysis,
        dag=dag5
    )

    consumer = BashOperator(
        task_id='kafka_consumer1',
        bash_command=f'python {dag_path}/dags/kafka/kafka_consumer.py',
        dag=dag5)

    producer = BashOperator(
        task_id='kafka_producer1',
        bash_command=f'python {dag_path}/dags/kafka/producer.py',
        dag=dag5)

producer
consumer >> Sentimental



# globals()["Consumer"] = dag1
# globals()["Producer"] = dag2
# globals()["Analysis"] = dag4

#globals()["main"] = dag5

