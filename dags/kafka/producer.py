# import library
import tweepy
from kafka import KafkaProducer

from datetime import datetime, timedelta
import os
import configparser
import time
import json
from bson import json_util
import logging

# Twitter Consumer and Access


class Producer:
    config = configparser.ConfigParser(interpolation=None)
    config.read("/opt/airflow/dags/kafka/config.ini")
    consumer_key = config["twitter"]["consumer_key"]
    consumer_secret = config['twitter']["consumer_secret"]
    access_token = config['twitter']["access_token"]
    access_token_secret = config['twitter']["access_token_secret"]
    bearer_token = config['twitter']['bearer_token']
    dag_path = os.getcwd()
    log_file = f"{dag_path}/logs/Custome_logs/Producer"

    # setup authentication
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # instantiation API
    api = tweepy.API(auth, wait_on_rate_limit=True)

    def getClient(self):
        client = tweepy.Client(bearer_token=self.bearer_token,
                               consumer_key=self.consumer_key,
                               consumer_secret=self.consumer_secret,
                               access_token=self.access_token,
                               access_token_secret=self.access_token_secret)
        return client

    def create_params(slef,keyword, start_date, end_date, max_results=100):
        query_params = {'query': keyword,
                        'start_time': start_date,
                        'end_time': end_date,
                        'max_results': max_results,
                        'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                        'tweet.fields': 'id,text,author_id,geo,created_at,public_metrics,referenced_tweets',
                        'user.fields': 'id,name,username,public_metrics,verified',
                        'place.fields': 'id,country,country_code,geo,name,place_type',
                        'next_token': {}}

        return query_params

    # Adjust time to local time (GMT +7 / UTC +7)
    def normalize_time(self,time):
        mytime = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
        mytime += timedelta(hours=7)
        return (mytime.strftime("%Y-%m-%d %H:%M:%S"))

    # instantiation of Kafka Producer
    def kafka_producer(self):
        logging.basicConfig(filename=f"{self.log_file}/p_{str(datetime.today())}.txt",
                            format='%(asctime)s %(message)s',
                            filemode='w')
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)



        try:
            producer = KafkaProducer(bootstrap_servers=["kafka:29092"], api_version=(0, 10, 1))
            logger.info("kafka started")

        except Exception as e:
            logger.error(e)
            exit(0)

        # Topic initialization, keyword, and max query
        topic_name = 'CFA'
        search_key = "Chick fil a Kitchener"
        maxId = -1
        maxTweets = 10





        #Looping to fetch tweet by using twitter API until specified limit
        client = self.getClient()
        print(client.get_me())
        query_para = self.create_params("chick-fil-a", None, datetime.today() - timedelta(days=1))
        logger.info("getting data")
        data = client.search_recent_tweets(query=query_para['query'], max_results=query_para['max_results'],
                                           end_time=query_para['end_time'])
        logger.info("data has been collected")

        logger.info("sending data to consumer")
        for i in data[0]:
            msg = i.data["id"] + ";" + i.data["text"]
            print(msg)
            producer.send(topic_name,  json.dumps(i.data, default=json_util.default).encode('utf-8'))
            time.sleep(1)

        producer.send(topic_name, json.dumps({"text":"stop-kafka"}, default=json_util.default).encode('utf-8'))
        logger.info("data has been send to consumer")
        producer.flush()
        producer.close()






if __name__ == "__main__":
    producer = Producer()
    producer.kafka_producer()