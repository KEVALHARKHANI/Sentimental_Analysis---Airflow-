import time
from json import loads
import csv

from kafka import KafkaConsumer
import logging
import sys
import os
from pymongo import MongoClient
import json
from datetime import datetime

dag_path = os.getcwd()
log_file = f"{dag_path}/logs/Custome_logs/Consumer"

def Mongoconnect():
    URL = f"mongodb://host.docker.internal/"
    url = f"mongodb://localhost:27017/"
    client = MongoClient(URL)
    db = client.admin
    # Issue the serverStatus command and print the results
    serverStatusResult = db.command("serverStatus")
    print(serverStatusResult)
    return client

def get_data_from_kafka(**kwargs):
    logging.basicConfig(filename=f"{log_file}/c_{str(datetime.today())}.txt",
                        format='%(asctime)s %(message)s',
                        filemode='w+')
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    try:
        logger.info("Connecting to mongodb...")
        client = Mongoconnect()
        logger.info("Mgdb Connected")

        mydb = client["Tweet_database"]
        collection_number = len([i for i in list(mydb.list_collection_names()) if "raw_data" in i])

        mycol = mydb.create_collection("raw_data_"+str(collection_number+1))
        logger.info("New collection has been Connected.")
        logger.info("Creating consumer")

        consumer = KafkaConsumer(
            'CFA',
            bootstrap_servers=['kafka:29092'],
            consumer_timeout_ms=3000,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=None)
        consumer.poll(timeout_ms=2000)
        logger.info("Creating has been created.")
    except Exception as e:
        print(e)
        logger.error(e)
        exit(0)







    try:
        flag = False
        logger.info("getting msg from producer")
        for i in range(30):
            time.sleep(1)
            for msg in consumer: # loop over messages

                #message = str(msg.value.decode('utf-8'))# decode JSON

                raw = json.loads(msg.value.decode('utf-8'))
                print(raw)

                #print(message)
                if raw["text"] == "stop-kafka":
                    print("worked")
                    flag = True
                else:
                    print("------appending in list-----")
                    raw['date'] = datetime.datetime.today()
                    x = mycol.insert_one(raw)

            if flag:
                break
        logger.info("data has been collected and stored into mongodb")
        client.close()
        consumer.close()



    except Exception as e:
        print(e)
        client.close()
        consumer.close()
        exit(0)

if __name__ == "__main__":
    get_data_from_kafka()