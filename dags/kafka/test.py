from pymongo import MongoClient
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tag import pos_tag
from nltk.tokenize import word_tokenize
import pickle
import re, string
from textblob import TextBlob
import csv
from datetime import datetime
from autocorrect import Speller
import pandas as pd

def Mongoconnect():
    URL = f"mongodb://host.docker.internal/"
    url = f"mongodb://localhost:27017/"
    client = MongoClient(url)
    db = client.admin
    # Issue the serverStatus command and print the results
    serverStatusResult = db.command("serverStatus")
    print(serverStatusResult)
    return client

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


def remove_noise(tweet_tokens, stop_words = ()):

    cleaned_tokens = []
    spell = Speller(lang='en')

    for token, tag in pos_tag(tweet_tokens):

        token = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+#]|[!*\(\),]| (?:%[0-9a-fA-F][0-9a-fA-F]))+','', token)
        token = re.sub(r"(@[A-Za-z0-9_]+)","", token)
        token = token.strip("\n").strip(" ")
        token = spell(token)


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

def clean_tweet(tweet):

    return ' '.join(re.sub("(^RT)|(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])| (\w+:\ / \ / \S+)", " ", tweet).split())

def get_tweet_sentiment(tweet):

    # create TextBlob object of passed tweet text
    analysis = TextBlob(tweet)
    # set sentiment
    if analysis.sentiment.polarity > 0:
        return 'Positive'
    elif analysis.sentiment.polarity == 0:
        return 'neutral'
    else:
        return 'Negative'

def syntimantal_analysis():
    with open(f'model_pkl', 'rb') as f:
        classifier = pickle.load(f)
    client = Mongoconnect()
    mydb = client["Tweet_database"]
    f = open(f'result_data.csv', 'w+')
    writer = csv.DictWriter(f, fieldnames=["id", "text", "result", "date", "token","nb","pol"], extrasaction='ignore')
    writer.writeheader()

    raw_collection = [i for i in list(mydb.list_collection_names()) if "raw_data" in i]

    for i in raw_collection:
        raw_data = mydb[i]
        for i in raw_data.find():
            review = ""
            i['text'] = token = decontracted(i['text'])
            custom_tokens = remove_noise(word_tokenize(clean_tweet(i["text"])))
            polarity_result = get_tweet_sentiment(clean_tweet(i["text"]))
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

            row = {"id":i["id"],"text":i["text"],"result":review,"date":datetime.today(),"token":custom_tokens,"nb":nb_result,"pol":polarity_result}
            #final_db.insert_one(row)
            print(row)
            writer.writerow(row)

    f.flush()
    f.close()


if __name__ == "__main__":
    text =  """We'll enjoyed the spicy deluxe sandwich Combo and a regular deluxe combo.
It was "just okay".. I doubt we'll be going back. Drive through had good. Knowledgeable employees taking orders.
Effective Jan 5th Ontario has banned indoor restaurant dining for the next 3 weeks"""
    # custom_tokens = remove_noise(word_tokenize(text))
    # with open(f'model_pkl', 'rb') as f:
    #     classifier = pickle.load(f)
    # result = classifier.classify(dict([token, True] for token in custom_tokens))
    # print(remove_noise(word_tokenize(text)))
    # print(get_tweet_sentiment(remove_noise(word_tokenize(text))))
    # print(result)

    syntimantal_analysis()
    #print(remove_noise(word_tokenize(text)))







