FROM apache/airflow:2.0.1

ADD dags/kafka/Create_topic.sh .
COPY requirements.txt .

RUN pip install -r requirements.txt
RUN pip install kafka-python
RUN pip install python-twitter
RUN pip install tweepy

RUN pip install requests
RUN pip uninstall  --yes azure-storage && pip install -U azure-storage-blob apache-airflow-providers-microsoft-azure==1.1.0
RUN pip install pymongo
RUN pip install nltk
RUN pip install textblob
RUN pip install autocorrect
RUN [ "python3", "-c", "import nltk; nltk.download('all')" ]

CMD [ "./Create_topic.sh" ]
