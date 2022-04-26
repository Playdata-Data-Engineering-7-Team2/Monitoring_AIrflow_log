import datetime as dt
import re
import json
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch import helpers

# ES 적재 함수
def bulk_insert(host, port, df, index):
    es_url = 'http://{host}:{port}'.format(host=host,port=port)
    es = Elasticsearch(es_url)
    data = [
      {
        "_index": index,
        "_source": {
            "datetime": x[0],
            "log-level": x[1],
            "message":x[2]}
      }
        for x in zip(df['datetime'],df['log-level'],df['message'])
    ]
    helpers.bulk(es, data)

# log 파싱 함수
def parsing(logs):
    import re
    # 정규표현식
    regex = r"\[(\d+-\d+-\d+) (\d+:\d+:\d+,\d+)\] \{\S+\} (DEBUG|INFO|WARN|FATAL|ERROR|TRACE)(?s)(.*)"
    # match 데이터 찾기
    matches = re.finditer(regex, logs, re.MULTILINE)
    #matches = re.finditer(regex, logs, re.DOTALL)
    dict_list = []
    for matchNum, match in enumerate(matches):
        # Timestamp or Status or Message에 하나라도 값이 없으면 제거
        if match.group(1) and match.group(3) and  match.group(4):
            row_ = dict()
            row_["datetime"]= match.group(1)+" "+match.group(2)[:-4]
            row_["log-level"] = match.group(3)
            row_["message"] = match.group(4)
            dict_list.append(row_)
    return dict_list

# Main 함수
def main(log):
    # debug 용도
    # print(log)

    # 로그 파싱
    parseData = parsing(log)

    # debug 용도
    # print(parseData)

    # pandas DataFrame 변경
    df = pd.DataFrame(parseData)

    # datetime 형식 지정
    df["datetime"] = pd.to_datetime(df["datetime"],format="%Y-%m-%d %H:%M:%S", errors = 'coerce')

    # ES 적재
    index_name = 'airflow-log_' + dt.datetime.now().strftime('%Y-%m-%d')
    # bulk_insert(df, index_name) # host, port, data, index
    bulk_insert("hadoop01", "9200", df, index_name) # host, port, data, index
    print(df.head())

from kafka import KafkaConsumer
from json import loads

brokers = ["hadoop02:9092","hadoop03:9092"]
consumer = KafkaConsumer(
    "airflow-log",
    bootstrap_servers=brokers,
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

for message in consumer:
    message = message.value
    main(message['message'])
