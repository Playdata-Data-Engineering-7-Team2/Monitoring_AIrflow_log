import findspark
findspark.init()

# PySpark 관련 패키지 import
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.pandas as ps

# api 처리 패키지 import
import requests
import json

# General 패키지 import
import configparser
import os
import pandas as pd 
from pathlib import Path
from datetime import datetime
from datetime import datetime, timedelta
from pandas.io.json import json_normalize 

config = configparser.ConfigParser()
config.read(Path("./config/config.ini"),encoding='utf-8') # == config.read(os.getcwd()+os.sep+'config'+os.sep+'config.ini',encoding='utf-8')

user = config['dev_mysql']['user']
password = config['dev_mysql']['password']
host = config['dev_mysql']['host']
port = config['dev_mysql']['port']
dbname = config['dev_mysql']['dbname']
url = config['dev_mysql']['url'].format(host=host,port=port,dbname=dbname)
service_key = config['api']['service_key']
bucket = config['aws_s3']['bucket']
aws_access_key_id = config['aws_s3']['aws_access_key_id']
aws_secret_access_key = config['aws_s3']['aws_secret_access_key']


# obs_post_data = pd.read_csv("./관측소 정보.csv")
obs_post_data= spark.read.format('jdbc').options(
    url=url,
    driver='com.mysql.jdbc.Driver',
    dbtable='obs_info',
    user=user,
    password=password).load()
obs_post_data.show(5)

obs_info_df = obs_post_data.select(obs_post_data.obs_post_id,obs_post_data.obs_post_name,obs_post_data.obs_lat,obs_post_data.obs_lon,split(col("obs_object"), ",").alias("obs_object_array"),obs_post_data.data_type).drop("obs_object")
obs_info_df = obs_info_df.sort(col("obs_object_array"),col("obs_post_id"))
obs_info_df.show(5)

tide_obs_post_list = obs_info_df.filter(col("data_type").contains("조위관측소")).select('obs_post_id').rdd.flatMap(lambda x: x).collect()
tidalbu_obs_post_list = obs_info_df.filter(col("data_type").contains("해양관측부이")).select('obs_post_id').rdd.flatMap(lambda x: x).collect()

for obs_code in tidalbu_obs_post_list:
    url = "http://www.khoa.go.kr/api/oceangrid/buObsRecent/search.do?ServiceKey={service_key}&ObsCode={obs_code}&ResultType=json".format(service_key=service_key, obs_code=obs_code)
    response = requests.get(url).json()
    data = response["result"]["data"]
    pdf = pd.json_normalize(data)
    df = spark.createDataFrame(pdf)
    df = df.withColumn('record_time', df['record_time'].cast("timestamp"))
    # with open('./datalake/'+obs_code+'.json', 'w', encoding='utf-8') as f:
    #     json.dump(response, f, ensure_ascii=False, indent=4)
    
for obs_code in tide_obs_post_list:
    url = "http://www.khoa.go.kr/api/oceangrid/tideObsRecent/search.do?ServiceKey={service_key}&ObsCode={obs_code}&ResultType=json".format(service_key=service_key, obs_code=obs_code)
    response = requests.get(url).json()
    data = response["result"]["data"]
    pdf = pd.json_normalize(data)
    df = spark.createDataFrame(pdf)
    df = df.withColumn('record_time', df['record_time'].cast("timestamp"))
    # with open('./datalake/'+obs_code+'.json', 'w', encoding='utf-8') as f:
    #     json.dump(response, f, ensure_ascii=False, indent=4)
    
now_datetime=datetime.now(timezone('Asia/Seoul')).strftime('%Y.%m.%d')
file_name = os.getcwd()+os.sep+'datalake'+os.sep+now_datetime+'_'+obs_code+".json"

spark.stop()
