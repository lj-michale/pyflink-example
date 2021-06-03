# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         stream_predict
# Description:
# Author:       orange
# Date:         2021/6/3
# -------------------------------------------------------------------------------

import pickle
import pandas as pd
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes
from pyflink.table.udf import udf
from pyflink.common.typeinfo import Types

env = StreamExecutionEnvironment.get_execution_environment()
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
t_env = StreamTableEnvironment.create(env, environment_settings=env_settings)

kafka_source_ddl = """ 
    create table kafka_source ( 
     X FLOAT
    ) with ( 
      'connector' = 'kafka', 
      'topic' = 'myTopic',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'myGroup',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'csv'
    )
"""

t_env.execute_sql(kafka_source_ddl)

with open('model.pickle', 'rb') as f:
    clf = pickle.load(f)


@udf(input_types=DataTypes.FLOAT(), result_type=DataTypes.FLOAT())
def predict(X):
    X = pd.Series([X]).values.reshape(-1, 1)
    y_pred = clf.predict(X)
    return y_pred


t_env.create_temporary_function('predict', predict)

result = t_env.from_path('kafka_source').select('X, predict(X) as y_pred')
data = t_env.to_append_stream(result, Types.ROW([Types.FLOAT(), Types.FLOAT()]))
data.print()

env.execute('stream predict job')



