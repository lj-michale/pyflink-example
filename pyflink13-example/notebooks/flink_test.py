from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.udf import ScalarFunction, udf, udaf
from pyflink.table import AggregateFunction, DataTypes, ListView
from pyflink.table.window import Slide
from pyflink.table.expressions import col, lit
from pyflink.common import Row
import pandas as pd

kafka_servers = ""#localhost
kafka_consumer_group_id = ""
source_topic = ""

#kafka数据格式,五分钟一批
'''
{
  "device_name": "",
  "ps_key": "test_a",
  "time_stamp": "2021-07-28 08:20:00",
  "ps_id": "test",
  "result_data": {
    "a": "2.59",
    "b": "2.52",
    "c": "2.41",
    "d": "24.0"
  },
  "device_type": "1",
  "result_code": "1"
}
'''

env = StreamExecutionEnvironment.get_execution_environment()
env.set_max_parallelism(1)
env.set_parallelism(1)
env_settings = EnvironmentSettings.new_instance().in_streaming_mode(
).use_blink_planner().build()
t_env = StreamTableEnvironment.create(env, environment_settings=env_settings)
t_env.execute_sql(f"""
CREATE TABLE source (
    device_name STRING,
    ps_key STRING,
    ps_id STRING,
    result_data MAP<STRING,STRING>,
    device_type STRING,
    result_code STRING,
    time_stamp TIMESTAMP(3),
    WATERMARK FOR time_stamp AS time_stamp 
    
) with (
    'connector' = 'kafka',
    'topic' = '{source_topic}',
    'properties.bootstrap.servers' = '{kafka_servers}',
    'properties.group.id' = '{kafka_consumer_group_id}',
    'scan.startup.mode' = 'latest-offset',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true',
    'format' = 'json'
)
""")

t_env.execute_sql(f"""
CREATE TABLE sink (
    ps_key STRING,
    start_time TIMESTAMP(3),   -- 窗口开始时间  
    end_time TIMESTAMP(3),       -- 窗口结束时间
    sys_ts TIMESTAMP(3) , 
    example_data STRING

) with (
    'connector' = 'print'
)
""")


#定义聚合函数
class test_function(AggregateFunction):
    def create_accumulator(self):
        return Row(ListView(), '')

    def accumulate(self, accumulator, result_data):
        accumulator[0].add(result_data)

    def get_value(self, accumulator):
        return accumulator[1].join(accumulator[0])

    def get_result_type(self):
        return DataTypes.STRING()

    def get_accumulator_type(self):
        return DataTypes.ROW([
            # declare the first column of the accumulator as a string ListView.
            DataTypes.FIELD(
                "f0",
                DataTypes.LIST_VIEW(
                    DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))),
            DataTypes.FIELD("f1", DataTypes.BIGINT())
        ])


t_env.create_temporary_function("test_func", test_function())
table_source = t_env.from_path('source')
slide_window = Slide.over(lit(15).minutes).every(lit(5).minutes).on(
    col('time_stamp')).alias("w")
result = table_source.filter(col('device_type')=='1')\
                     .window(slide_window)\
                     .group_by(col('w'),col('ps_key'))\
                     .select("ps_key,w.start as start_time,w.end as end_time,w.proctime,test_func(result_data)")\
                    .insert_into('sink')

t_env.execute('pyflink')
