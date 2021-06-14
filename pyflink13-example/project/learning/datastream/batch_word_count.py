# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         batch_word_count
# Description:
# Author:       orange
# Date:         2021/6/14
# -------------------------------------------------------------------------------
import os
import shutil

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings


# ########################### 初始化批处理环境 ###########################

# 创建 Blink 批处理环境
env = StreamExecutionEnvironment.get_execution_environment()
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
t_env = StreamTableEnvironment.create(env, environment_settings=env_settings)

# 创建 Flink 批处理环境
# env_settings = EnvironmentSettings.new_instance().in_batch_mode().use_old_planner().build()
# t_env = BatchTableEnvironment.create(environment_settings=env_settings)

# ########################### 创建源表(source) ###########################
# source 指数据源，即待处理的数据流的源头，这里使用同级目录下的 word.csv，实际中可能来自于 MySQL、Kafka、Hive 等

dir_word = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'word.csv')
print(dir_word)

# 基于 SQL API
t_env.execute_sql(f"""
    CREATE TABLE source (
        id BIGINT,     -- ID
        word STRING    -- 单词
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file://{dir_word}',
        'format' = 'csv'
    )
""")

# 基于 Table API
# t_env.connect(FileSystem().path(dir_word)) \
#     .with_format(OldCsv()
#                  .field('id', DataTypes.BIGINT())
#                  .field('word', DataTypes.STRING())) \
#     .with_schema(Schema()
#                  .field('id', DataTypes.BIGINT())
#                  .field('word', DataTypes.STRING())) \
#     .create_temporary_table('source')

# 查看数据
t_env.from_path('source').print_schema()  # 查看 schema
t_env.from_path('source').to_pandas()  # 转为 pandas.DataFrame
# result = t_env.from_path('source').to_pandas()
# print(result)

# table_result = t_env.from_path('source').limit(10).execute()
# with table_result.collect() as results:
#     for result in results:
#         print(result)