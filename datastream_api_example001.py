# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         datastream_api_example001
# Description:
# Author:       orange
# Date:         2021/4/27
# -------------------------------------------------------------------------------
from pyflink.common import Row
from pyflink.common.serialization import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.table import StreamTableEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(4)

# 创建数据源
# 接下来，需要为作业创建一个数据源。PyFlink 中提供了多种方式来定义数据源。
# 方式一：from_collection
# PyFlink 支持用户从一个列表创建源表。
# 以下示例定义了包含了 3 行数据的表：[(1, 'aaa|bb'), (2, 'bb|a'), (3, 'aaa|a')]，该表有 2 列，列名分别为 a 和 b，类型分别为 VARCHAR 和 BIGINT。

ds = env.from_collection(
        collection=[(1, 'aaa|bb'), (2, 'bb|a'), (3, 'aaa|a')],
        type_info=Types.ROW([Types.INT(), Types.STRING()]))

# 方式二：使用 PyFlink DataStream API 中定义的 connector
# 此外，也可以使用 PyFlink DataStream API 中已经支持的 connector，需要注意的是，1.12 中仅提供了 Kafka connector 的支持。
deserialization_schema = JsonRowDeserializationSchema.builder() \
    .type_info(type_info=Types.ROW([Types.INT(), Types.STRING()])).build()

kafka_consumer = FlinkKafkaConsumer(
    topics='test_source_topic',
    deserialization_schema=deserialization_schema,
    properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'})

ds = env.add_source(kafka_consumer)

# 方式三：使用 PyFlink Table API 中定义的 connector
# 以下示例定义了如何将 Table & SQL 中支持的 connector 用于 PyFlink DataStream API 作业。
t_env = StreamTableEnvironment.create(stream_execution_environment=env)

t_env.execute_sql("""
        CREATE TABLE my_source (
          a INT,
          b VARCHAR
        ) WITH (
          'connector' = 'datagen',
          'number-of-rows' = '10'
        )
    """)

ds = t_env.to_append_stream(
    t_env.from_path('my_source'),
    Types.ROW([Types.INT(), Types.STRING()]))

# ###################### 定义计算逻辑
# 生成数据源对应的 DataStream 对象之后，接下来就可以使用 PyFlink DataStream API 中定义的各种操作，定义计算逻辑，对 DataStream 对象进行变换了，比如：


def split(s):
    splits = s[1].split("|")
    for sp in splits:
       yield s[0], sp


ds = ds.map(lambda i: (i[0] + 1, i[1])) \
       .flat_map(split) \
       .key_by(lambda i: i[1]) \
       .reduce(lambda i, j: (i[0] + j[0], i[1]))


# 写出结果数据
# 方式一：print
# 可以调用 DataStream 对象上的 print 方法，将 DataStream 的结果打印到标准输出中，比如：
ds.print()

# 方式二：使用 PyFlink DataStream API 中定义的 connector
# 可以直接使用 PyFlink DataStream API 中已经支持的 connector，需要注意的是，1.12 中提供了对于 FileSystem、JDBC、Kafka connector 的支持，以 Kafka 为例：
serialization_schema = JsonRowSerializationSchema.builder() \
    .with_type_info(type_info=Types.ROW([Types.INT(), Types.STRING()])).build()

kafka_producer = FlinkKafkaProducer(
    topic='test_sink_topic',
    serialization_schema=serialization_schema,
    producer_config={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'})

ds.add_sink(kafka_producer)

# 方式三：使用 PyFlink Table API 中定义的 connector
# 以下示例展示了如何将 Table & SQL 中支持的 connector，用作 PyFlink DataStream API 作业的 sink。
# 写法一：ds类型为Types.ROW


def split(s):
    splits = s[1].split("|")
    for sp in splits:
        yield Row(s[0], sp)


ds = ds.map(lambda i: (i[0] + 1, i[1])) \
       .flat_map(split, Types.ROW([Types.INT(), Types.STRING()])) \
       .key_by(lambda i: i[1]) \
       .reduce(lambda i, j: Row(i[0] + j[0], i[1]))

# 写法二：ds类型为Types.TUPLE


def split(s):
    splits = s[1].split("|")
    for sp in splits:
        yield s[0], sp


ds = ds.map(lambda i: (i[0] + 1, i[1])) \
       .flat_map(split, Types.TUPLE([Types.INT(), Types.STRING()])) \
       .key_by(lambda i: i[1]) \
       .reduce(lambda i, j: (i[0] + j[0], i[1]))

# 将ds写出到sink
t_env.execute_sql("""
        CREATE TABLE my_sink (
          a INT,
          b VARCHAR
        ) WITH (
          'connector' = 'print'
        )
    """)

table = t_env.from_data_stream(ds)
table_result = table.execute_insert("my_sink")



