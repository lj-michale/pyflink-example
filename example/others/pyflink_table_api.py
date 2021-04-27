# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         pyflink_table_api
# Description:
# Author:       orange
# Date:         2021/4/27
# -------------------------------------------------------------------------------
from pyflink.common.serialization import SimpleStringEncoder
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import StreamingFileSink
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.catalog import HiveCatalog
from pyflink.table.expressions import col
from pyflink.table.udf import udf

env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
t_env = StreamTableEnvironment.create(environment_settings=env_settings)

t_env.get_config().get_configuration().set_string('parallelism.default', '4')

# ####################### 创建数据源表
# 方式一：from_elements
tab = t_env.from_elements([("hello", 1), ("world", 2), ("flink", 3)], ['a', 'b'])
# 方式二：DDL
t_env.execute_sql("""
        CREATE TABLE my_source (
          a VARCHAR,
          b VARCHAR
        ) WITH (
          'connector' = 'datagen',
          'number-of-rows' = '10'
        )
    """)
tab = t_env.from_path('my_source')

"""
当前仅有部分 connector 的实现包含在 Flink 官方提供的发行包中，比如 FileSystem，DataGen、Print、BlackHole 等，
大部分 connector 的实现当前没有包含在 Flink 官方提供的发行包中，比如 Kafka、ES 等。针对没有包含在 Flink 官方提供的发行包中的 connector，
如果需要在 PyFlink 作业中使用，用户需要显式地指定相应 FAT JAR，比如针对 Kafka，需要使用 JAR 包 [2]，JAR 包可以通过如下方式指定：
"""
# 注意：file:///前缀不能省略
t_env.get_config().get_configuration().set_string("pipeline.jars", "file:///my/jar/path/flink-sql-connector-kafka_2.11-1.12.0.jar")

# 方式三：catalog
hive_catalog = HiveCatalog("hive_catalog")
t_env.register_catalog("hive_catalog", hive_catalog)
t_env.use_catalog("hive_catalog")
# 假设hive catalog中已经定义了一个名字为source_table的表
tab = t_env.from_path('source_table')

# ######################################## 定义作业的计算逻辑
# 方式一：通过 Table API
# 得到 source 表之后，接下来就可以使用 Table API 中提供的各种操作，定义作业的计算逻辑，对表进行各种变换，比如：
@udf(result_type=DataTypes.STRING())
def sub_string(s: str, begin: int, end: int):
   return s[begin:end]


transformed_tab = tab.select(sub_string(col('a'), 2, 4))

# 方式二：通过 SQL 语句
# 除了可以使用 Table API 中提供的各种操作之外，也可以直接通过 SQL 语句来对表进行变换，比如上述逻辑，也可以通过 SQL 语句来实现：
t_env.create_temporary_function("sub_string", sub_string)
transformed_tab = t_env.sql_query("SELECT sub_string(a, 2, 4) FROM %s" % tab)

# 5）查看执行计划
# 用户在开发或者调试作业的过程中，可能需要查看作业的执行计划，可以通过如下方式。
# 方式一：Table.explain
# 比如，当我们需要知道 transformed_tab 当前的执行计划时，可以执行：print(transformed_tab.explain())，得到如下输出：

# 方式二：TableEnvironment.explain_sql
# 方式一适用于查看某一个 table 的执行计划，有时候并没有一个现成的 table 对象可用，比如：
print(t_env.explain_sql("INSERT INTO my_sink SELECT * FROM %s " % transformed_tab))

# 6）写出结果数据
# 方式一：通过 DDL
# 和创建数据源表类似，也可以通过 DDL 的方式来创建结果表。
t_env.execute_sql("""
        CREATE TABLE my_sink (
          `sum` VARCHAR
        ) WITH (
          'connector' = 'print'
        )
    """)

table_result = transformed_tab.execute_insert('my_sink')

# 方式二：collect
# 也可以通过 collect 方法，将 table 的结果收集到客户端，并逐条查看。
table_result = transformed_tab.execute()
with table_result.collect() as results:
    for result in results:
        print(result)

# 方式三：to_pandas
# 也可以通过 to_pandas 方法，将 table 的结果转换成 pandas.DataFrame 并查看。
result = transformed_tab.to_pandas()
print(result)























