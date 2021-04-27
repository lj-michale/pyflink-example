# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         dataStream_api_example003
# Description:
# Author:       orange
# Date:         2021/4/27
# -------------------------------------------------------------------------------

from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


def data_stream_api_demo():

    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    env.set_parallelism(4)

    t_env.execute_sql(
        """
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

    def split(s):
        splits = s[1].split("|")
        for sp in splits:
            yield s[0], sp

    ds = ds.map(lambda i: (i[0] + 1, i[1])) \
           .flat_map(split, Types.TUPLE([Types.INT(), Types.STRING()])) \
           .key_by(lambda i: i[1]) \
           .reduce(lambda i, j: (i[0] + j[0], i[1]))

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

    # 1）等待作业执行结束，用于local执行，否则可能作业尚未执行结束，该脚本已退出，会导致minicluster过早退出
    # 2）当作业通过detach模式往remote集群提交时，比如YARN/Standalone/K8s等，需要移除该方法
    table_result.wait()


if __name__ == '__main__':
    data_stream_api_demo()
