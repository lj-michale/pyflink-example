# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         table_api_example001
# Description:
# Author:       orange
# Date:         2021/4/27
# -------------------------------------------------------------------------------
from pyflink.table import DataTypes, EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import col
from pyflink.table.udf import udf

def table_api_demo():
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
    t_env = StreamTableEnvironment.create(environment_settings=env_settings)
    t_env.get_config().get_configuration().set_string('parallelism.default', '4')

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

    @udf(result_type=DataTypes.STRING())
    def sub_string(s: str, begin: int, end: int):
        return s[begin:end]

    transformed_tab = tab.select(sub_string(col('a'), 2, 4))

    t_env.execute_sql("""
            CREATE TABLE my_sink (
              `sum` VARCHAR
            ) WITH (
              'connector' = 'print'
            )
        """)

    table_result = transformed_tab.execute_insert('my_sink')

    # 1）等待作业执行结束，用于local执行，否则可能作业尚未执行结束，该脚本已退出，会导致minicluster过早退出
    # 2）当作业通过detach模式往remote集群提交时，比如YARN/Standalone/K8s等，需要移除该方法
    table_result.wait()


if __name__ == '__main__':
    table_api_demo()