# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         Python_UDTAF
# Description:
# Author:       orange
# Date:         2021/6/3
# -------------------------------------------------------------------------------
from pyflink.common import Row
from pyflink.table import DataTypes, EnvironmentSettings, StreamTableEnvironment
from pyflink.table.udf import udtaf, TableAggregateFunction


class Top2(TableAggregateFunction):
    """
    Function Top2 With PyFlink1.13 UDTAF
    :param
    :return:
    """
    def create_accumulator(self):
        # 存储当前最大的两个值
        return [None, None]

    def accumulate(self, accumulator, input_row):
        if input_row[0] is not None:
            # 新的输入值最大
            if accumulator[0] is None or input_row[0] > accumulator[0]:
                accumulator[1] = accumulator[0]
                accumulator[0] = input_row[0]
            # 新的输入值次大
            elif accumulator[1] is None or input_row[0] > accumulator[1]:
                accumulator[1] = input_row[0]

    def emit_value(self, accumulator):
        yield Row(accumulator[0])
        if accumulator[1] is not None:
            yield Row(accumulator[1])


top2 = udtaf(f=Top2(),
             result_type=DataTypes.ROW([DataTypes.FIELD("a", DataTypes.BIGINT())]),
             accumulator_type=DataTypes.ARRAY(DataTypes.BIGINT()))

if __name__ == '__main__':

    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
    t_env = StreamTableEnvironment.create(environment_settings=env_settings)

    t = t_env.from_elements([(1, 'Hi', 'Hello'),
                             (3, 'Hi', 'hi'),
                             (5, 'Hi2', 'hi'),
                             (2, 'Hi', 'Hello'),
                             (7, 'Hi', 'Hello')],
                            ['a', 'b', 'c'])

    t_env.execute_sql("""
           CREATE TABLE my_sink (
             word VARCHAR,
             `sum` BIGINT
           ) WITH (
             'connector' = 'print'
           )
        """)

    result = t.group_by(t.b).flat_aggregate(top2).select("b, a").execute_insert("my_sink")

    # 1）等待作业执行结束，用于local执行，否则可能作业尚未执行结束，该脚本已退出，会导致minicluster过早退出
    # 2）当作业通过detach模式往remote集群提交时，比如YARN/Standalone/K8s等，需要移除该方法
    result.wait()
