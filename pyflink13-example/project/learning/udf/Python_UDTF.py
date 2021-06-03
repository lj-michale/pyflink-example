# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         Python_UDTF
# Description:
# Author:       orange
# Date:         2021/6/3
# -------------------------------------------------------------------------------

from pyflink.table.udf import udtf
from pyflink.table import DataTypes, EnvironmentSettings, StreamTableEnvironment


@udtf(result_types=[DataTypes.STRING(), DataTypes.STRING()])
def split(s: str, sep: str):
    splits = s.split(sep)
    yield splits[0], splits[1]


if __name__ == '__main__':

    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
    t_env = StreamTableEnvironment.create(environment_settings=env_settings)

    table = t_env.from_elements([("hello|word", 1), ("abc|def", 2)], ['a', 'b'])

    table.join_lateral(split(table.a, '|').alias("c1, c2"))
    table.left_outer_join_lateral(split(table.a, '|').alias("c1, c2"))



