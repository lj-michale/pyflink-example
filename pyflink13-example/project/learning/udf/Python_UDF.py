# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         Python_UDF
# Description:
# Author:       orange
# Date:         2021/6/3
# -------------------------------------------------------------------------------
import functools

from pyflink.table.udf import udf, FunctionContext, ScalarFunction
from pyflink.table import DataTypes, EnvironmentSettings, StreamTableEnvironment


# 方式一：
@udf(result_type=DataTypes.STRING())
def sub_string(s: str, begin: int, end: int):
    return s[begin:end]


# 方式二:
sub_string = udf(lambda s, begin, end: s[begin:end], result_type=DataTypes.STRING())


# 方式三：
class SubString(object):
    def __call__(self, s: str, begin: int, end: int):
        return s[begin:end]


sub_string = udf(SubString(), result_type=DataTypes.STRING())


# 方式四：
def sub_string(s: str, begin: int, end: int):
    return s[begin:end]


sub_string_begin_1 = udf(functools.partial(sub_string, begin=1), result_type=DataTypes.STRING())


# 方式五：
class SubString(ScalarFunction):
    def open(self, function_context: FunctionContext):
        pass

    def eval(self, s: str, begin: int, end: int):
        return s[begin:end]


sub_string = udf(SubString(), result_type=DataTypes.STRING())


if __name__ == '__main__':

    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
    t_env = StreamTableEnvironment.create(environment_settings=env_settings)

    table = t_env.from_elements([("hello", 1), ("world", 2), ("flink", 3)], ['a', 'b'])
    table.select(sub_string(table.a, 1, 3)).p






