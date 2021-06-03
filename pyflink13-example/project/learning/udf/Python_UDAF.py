# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         Python_UDAF
# Description:
# Author:       orange
# Date:         2021/6/3
# -------------------------------------------------------------------------------

from pyflink.common import Row
from pyflink.table import AggregateFunction, DataTypes, EnvironmentSettings, StreamTableEnvironment
from pyflink.table.udf import udaf


class WeightedAvg(AggregateFunction):

    def create_accumulator(self):
        # Row(sum, count)
        return Row(0, 0)

    def get_value(self, accumulator: Row) -> float:
        if accumulator[1] == 0:
            return 0
        else:
            return accumulator[0] / accumulator[1]

    def accumulate(self, accumulator: Row, value, weight):
        accumulator[0] += value * weight
        accumulator[1] += weight

    def retract(self, accumulator: Row, value, weight):
        accumulator[0] -= value * weight
        accumulator[1] -= weight


weighted_avg = udaf(f=WeightedAvg(),
                    result_type=DataTypes.DOUBLE(),
                    accumulator_type=DataTypes.ROW([
                        DataTypes.FIELD("f0", DataTypes.BIGINT()),
                        DataTypes.FIELD("f1", DataTypes.BIGINT())]))

if __name__ == '__main__':

    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
    t_env = StreamTableEnvironment.create(environment_settings=env_settings)

    t = t_env.from_elements([(1, 2, "Lee"), (3, 4, "Jay"), (5, 6, "Jay"), (7, 8, "Lee")],
                            ["value", "count", "name"])

    t.group_by(t.name).select(weighted_avg(t.value, t.count).alias("avg"))
