# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         State_Example001
# Description:  state 功能
# Author:       orange
# Date:         2021/6/9
# -------------------------------------------------------------------------------

from pyflink.common import WatermarkStrategy, Row
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import NumberSequenceSource
from pyflink.datastream.functions import RuntimeContext, MapFunction
from pyflink.datastream.state import ValueStateDescriptor


class MyMapFunction(MapFunction):

    def open(self, runtime_context: RuntimeContext):
        state_desc = ValueStateDescriptor('cnt', Types.LONG())
        # 定义value state
        self.cnt_state = runtime_context.get_state(state_desc)

    def map(self, value):
        cnt = self.cnt_state.value()
        if cnt is None:
            cnt = 0

        new_cnt = cnt + 1
        self.cnt_state.update(new_cnt)
        return value[0], new_cnt


def state_access_demo():

    # 1. 创建 StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()

    # 2. 创建数据源
    seq_num_source = NumberSequenceSource(1, 100)
    ds = env.from_source(
        source=seq_num_source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name='seq_num_source',
        type_info=Types.LONG())

    # 3. 定义执行逻辑
    ds = ds.map(lambda a: Row(a % 4, 1), output_type=Types.ROW([Types.LONG(), Types.LONG()])) \
           .key_by(lambda a: a[0]) \
           .map(MyMapFunction(), output_type=Types.TUPLE([Types.LONG(), Types.LONG()]))

    # 4. 将打印结果数据
    ds.print()

    # 5. 执行作业
    env.execute()


if __name__ == '__main__':

    state_access_demo()
