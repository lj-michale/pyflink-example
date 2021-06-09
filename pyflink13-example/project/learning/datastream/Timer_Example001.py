# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         Timer_Example001
# Description:
# Author:       orange
# Date:         2021/6/9
# -------------------------------------------------------------------------------
import datetime

from pyflink.common import Row, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.table import StreamTableEnvironment


class CountWithTimeoutFunction(KeyedProcessFunction):

    def __init__(self):
        self.state = None

    def open(self, runtime_context: RuntimeContext):
        self.state = runtime_context.get_state(ValueStateDescriptor(
            "my_state", Types.ROW([Types.STRING(), Types.LONG(), Types.LONG()])))

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        # retrieve the current count
        current = self.state.value()
        if current is None:
            current = Row(value.f1, 0, 0)

        # update the state's count
        current[1] += 1

        # set the state's timestamp to the record's assigned event time timestamp
        current[2] = ctx.timestamp()

        # write the state back
        self.state.update(current)

        # schedule the next timer 60 seconds from the current event time
        ctx.timer_service().register_event_time_timer(current[2] + 60000)

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        # get the state for the key that scheduled the timer
        result = self.state.value()

        # check if this is an outdated timer or the latest timer
        if timestamp == result[2] + 60000:
            # emit the state on timeout
            yield result[0], result[1]


class MyTimestampAssigner(TimestampAssigner):

    def __init__(self):
        self.epoch = datetime.datetime.utcfromtimestamp(0)

    def extract_timestamp(self, value, record_timestamp) -> int:
        return int((value[0] - self.epoch).total_seconds() * 1000)


if __name__ == '__main__':

    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    t_env.execute_sql("""
            CREATE TABLE my_source (
              a TIMESTAMP(3),
              b VARCHAR,
              c VARCHAR
            ) WITH (
              'connector' = 'datagen',
              'rows-per-second' = '10'
            )
        """)

    stream = t_env.to_append_stream(
        t_env.from_path('my_source'),
        Types.ROW([Types.SQL_TIMESTAMP(), Types.STRING(), Types.STRING()]))

    watermarked_stream = stream.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_monotonous_timestamps()
                         .with_timestamp_assigner(MyTimestampAssigner()))

    # apply the process function onto a keyed stream
    watermarked_stream.key_by(lambda value: value[1])\
        .process(CountWithTimeoutFunction()) \
        .print()

    env.execute()