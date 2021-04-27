# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         data_stream_api
# Description:
# Author:       orange
# Date:         2021/4/27
# -------------------------------------------------------------------------------

from pyflink.common.serialization import SimpleStringEncoder
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import StreamingFileSink


def tutorial():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    ds = env.from_collection(
        collection=[(1, 'aaa'), (2, 'bbb')],
        type_info=Types.ROW([Types.INT(), Types.STRING()]))
    ds.print()
    ds.add_sink(StreamingFileSink
                .for_row_format('file:\\\E:\\company\\myself\\items\\python\\python_learn\\pyflink12-example\\tmp\\output', SimpleStringEncoder())
                .build())
    env.execute("tutorial_job")


if __name__ == '__main__':
    tutorial()

