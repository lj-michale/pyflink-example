# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         data_stream_api_demo
# Description:  适合调试
# Author:       orange
# Date:         2021/6/14
# -------------------------------------------------------------------------------
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment


def data_stream_api_demo():

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)

    ds = env.from_collection(
        collection=[(1, 'aaa|bb'), (2, 'bb|a'), (3, 'aaa|a')],
        type_info=Types.ROW([Types.INT(), Types.STRING()]))

    def split(s):
        splits = s[1].split("|")
        for sp in splits:
            yield s[0], sp

    ds = ds.map(lambda i: (i[0] + 1, i[1])) \
        .flat_map(split) \
        .key_by(lambda i: i[1]) \
        .reduce(lambda i, j: (i[0] + j[0], i[1]))

    ds.print()

    env.execute()


if __name__ == '__main__':
    data_stream_api_demo()