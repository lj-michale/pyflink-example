# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         dataset_example_001
# Description:  ./bin/pyflink.sh <Script>[ <pathToPackage1>[ <pathToPackageX]][ - <param1>[ <paramX>]]
# Author:       orange
# Date:         2021/7/27
# -------------------------------------------------------------------------------

import os
import shutil

from pyflink.dataset import ExecutionEnvironment
from pyflink.table.descriptors import Schema, OldCsv, FileSystem
from pyflink.table.expressions import lit
from pyflink.table import (
    TableConfig,
    DataTypes,
    BatchTableEnvironment
)

exec_env = ExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(1)
t_config = TableConfig()
t_env = BatchTableEnvironment.create(exec_env, t_config)

t_env.connect(FileSystem().path('C:\\Users\\DELL\\Desktop\\PYFLINK\\input.csv')) \
    .with_format(OldCsv()
                 .field('word', DataTypes.STRING())) \
    .with_schema(Schema()
                 .field('word', DataTypes.STRING())) \
    .create_temporary_table('mySource')

t_env.connect(FileSystem().path('C:\\Users\\DELL\\Desktop\\PYFLINK\\ouput.csv')) \
    .with_format(OldCsv()
                 .field_delimiter('\t')
                 .field('word', DataTypes.STRING())
                 .field('count', DataTypes.BIGINT())) \
    .with_schema(Schema()
                 .field('word', DataTypes.STRING())
                 .field('count', DataTypes.BIGINT())) \
    .create_temporary_table('mySink')

tab = t_env.from_path('mySource')
tab.group_by(tab.word) \
   .select(tab.word, lit(1).count) \
   .execute_insert('mySink').wait()