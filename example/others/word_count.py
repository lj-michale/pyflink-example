# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         word_count
# Description:
# Author:       orange
# Date:         2021/4/27
# -------------------------------------------------------------------------------

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import TableConfig, DataTypes, BatchTableEnvironment
from pyflink.table.descriptors import Schema, OldCsv, FileSystem
from pyflink.table.expressions import lit


