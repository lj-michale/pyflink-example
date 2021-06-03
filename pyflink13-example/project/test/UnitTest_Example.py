# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         UnitTest_Example
# Description:
# Author:       orange
# Date:         2021/6/3
# -------------------------------------------------------------------------------

import unittest

from example.others.demo1 import MyClass


class MyclassTest(unittest.TestCase):

    def setUp(self) -> None:
        '''
        测试之前的准备工作
        :return:
        '''
        self.clac = MyClass(4, 3)

    def tearDown(self) -> None:
        '''
        测试之后的收尾
        如关闭数据库
        :return:
        '''
        pass

    def test_add(self):
        ret = self.clac.add()
        self.assertEqual(ret, 9)

    def test_sub(self):
        ret = self.clac.sub()
        self.assertEqual(ret, -1)


if __name__ == '__main__':

    suite = unittest.TestSuite()
    suite.addTest(MyclassTest('test_add'))
    suite.addTest(MyclassTest('test_sub'))

    runner = unittest.TextTestRunner()
    runner.run(suite)
