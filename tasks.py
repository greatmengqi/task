# -*-coding:utf-8 -*-
import subprocess as sub
from xmlrpc import client
from configuration import *
from subProcessHelper import *
from log.loghandle import logger


class BigDataTask(object):

    def __init__(self, tasktype, startTime, endTime, *parm1, **parm2):
        self.tasktype = tasktype
        self.startTime = startTime
        self.endTime = endTime
        self.month = startTime[0: 7]
        self.dataPath = "" # wait

    def putDataToHdfs(self):
        # 判断是否存在本月数据文件夹
        command = "test -d {}/{}".format(defaultConf.path, self.month)
        file_exist = exec_command(command)
        if file_exist == 0:  # 存在
            putcommand = "hadoop fs -put {}/{} /aisdata_origin".format(
                defaultConf.path, self.month)
            exec_command(putcommand)
            return 0
        else:
            logger.info(self.month + "月份数据不存在")
            return 1

    def messageCastics(self):
        def callback(returncode):
            pass

        return exec_shell("message.sh", callback, self.startTime, self.endTime)

    def saveDateToHbase(self):
        def callback(returncode):
            pass

        return exec_shell("saveFile.sh", callback, self.startTime, self.endTime,)

    def shipClassification(self):
        def callback(returncode):
            pass

        return exec_shell('shipClassification.sh', callback, self.startTime, self.endTime)

    def run(self):
        # funcs = [self.putDataToHdfs, self.messageCastics, self.saveDateToHbase, self.shipClassification]
        # for func in funcs:
        #     if func() != 0:
        #         return False
        return True
