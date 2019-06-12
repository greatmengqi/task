from sqlHelper import sql_helper
from . import logHelper as log
from log.loghandle import logger


def startTask(taskid):
    sql_helper.exec("UPDATE [catsic].[dbo].[d8_ais_task] set nowStatus = 1 WHERE id = %d" % (taskid))
    logger.info("任务" + str(taskid) + "任务开始")
    log.update(taskid, "任务开始")


def taskSuccessful(taskid):
    sql_helper.exec("UPDATE [catsic].[dbo].[d8_ais_task] set nowStatus = 2 WHERE id = %d" % (taskid))
    logger.info("任务" + str(taskid) + "任务结束")
    log.update(taskid, "任务结束")


def taskError(taskid):
    sql_helper.exec("UPDATE [catsic].[dbo].[d8_ais_task] set nowStatus = 3 WHERE id = %d" % (taskid))
    logger.info("任务" + str(taskid) + "任务出错")
    log.update(taskid, "任务出错")
