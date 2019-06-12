from sqlHelper import sql_helper
from statisHelper import statusHelpr
from tasks import BigDataTask


def getTask():
    task = None
    taskList = sql_helper.feachAll(r"SELECT * FROM [catsic].[dbo].[d8_ais_task] WHERE invalidTime is null")
    exeingTask = list(filter(lambda obj: True if obj['nowStatus'] == 1 else False, taskList))

    if len(exeingTask) != 0:
        pass
        # print("有任务正在执行")
    else:
        waitTasks: list = list(filter(lambda obj: True if obj['nowStatus'] == 4 else False, taskList))
        print(waitTasks)
        waitTasks.sort(key=lambda x: x['createTime'])
        if len(waitTasks) == 0:
            # 没有任务
            # print("没有任务需要执行")
            return
        else:
            task = waitTasks[0]
            # print("开始执行任务：%d" % (task["id"]))
    return task


def execTask(task):
    """
    :param task:
    {'taskType': -1, 'startDate': '2019-05', 'createManId': 1, 'createTime': '2019-05-15 17:11:13.8070000', 'nowStatus': 4, 'key': 101, 'taskInfo': '""', 'level': None, 'endDate': '2019-05', 'operationManId': 1, 'operationTime': '2019-05-15 17:11:13.8070000', 'operationType': 0, 'tackTime': '2019-05-15 17:11:13.8070000', 'invalidTime': '2019-05-15 17:11:46.0000000', 'id': 1}
    :return:
    """
    bigDataTask = BigDataTask(tasktype=task['taskType'], startTime=task['startDate'], endTime=task['endDate'])
    statusHelpr.startTask(task["id"])

    code = bigDataTask.run()

    if code is True:
        statusHelpr.taskSuccessful(task["id"])
    else:
        statusHelpr.taskError(task["id"])


if __name__ == "__main__":
    task = getTask()
    if task is not None:
        execTask(task)
