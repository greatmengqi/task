from sqlHelper import sql_helper


def update(taskId, message):
    # print(message)
    sql_helper.exec("INSERT into [catsic].[dbo].[d8_ais_log]  VALUES (getDate(),'%s','%d')" % (message, taskId))
