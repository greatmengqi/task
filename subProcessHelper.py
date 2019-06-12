from configuration import *
import subprocess as sub
from log.loghandle import logger


def exec_shell(shell, name, callback, *args):
    spark = getConfBySection("spark")
    command = "sh {path}/{shell}".format(path=spark.shellpath, shell=shell)
    if len(args) == 0:
        pass
    else:
        for arg in args:
            command = command + r" {}".format(arg)
    # popen = sub.Popen(command, stdout=sub.PIPE, stderr=sub.STDOUT, shell=True)
    # code = get_process_code(popen)

    # debug
    code = 0
    logger.info(command)

    if code == 0:
        # 数据处理成功
        logger.info(name + "执行成功")
        return callback(0)
    else:
        # 数据处理失败
        logger.info(name + "执行失败")
        return callback(1)


def exec_command(command, callback=None, *args):
    logger.info(command)
    popen = sub.Popen(command, stdout=sub.PIPE, stderr=sub.STDOUT, shell=True)
    code = get_process_code(popen)

    if callback is None:
        return code
    else:
        return callback(code)


def get_process_code(popen):
    for line in iter(popen.stdout.readline, b''):
        if popen.poll() is not None:
            pass
        else:
            logger.info(line)
    popen.stdout.close()
    popen.wait()
    return popen.returncode
