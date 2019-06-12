# TaskServer.py文件将会部署在集群所在的主机上,10.86.11.66
from xmlrpc import server
from configuration import *
import subprocess as sub
from log.loghandle import logger


class TaskServer(server.SimpleXMLRPCServer):

    def _dispatch(self, method, params):
        try:
            func = getattr(self, 'export_' + method)
        except AttributeError:
            raise Exception('method "%s" is not supported' % method)
        else:
            return func(*params)




def getProcessCode(popen):
    for i in iter(popen.stdout.readline, b''):
        if popen.poll() is not None:
            pass
        else:
            print("stdout", i)

    popen.stdout.close()
    popen.wait()
    code = popen.returncode
    print("命令执行结果" + str(code))
    return popen.returncode


server = TaskServer((defaultConf.host, int(defaultConf.port)))
server.serve_forever()
