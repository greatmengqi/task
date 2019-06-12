from xmlrpc import client
from configuration import defaultConf

proxy = client.ServerProxy("http://" + defaultConf.host + ":" + defaultConf.port)

while True:
    print(proxy.add(1, 2))