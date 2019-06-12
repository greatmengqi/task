import configparser as cp


class Configuration(object):

    def __init__(self, path, section):
        parser = cp.ConfigParser()
        parser.read(path, "utf8")
        for item in parser.options(section):
            setattr(self, item, parser.get(section, item))


defaultConf = Configuration(r"./conf/sqlConf.properties", 'hdfs')


def getConfBySection(section):
    return Configuration(r"./conf/sqlConf.properties", section)
