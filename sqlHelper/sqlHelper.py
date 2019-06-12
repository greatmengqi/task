import pymssql as sql
import configparser as cp


class SqlHelper:
    # server = None
    # user = None
    # password = None
    # database = None

    def __init__(self):
        parser = cp.ConfigParser()
        parser.read(r"./conf/sqlConf.properties", "utf8")

        self.server = parser.get("sqlConf", "server")
        self.user = parser.get("sqlConf", "user")
        self.password = parser.get("sqlConf", "password")
        self.database = parser.get("sqlConf", "database")

    def __getConnect(self):
        return sql.connect(self.server, self.user, self.password, self.database)

    def __getCursor(self):
        self.connect: sql.Connection = self.__getConnect()
        return self.connect.cursor()

    def feachAll(self, dql: str):
        """
        获取表
        :param dql:
        :param struct:
        :return:
        """
        resList = []
        cursor: sql.Cursor = self.__getCursor()
        cursor.execute(dql)

        # 获取表的结构
        description = cursor.description
        files = []
        for i in description:
            files.append(i[0])
        # 获取表内容
        cursor.execute(dql)
        row: list = cursor.fetchone()
        while row:
            resList.append(list(zip(files, row)))
            row = cursor.fetchone()
        return list(map(lambda x: dict(x), resList))

    def exec(self, dml):
        # print(dml)
        cursor: sql.Cursor = self.__getCursor()
        cursor.execute(dml)
        self.connect.commit()
        self.connect.close()

    def bulkInsert(self, dml: str, values):
        """
        :param dml:
            "INSERT INTO persons VALUES (%d, %s, %s)"
        :param values:
            [(1, 'John Smith', 'John Doe'),
             (2, 'Jane Doe', 'Joe Dog'),
             (3, 'Mike T.', 'Sarah H.')]
        :return:
        """
        cursor = self.__getCursor()
        cursor.executemany(dml, values)
        self.connect.commit()
        self.connect.close()

    def __close(self):
        self.connect.close()


sql_helper = SqlHelper()

if __name__ == "__main__":
    sql_helper = SqlHelper()
    sql_helper.exec("UPDATE [catsic].[dbo].[d8_ais_task] set nowStatus = 1 WHERE id = 10")
