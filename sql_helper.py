import logging

import pymysql
from dbutils.persistent_db import PersistentDB
from pymysql.constants import CLIENT

_logger = logging.getLogger(__name__)


class SQLHelper(object):
    """
    user dbutils create connection pool
    see reference https://webwareforpython.github.io/DBUtils/main.html
    """

    def __init__(self, **kwargs):
        self.__connection_pool = PersistentDB(
            creator=pymysql,  # 使用链接数据库的模块
            maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
            host=kwargs["host"],
            port=kwargs["port"],
            user=kwargs["user"],
            password=kwargs.get("password"),
            database=kwargs["database"],
            charset=kwargs.get("charset", "utf8mb4"),
            client_flag=CLIENT.MULTI_STATEMENTS,
        )

    def connection(self, cursor):
        conn = self.__connection_pool.connection()
        cursor = conn.cursor(cursor=cursor)
        return conn, cursor

    @staticmethod
    def close(conn, cursor):
        conn.commit()
        cursor.close()
        conn.close()

    def fetch_one(self, sql, args=None, cursor=pymysql.cursors.DictCursor):
        _logger.debug("exec sql %s and params %s", sql, args)
        conn, cursor = self.connection(cursor)
        cursor.execute(sql, args)
        obj = cursor.fetchone()
        self.close(conn, cursor)
        return obj

    def fetch_all(self, sql, args=None, cursor=pymysql.cursors.DictCursor):
        _logger.debug("exec sql %s and params %s", sql, args)
        conn, cursor = self.connection(cursor)
        cursor.execute(sql, args)
        obj = cursor.fetchall()
        self.close(conn, cursor)
        return obj

    def update(self, sql, args=None, cursor=pymysql.cursors.DictCursor):
        return self.execute(sql, args=args, cursor=cursor)

    def execute(self, sql, args=None, cursor=pymysql.cursors.DictCursor):
        conn, cursor = self.connection(cursor)
        result = cursor.execute(sql, args)
        self.close(conn, cursor)
        return result


if __name__ == "__main__":
    db_config = {
        "host": "127.0.0.1",
        "user": "root",
        "password": "123456",
        "database": "fia",
        "port": 3306,
    }
    tests_cli = SQLHelper(**db_config)
    result = tests_cli.fetch_all("show tables;")
    for line in result:
        table = list(line.values())[0]
        print(
            f""" ## {table}
|    Fields    | Type | Remark |
| :----------: | :--: | :----: |"""
        )
        result = tests_cli.fetch_one(f"show create table {table}")
        for line in result["Create Table"].split("\n"):
            # print("====", line)

            if line.startswith("  `"):
                field_type = "int"
                if "varchar" in line:
                    field_type = "string"
                if "text" in line:
                    field_type = "text"

                line = line[line.find("`") + 1 :]
                name = line[: line.find("`")]
                comment = ""
                pos = line.find("COMMENT")
                if pos >= 0:
                    line = line[pos + 8 :]

                    comment = line.strip(",").strip("'")
                print("|", name, "|", field_type, "|", comment, "|")

        print("\n")
