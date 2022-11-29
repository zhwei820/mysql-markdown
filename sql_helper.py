import logging

import pymysql

_logger = logging.getLogger(__name__)


class SQLHelper(object):
    """
    user dbutils create connection pool
    """

    def __init__(self, **kwargs):
        self.db = pymysql.connect(**kwargs)

    def connection(self, cursor):
        return self.db, self.db.cursor(cursor)

    def fetch_one(self, sql, args=None, cursor=pymysql.cursors.DictCursor):
        _logger.debug("exec sql %s and params %s", sql, args)
        conn, cursor = self.connection(cursor)
        cursor.execute(sql, args)
        obj = cursor.fetchone()
        return obj

    def fetch_all(self, sql, args=None, cursor=pymysql.cursors.DictCursor):
        _logger.debug("exec sql %s and params %s", sql, args)
        conn, cursor = self.connection(cursor)
        cursor.execute(sql, args)
        obj = cursor.fetchall()
        return obj

    def update(self, sql, args=None, cursor=pymysql.cursors.DictCursor):
        return self.execute(sql, args=args, cursor=cursor)

    def execute(self, sql, args=None, cursor=pymysql.cursors.DictCursor):
        conn, cursor = self.connection(cursor)
        result = cursor.execute(sql, args)
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
