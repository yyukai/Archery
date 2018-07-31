# -*- coding: UTF-8 -*- 

import psycopg2
import logging
import re
logger = logging.getLogger('default')


class PgSQLDao(object):
    def __init__(self):
        pass

    # 连进指定的mysql实例里，读取所有databases并返回
    def getAlldbByCluster(self, masterHost, masterPort, masterUser, masterPassword):
        listDb = []
        conn = None
        cursor = None

        try:
            conn = psycopg2.connect(host=masterHost, port=masterPort, user=masterUser, password=masterPassword,
                                    dbname='postgres')
            cursor = conn.cursor()
            cursor.execute("SELECT datname FROM pg_database;")
            listDb = [row[0] for row in cursor.fetchall()
                      if row[0] not in ('postgres', 'template0', 'template1')]
        except psycopg2.Warning as w:
            raise Exception(w)
        except psycopg2.Error as e:
            raise Exception(e)
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.commit()
                conn.close()
        return listDb

    # 连进指定的mysql实例里，读取所有tables并返回
    def getAllTableByDb(self, masterHost, masterPort, masterUser, masterPassword, dbName):
        listTb = []
        conn = None
        cursor = None

        try:
            conn = psycopg2.connect(host=masterHost, port=masterPort, user=masterUser, password=masterPassword, dbname=dbName)
            cursor = conn.cursor()
            sql = """SELECT tablename FROM pg_tables WHERE tableowner='{}';""".format(masterUser)
            cursor.execute(sql)
            listTb = [row[0] for row in cursor.fetchall()]
        except psycopg2.Warning as w:
            raise Exception(w)
        except psycopg2.Error as e:
            raise Exception(e)
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.commit()
                conn.close()
        return listTb

    # 连进指定的mysql实例里，读取所有Columns并返回
    def getAllColumnsByTb(self, masterHost, masterPort, masterUser, masterPassword, dbName, tbName):
        listCol = []
        conn = None
        cursor = None

        try:
            conn = psycopg2.connect(host=masterHost, port=masterPort, user=masterUser, password=masterPassword, dbname=dbName)
            cursor = conn.cursor()
            sql = """SELECT column_name FROM information_schema.columns WHERE table_name = '{}';""".format(tbName)
            cursor.execute(sql)
            listCol = [row[0] for row in cursor.fetchall()]
        except psycopg2.Warning as w:
            raise Exception(w)
        except psycopg2.Error as e:
            raise Exception(e)
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.commit()
                conn.close()
        return listCol

    # 连进指定的mysql实例里，执行sql并返回
    def query(self, masterHost, masterPort, masterUser, masterPassword, dbName, sql, limit_num=0):
        result = {'column_list': [], 'rows': [], 'effect_row': 0}
        conn = None
        cursor = None
        is_show_create_table, tb_name = False, None
        try:
            conn = psycopg2.connect(host=masterHost, port=masterPort, user=masterUser, password=masterPassword, dbname=dbName)
            cursor = conn.cursor()
            if re.match(r"^show\s+create\s+table", sql.lower()):
                is_show_create_table = True
                tb_name = re.sub('^show\s+create\s+table', '', sql[:-1], count=1, flags=0).strip()
                sql = """SELECT column_name,
                                       data_type,
                                       ''||COALESCE(character_maximum_length, -1) as length,
                                       is_nullable
                                FROM   information_schema.columns
                                WHERE  table_catalog = '{0}'
                                       AND table_name = '{1}'
                                ORDER  BY ordinal_position;
                """.format(dbName, tb_name)
            print(sql)
            cursor.execute(sql)
            if int(limit_num) > 0:
                rows = cursor.fetchmany(size=int(limit_num))
            else:
                rows = cursor.fetchall()
            fields = cursor.description

            column_list = []
            if fields:
                for i in fields:
                    column_list.append(i[0])
            result['column_list'] = column_list
            if is_show_create_table is True:
                result['column_list'] = ["Table", "Create Table"]
                show_create_table_text = "column_name | data_type | character_maximum_length | is_nullable\n"
                for r in rows:
                    show_create_table_text += ' | '.join(r) + '\n'
                result['rows'] = ((tb_name, show_create_table_text),)
            else:
                result['rows'] = rows
            result['effect_row'] = -1 if cursor.rowcount is None else cursor.rowcount

        except psycopg2.Warning as w:
            logger.warning(str(w))
            result['Warning'] = str(w)
        except psycopg2.Error as e:
            logger.error(str(e))
            result['Error'] = str(e)
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                try:
                    conn.rollback()
                    conn.close()
                except:
                    conn.close()
        return result

    # 连进指定的mysql实例里，执行sql并返回
    def execute(self, masterHost, masterPort, masterUser, masterPassword, dbName, sql):
        result = {}
        conn = None
        cursor = None

        try:
            conn = psycopg2.connect(host=masterHost, port=masterPort, user=masterUser, password=masterPassword, dbname=dbName)
            cursor = conn.cursor()
            cursor.execute(sql)
            result['effect_row'] = -1 if cursor.rowcount is None else cursor.rowcount
            conn.commit()
        except psycopg2.Warning as w:
            logger.warning(str(w))
            result['Warning'] = str(w)
        except psycopg2.Error as e:
            logger.error(str(e))
            result['Error'] = str(e)
        finally:
            if result.get('Error') or result.get('Warning'):
                conn.close()
            elif cursor is not None:
                cursor.close()
                conn.close()
        return result
