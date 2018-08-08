# -*- coding: UTF-8 -*- 

import MySQLdb
import psycopg2
import traceback
from sql.utils.aes_decryptor import Prpcrypt
from sql.models import Instance
import logging
import re

prpCryptor = Prpcrypt()

logger = logging.getLogger('default')


class Dao(object):
    def __init__(self, instance_name=None, **kwargs):
        if instance_name:
            try:
                instance_info = Instance.objects.get(instance_name=instance_name)
                self.host = instance_info.host
                self.port = int(instance_info.port)
                self.user = instance_info.user
                self.password = prpCryptor.decrypt(instance_info.password)
                self.db_type = instance_info.db_type
            except Exception:
                raise Exception('找不到对应的实例配置信息，请配置')
        else:
            self.host = kwargs.get('host', '')
            self.port = kwargs.get('port', 0)
            self.user = kwargs.get('user', '')
            self.password = prpCryptor.decrypt(kwargs.get('password', ''))
            self.db_type = kwargs.get('db_type', 'mysql')

    # 连进指定的mysql实例里，读取所有databases并返回
    def getAlldbByCluster(self):
        conn = None
        cursor = None

        if self.db_type == "mysql":
            try:
                conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.password,
                                       charset='utf8')
                cursor = conn.cursor()
                sql = "show databases"
                cursor.execute(sql)
                db_list = [row[0] for row in cursor.fetchall()
                           if row[0] not in ('information_schema', 'performance_schema', 'mysql', 'test')]
            except MySQLdb.Warning as w:
                raise Exception(w)
            except MySQLdb.Error as e:
                raise Exception(e)
            finally:
                if cursor is not None:
                    cursor.close()
                if conn is not None:
                    conn.commit()
                    conn.close()
        elif self.db_type == "pgsql":
            try:
                conn = psycopg2.connect(host=self.host, port=self.port, user=self.user, passwd=self.password,
                                        dbname='postgres')
                cursor = conn.cursor()
                cursor.execute("SELECT datname FROM pg_database;")
                db_list = [row[0] for row in cursor.fetchall() if row[0] not in ('postgres', 'template0', 'template1')]
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
        return db_list

    # 连进指定的mysql实例里，读取所有tables并返回
    def getAllTableByDb(self, db_name):
        if self.db_type == "mysql":
            try:
                conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.password, db=db_name,
                                       charset='utf8')
                cursor = conn.cursor()
                sql = "show tables"
                cursor.execute(sql)
                tb_list = [row[0] for row in cursor.fetchall() if row[0] not in ['test']]
            except MySQLdb.Warning as w:
                raise Exception(w)
            except MySQLdb.Error as e:
                raise Exception(e)
            finally:
                conn.commit()
                conn.close()
        elif self.db_type == "pgsql":
            try:
                conn = psycopg2.connect(host=self.host, port=self.port, user=self.user, passwd=self.password, dbname=db_name)
                cursor = conn.cursor()
                sql = """SELECT tablename FROM pg_tables WHERE tableowner='{}';""".format(self.user)
                cursor.execute(sql)
                tb_list = [row[0] for row in cursor.fetchall()]
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
        return tb_list

    # 连进指定的mysql实例里，读取所有Columns并返回
    def getAllColumnsByTb(self, db_name, tb_name):
        if self.db_type == "mysql":
            try:
                conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.password, db=db_name,
                                       charset='utf8')
                cursor = conn.cursor()
                sql = "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s';" % (
                    db_name, tb_name)
                cursor.execute(sql)
                col_list = [row[0] for row in cursor.fetchall()]
            except MySQLdb.Warning as w:
                raise Exception(w)
            except MySQLdb.Error as e:
                raise Exception(e)
            finally:
                conn.commit()
                conn.close()
        elif self.db_type == "pgsql":
            try:
                conn = psycopg2.connect(host=self.host, port=self.port, user=self.user, passwd=self.password,
                                        dbname=db_name)
                cursor = conn.cursor()
                sql = """SELECT column_name FROM information_schema.columns WHERE table_name = '{}';""".format(tb_name)
                cursor.execute(sql)
                col_list = [row[0] for row in cursor.fetchall()]
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
        return col_list

    # 连进指定的mysql实例里，执行sql并返回
    def mysql_query(self, db_name, sql, limit_num=0):
        result = {'column_list': [], 'rows': [], 'effect_row': 0}
        try:
            conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.password, db=db_name,
                                   charset='utf8')
            cursor = conn.cursor()
            effect_row = cursor.execute(sql)
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
            result['rows'] = rows
            result['effect_row'] = effect_row

        except MySQLdb.Warning as w:
            logger.error(traceback.format_exc())
            result['Warning'] = str(w)
        except MySQLdb.Error as e:
            logger.error(traceback.format_exc())
            result['Error'] = str(e)
        else:
            conn.rollback()
            conn.close()
        return result

    # 连进指定的mysql实例里，执行sql并返回
    def mysql_execute(self, db_name, sql):
        result = {}
        try:
            conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.password, db=db_name,
                                   charset='utf8')
            cursor = conn.cursor()
            effect_row = cursor.execute(sql)
            # result = {}
            # result['effect_row'] = effect_row
            conn.commit()
        except MySQLdb.Warning as w:
            logger.error(traceback.format_exc())
            result['Warning'] = str(w)
        except MySQLdb.Error as e:
            logger.error(traceback.format_exc())
            result['Error'] = str(e)
        else:
            cursor.close()
            conn.close()
        return result

    # 连进指定的pgsql实例里，执行sql并返回
    def pgsql_query(self, db_name, sql, limit_num=0):
        result = {'column_list': [], 'rows': [], 'effect_row': 0}
        conn = None
        cursor = None
        is_show_create_table, tb_name = False, None
        try:
            conn = psycopg2.connect(host=self.host, port=self.port, user=self.user, passwd=self.password, dbname=db_name)
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
                """.format(db_name, tb_name)
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

    # 连进指定的pgsql实例里，执行sql并返回
    def pgsql_execute(self, db_name, sql):
        result = {}
        conn = None
        cursor = None

        try:
            conn = psycopg2.connect(host=self.host, port=self.port, user=self.user, passwd=self.password, dbname=db_name)
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
