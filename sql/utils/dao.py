# -*- coding: UTF-8 -*- 

import MySQLdb
import psycopg2
import pyodbc
import cx_Oracle
import traceback
from common.utils.aes_decryptor import Prpcrypt
from sql.models import Instance
import logging
import re

logger = logging.getLogger('default')
decryptor = Prpcrypt()


class Dao(object):
    def __init__(self, instance_name=None, db_name=None, flag=False, **kwargs):
        self.flag = flag
        if instance_name:
            try:
                instance_info = Instance.objects.get(instance_name=instance_name)
                self.host = instance_info.host
                self.port = int(instance_info.port)
                self.user = instance_info.user
                self.password = decryptor.decrypt(instance_info.password)
                self.osn = instance_info.osn
                self.db_type = instance_info.db_type
                if flag is True:
                    if self.db_type == "mysql":
                        # flag表示返回dao长连接对象，减少重连次数
                        self.conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user,
                                                    passwd=self.password, db=db_name, charset='utf8')
                    elif self.db_type == "pgsql":
                        self.conn = psycopg2.connect(host=self.host, port=self.port, user=self.user,
                                                     password=self.password, dbname=db_name)
                    elif self.db_type == "oracle":
                        self.conn = cx_Oracle.connect(self.user, self.password, '{}:/{}:pooled'.format(self.host, db_name),
                                                      cclass="HOL", purity=cx_Oracle.ATTR_PURITY_SELF)
                    elif self.db_type == "mssql":
                        driver = 'DRIVER={ODBC Driver 17 for SQL Server};' + "SERVER={},{};DATABASE={};UID={};PWD={}".format(
                            self.host, self.port, db_name, self.user, self.password
                            )
                        self.conn = pyodbc.connect(driver)
                    self.cursor = self.conn.cursor()
            except Exception as e:
                raise Exception('实例链接出错：%s' % str(e))
        else:
            self.host = kwargs.get('host', '')
            self.port = kwargs.get('port', 0)
            self.user = kwargs.get('user', '')
            self.password = decryptor.decrypt(kwargs.get('password', ''))
            self.db_type = kwargs.get('db_type', 'mysql')

    def close(self):
        self.cursor.close()
        self.conn.close()

    # 连进指定的mysql实例里，读取所有databases并返回
    def getAlldbByCluster(self):
        conn, cursor = None, None
        if self.db_type == "mysql":
            try:
                conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.password,
                                       charset='utf8', connect_timeout=3)
                cursor = conn.cursor()
                sql = "show databases"
                cursor.execute(sql)
                db_list = [row[0] for row in cursor.fetchall()
                           if row[0] not in ('information_schema', 'performance_schema', 'mysql', 'test', 'sys')]
            except MySQLdb.Warning as w:
                logger.error(traceback.format_exc())
                raise Exception(w)
            except MySQLdb.Error as e:
                logger.error(traceback.format_exc())
                raise Exception(e)
            else:
                cursor.close()
                conn.close()
        elif self.db_type == "pgsql":
            try:
                conn = psycopg2.connect(host=self.host, port=self.port, user=self.user, password=self.password,
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
        elif self.db_type == "oracle":
            try:
                driver = '{}/{}@{}/{}'.format(self.user, self.password, self.host, self.osn)
                conn = cx_Oracle.connect(driver)
                cursor = conn.cursor()
                cursor.execute("select * from dba_users where owner='{}'".format(self.user))
                db_list = [item[0] for item in cursor.fetchall()]
            except Exception as e:
                raise Exception(e)
            finally:
                if cursor is not None:
                    cursor.close()
                if conn is not None:
                    conn.commit()
                    conn.close()
        elif self.db_type == "mssql":
            try:
                driver = 'DRIVER={ODBC Driver 17 for SQL Server};' + """SERVER={},{};UID={};PWD={}""".format(
                    self.host, self.port, self.user, self.password
                )
                conn = pyodbc.connect(driver)
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sys.databases ;")
                db_list = [item[0] for item in cursor.fetchall()]
            except (pyodbc.Error, pyodbc.OperationalError) as e:
                raise Exception(e)
            except Exception as e:
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
        conn, cursor = None, None
        if self.db_type == "mysql":
            try:
                conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.password, db=db_name,
                                       charset='utf8', connect_timeout=3)
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
                conn = psycopg2.connect(host=self.host, port=self.port, user=self.user, password=self.password, dbname=db_name)
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
        elif self.db_type == "oracle":
            try:
                driver = '{}/{}@{}/{}'.format(self.user, self.password, self.host, self.osn)
                conn = cx_Oracle.connect(driver)
                cursor = conn.cursor()
                print("sdfsdfsf")
                cursor.execute("""select OBJECT_NAME from dba_objects where OWNER = '{}'""".format(self.user))
                tb_list = [item[0] for item in cursor.fetchall()]
            except Exception as e:
                raise Exception(e)
            finally:
                if cursor is not None:
                    cursor.close()
                if conn is not None:
                    conn.commit()
                    conn.close()
        elif self.db_type == "mssql":
            try:
                driver = 'DRIVER={ODBC Driver 17 for SQL Server};' + """SERVER={},{};DATABASE={};UID={};PWD={}""".format(
                    self.host, self.port, db_name, self.user, self.password
                )
                conn = pyodbc.connect(driver)
                cursor = conn.cursor()
                sql = r"SELECT TABLE_NAME FROM {}.INFORMATION_SCHEMA.Tables WHERE TABLE_TYPE = 'BASE TABLE'".format(
                    db_name)
                cursor.execute(sql)
                tb_list = [item[0] for item in cursor.fetchall()]
            except (pyodbc.Error, pyodbc.OperationalError) as e:
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
        conn, cursor = None, None
        if self.db_type == "mysql":
            try:
                conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.password, db=db_name,
                                       charset='utf8', connect_timeout=3)
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
                conn = psycopg2.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                                        dbname=db_name)
                cursor = conn.cursor()
                sql = """SELECT column_name FROM information_schema.columns WHERE table_name = '{}';""".format(tb_name)
                cursor.execute(sql)
                rows = cursor.fetchall()
                col_list = [row[0] for row in rows]
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
        elif self.db_type == "oracle":
            try:
                driver = '{}/{}@{}/{}'.format(self.user, self.password, self.host, self.osn)
                conn = cx_Oracle.connect(driver)
                cursor = conn.cursor()
                cursor.execute("select * from dba_tab_columns where owner in ('{}') and table_name in ('{}');".format(self.user, tb_name))
                col_list = [col[0] for col in cursor.fetchall()]
            except Exception as e:
                raise Exception(e)
            finally:
                if cursor is not None:
                    cursor.close()
                if conn is not None:
                    conn.commit()
                    conn.close()
        elif self.db_type == "mssql":
            try:
                driver = 'DRIVER={ODBC Driver 17 for SQL Server};' + """SERVER={},{};UID={};PWD={}""".format(
                    self.host, self.port, self.user, self.password
                )
                conn = pyodbc.connect(driver)
                cursor = conn.cursor()
                sql = r"""select c.name
                    from (select name,id,uid from {0}..sysobjects where (xtype='U' or xtype='V') ) o 
                    inner join {0}..syscolumns c on o.id=c.id 
                    inner join {0}..systypes t on c.xtype=t.xusertype 
                    left join {0}..sysusers u on u.uid=o.uid
                    left join (select name,id,uid,parent_obj from {0}..sysobjects where xtype='PK' )  opk on opk.parent_obj=o.id 
                    left join (select id,name,indid from {0}..sysindexes) ie on ie.id=o.id and ie.name=opk.name
                    left join {0}..sysindexkeys i on i.id=o.id and i.colid=c.colid and i.indid=ie.indid
                    WHERE O.name NOT LIKE 'MS%' AND O.name NOT LIKE 'SY%'
                    and O.name='{1}'
                    order by o.name,c.colid""".format(db_name, tb_name)
                cursor.execute(sql)
                col_list = [col[0] for col in cursor.fetchall()]
                print('MS SQL col_list:', col_list)
            except (pyodbc.Error, pyodbc.OperationalError) as e:
                raise Exception(e)
            finally:
                if cursor is not None:
                    cursor.close()
                if conn is not None:
                    conn.commit()
                    conn.close()
        return col_list

    # 连进指定的mysql实例里，执行sql并返回
    def mysql_query(self, db_name=None, sql='', limit_num=0):
        result = {'column_list': [], 'rows': [], 'effect_row': 0}
        try:
            if self.flag:
                conn = self.conn
                cursor = self.cursor
                if db_name:
                    cursor.execute('use {}'.format(db_name))
            else:
                conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.password, db=db_name,
                                       charset='utf8', connect_timeout=3)
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
            if self.flag:
                # 结束后手动close
                pass
            else:
                conn.rollback()
                cursor.close()
                conn.close()
        return result

    # 连进指定的mysql实例里，执行sql并返回
    def mysql_execute(self, db_name, sql):
        result = {}
        try:
            if self.flag:
                conn = self.conn
                cursor = self.cursor
            else:
                conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.password, db=db_name,
                                       charset='utf8', connect_timeout=3)
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
    def pgsql_query(self, db_name=None, sql='', limit_num=0):
        result = {'column_list': [], 'rows': [], 'effect_row': 0}
        conn = None
        cursor = None
        is_show_create_table, tb_name = False, None
        try:
            if self.flag:
                conn = self.conn
                cursor = self.cursor
            else:
                conn = psycopg2.connect(host=self.host, port=self.port, user=self.user, password=self.password, dbname=db_name)
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
                result['column_list'] = ["column_name", "data_type", "character_maximum_length", "is_nullable"]
                result['rows'] = [list(r) for r in rows]
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
            if self.flag:
                conn = self.conn
                cursor = self.cursor
            else:
                conn = psycopg2.connect(host=self.host, port=self.port, user=self.user, password=self.password, dbname=db_name)
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

    def mssql_query(self, db_name=None, sql='', limit_num=0):
        result = {}
        conn = None
        cursor = None
        is_show_create_table, tb_name = False, None
        try:
            if self.flag:
                conn = self.conn
                cursor = self.cursor
            else:
                driver = 'DRIVER={ODBC Driver 17 for SQL Server};' + """SERVER={},{};UID={};PWD={}""".format(
                    self.host, self.port, self.user, self.password
                )
                conn = pyodbc.connect(driver)
                cursor = conn.cursor()
            if re.match(r"^show\s+create\s+table", sql.lower()):
                is_show_create_table = True
                tb_name = re.sub('^show\s+create\s+table', '', sql[:-1], count=1, flags=0).strip()
                sql = r"""select 
                c.name ColumnName,
                t.name ColumnType,
                c.length  ColumnLength,
                c.scale   ColumnScale,
                c.isnullable ColumnNull,
                    case when i.id is not null then 'Y' else 'N' end TablePk
                from (select name,id,uid from {0}..sysobjects where (xtype='U' or xtype='V') ) o 
                inner join {0}..syscolumns c on o.id=c.id 
                inner join {0}..systypes t on c.xtype=t.xusertype 
                left join {0}..sysusers u on u.uid=o.uid
                left join (select name,id,uid,parent_obj from {0}..sysobjects where xtype='PK' )  opk on opk.parent_obj=o.id 
                left join (select id,name,indid from {0}..sysindexes) ie on ie.id=o.id and ie.name=opk.name
                left join {0}..sysindexkeys i on i.id=o.id and i.colid=c.colid and i.indid=ie.indid
                WHERE O.name NOT LIKE 'MS%' AND O.name NOT LIKE 'SY%'
                and O.name='{1}'
                order by o.name,c.colid""".format(db_name, tb_name)
            effect_row = cursor.execute(sql)
            if int(limit_num) > 0:
                rows = cursor.fetchmany(int(limit_num))
            else:
                rows = cursor.fetchall()
            print('rows:', rows)
            fields = cursor.description

            column_list = []
            if fields:
                for i in fields:
                    column_list.append(i[0])
            result['column_list'] = column_list
            result['effect_row'] = effect_row.arraysize
            if is_show_create_table is True:
                result['column_list'] = ["ColumnName", "ColumnType", "ColumnLength", "ColumnScale", "ColumnNull"]
                result['rows'] = [list(r) for r in rows]
            else:
                result['rows'] = list(rows)
            print(result)
            conn.commit()
        except pyodbc.Warning as w:
            raise Exception(w)
        except pyodbc.Error as e:
            raise Exception(e)
        except:
            raise Exception()
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.commit()
                conn.close()
        return result

    def mssql_execute(self, db_name, sql):
        result = {}
        try:
            if self.flag:
                conn = self.conn
                cursor = self.cursor
            else:
                driver = 'DRIVER={ODBC Driver 17 for SQL Server};' + """SERVER={},{};UID={};PWD={}""".format(
                    self.host, self.port, self.user, self.password
                )
                conn = pyodbc.connect(driver)
                cursor = conn.cursor()
            effect_row = cursor.execute(sql)
            conn.commit()
            rows = cursor.fetchall()
            fields = cursor.description

            column_list = []
            if fields:
                for i in fields:
                    column_list.append(i[0])
            result['column_list'] = column_list
            result['effect_row'] = effect_row.arraysize
            result['rows'] = list(rows)
        except pyodbc.Warning as w:
            raise Exception(w)
        except pyodbc.Error as e:
            raise Exception(e)
        except:
            raise Exception()
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.commit()
                conn.close()
        return result

    def oracle_query(self, db_name=None, sql='', limit_num=0):
        result = {}
        conn = None
        cursor = None
        is_show_create_table, tb_name = False, None
        try:
            if self.flag:
                conn = self.conn
                cursor = self.cursor
            else:
                driver = '{}/{}@{}/{}'.format(self.user, self.password, self.host, self.osn)
                conn = cx_Oracle.connect(driver)
                cursor = conn.cursor()
            if re.match(r"^show\s+create\s+table", sql.lower()):
                is_show_create_table = True
                tb_name = re.sub('^show\s+create\s+table', '', sql[:-1], count=1, flags=0).strip()
                sql = """select dbms_metadata.get_ddl('TABLE','{}','{}') from dual
                      """.format(tb_name, self.user)
            effect_row = cursor.execute(sql)
            if int(limit_num) > 0:
                rows = cursor.fetchmany(int(limit_num))
            else:
                rows = cursor.fetchall()
            print('rows:', rows)
            fields = cursor.description

            column_list = []
            if fields:
                for i in fields:
                    column_list.append(i[0])
            result['column_list'] = column_list
            result['effect_row'] = effect_row.arraysize
            if is_show_create_table is True:
                result['column_list'] = ["ColumnName", "ColumnType", "ColumnLength", "ColumnScale", "ColumnNull"]
                result['rows'] = [list(r) for r in rows]
            else:
                result['rows'] = list(rows)
            print(result)
            conn.commit()
        except cx_Oracle.DatabaseError as e:
            raise Exception(e)
        except Exception as e:
            raise Exception(e)
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.commit()
                conn.close()
        return result

    def oracle_execute(self, db_name, sql):
        result = {}
        try:
            if self.flag:
                conn = self.conn
                cursor = self.cursor
            else:
                driver = '{}/{}@{}/{}'.format(self.user, self.password, self.host, self.osn)
                conn = cx_Oracle.connect(driver)
                cursor = conn.cursor()
            effect_row = cursor.execute(sql)
            conn.commit()
            rows = cursor.fetchall()
            fields = cursor.description

            column_list = []
            if fields:
                for i in fields:
                    column_list.append(i[0])
            result['column_list'] = column_list
            result['effect_row'] = effect_row.arraysize
            result['rows'] = list(rows)
        except cx_Oracle.DatabaseError as e:
            raise Exception(e)
        except Exception as e:
            raise Exception(e)
        except:
            raise Exception()
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.commit()
                conn.close()
        return result
