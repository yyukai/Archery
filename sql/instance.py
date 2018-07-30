# -*- coding: UTF-8 -*-
import simplejson as json

from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse

from sql.utils.aes_decryptor import Prpcrypt
from sql.utils.dao import Dao
from sql.utils.dao_pgsql import PgSQLDao
from .models import MasterConfig, SlaveConfig

dao = Dao()
pgsql_dao = PgSQLDao()
prpCryptor = Prpcrypt()

# 获取实例里面的数据库集合
@csrf_exempt
def getdbNameList(request):
    clusterName = request.POST.get('cluster_name')
    is_master = request.POST.get('is_master')
    result = {'status': 0, 'msg': 'ok', 'data': []}

    if is_master:
        try:
            master_info = MasterConfig.objects.get(cluster_name=clusterName)
        except Exception:
            result['status'] = 1
            result['msg'] = '找不到对应的主库配置信息，请配置'
            return HttpResponse(json.dumps(result), content_type='application/json')

        try:
            if master_info.cluster_type == "mysql":
                # 取出该实例主库的连接方式，为了后面连进去获取所有databases
                listDb = dao.getAlldbByCluster(master_info.master_host, master_info.master_port, master_info.master_user,
                                               prpCryptor.decrypt(master_info.master_password))
            elif master_info.cluster_type == "pgsql":
                listDb = pgsql_dao.getAlldbByCluster(master_info.master_host, master_info.master_port,
                                                     master_info.master_user,
                                                     prpCryptor.decrypt(master_info.master_password))
            else:
                listDb = list()
            # 要把result转成JSON存进数据库里，方便SQL单子详细信息展示
            result['data'] = listDb
        except Exception as msg:
            result['status'] = 1
            result['msg'] = str(msg)
    else:
        try:
            slave_info = SlaveConfig.objects.get(cluster_name=clusterName)
        except Exception:
            result['status'] = 1
            result['msg'] = '找不到对应的主库配置信息，请配置'
            return HttpResponse(json.dumps(result), content_type='application/json')

        try:
            if slave_info.cluster_type == "mysql":
                # 取出该实例主库的连接方式，为了后面连进去获取所有databases
                listDb = dao.getAlldbByCluster(slave_info.slave_host, slave_info.slave_port,
                                               slave_info.slave_user,
                                               prpCryptor.decrypt(slave_info.slave_password))
            elif slave_info.cluster_type == "pgsql":
                listDb = pgsql_dao.getAlldbByCluster(slave_info.slave_host, slave_info.slave_port,
                                                slave_info.slave_user,
                                                prpCryptor.decrypt(slave_info.slave_password))
            else:
                listDb = list()
            # 要把result转成JSON存进数据库里，方便SQL单子详细信息展示
            result['data'] = listDb
        except Exception as msg:
            result['status'] = 1
            result['msg'] = str(msg)

    return HttpResponse(json.dumps(result), content_type='application/json')


# 获取数据库的表集合
@csrf_exempt
def getTableNameList(request):
    clusterName = request.POST.get('cluster_name')
    db_name = request.POST.get('db_name')
    is_master = request.POST.get('is_master')
    result = {'status': 0, 'msg': 'ok', 'data': []}

    if is_master:
        try:
            master_info = MasterConfig.objects.get(cluster_name=clusterName)
        except Exception:
            result['status'] = 1
            result['msg'] = '找不到对应的主库配置信息，请配置'
            return HttpResponse(json.dumps(result), content_type='application/json')

        try:
            if master_info.cluster_type == "mysql":
                # 取出该实例主库的连接方式，为了后面连进去获取所有的表
                listTb = dao.getAllTableByDb(master_info.master_host, master_info.master_port, master_info.master_user,
                                             prpCryptor.decrypt(master_info.master_password), db_name)
            elif master_info.cluster_type == "pgsql":
                listTb = pgsql_dao.getAllTableByDb(master_info.master_host, master_info.master_port, master_info.master_user,
                                             prpCryptor.decrypt(master_info.master_password), db_name)
            else:
                listTb = list()
            # 要把result转成JSON存进数据库里，方便SQL单子详细信息展示
            result['data'] = listTb
        except Exception as msg:
            result['status'] = 1
            result['msg'] = str(msg)

    else:
        try:
            slave_info = SlaveConfig.objects.get(cluster_name=clusterName)
        except Exception:
            result['status'] = 1
            result['msg'] = '找不到对应的从库配置信息，请配置'
            return HttpResponse(json.dumps(result), content_type='application/json')

        try:
            if slave_info.cluster_type == "mysql":
                # 取出该实例从库的连接方式，为了后面连进去获取所有的表
                listTb = dao.getAllTableByDb(slave_info.slave_host, slave_info.slave_port, slave_info.slave_user,
                                             prpCryptor.decrypt(slave_info.slave_password), db_name)
            elif slave_info.cluster_type == "pgsql":
                # 取出该实例从库的连接方式，为了后面连进去获取所有的表
                listTb = pgsql_dao.getAllTableByDb(slave_info.slave_host, slave_info.slave_port, slave_info.slave_user,
                                             prpCryptor.decrypt(slave_info.slave_password), db_name)
            else:
                listTb = list()

            # 要把result转成JSON存进数据库里，方便SQL单子详细信息展示
            result['data'] = listTb
        except Exception as msg:
            result['status'] = 1
            result['msg'] = str(msg)

    return HttpResponse(json.dumps(result), content_type='application/json')


# 获取表里面的字段集合
@csrf_exempt
def getColumnNameList(request):
    clusterName = request.POST.get('cluster_name')
    db_name = request.POST.get('db_name')
    tb_name = request.POST.get('tb_name')
    is_master = request.POST.get('is_master')
    result = {'status': 0, 'msg': 'ok', 'data': []}

    if is_master:
        try:
            master_info = MasterConfig.objects.get(cluster_name=clusterName)
        except Exception:
            result['status'] = 1
            result['msg'] = '找不到对应的主库配置信息，请配置'
            return HttpResponse(json.dumps(result), content_type='application/json')

        try:
            if master_info.cluster_type == "mysql":
                # 取出该实例主库的连接方式，为了后面连进去获取所有字段
                listCol = dao.getAllColumnsByTb(master_info.master_host, master_info.master_port,
                                                master_info.master_user,
                                                prpCryptor.decrypt(master_info.master_password), db_name, tb_name)
            elif master_info.cluster_type == "pgsql":
                listCol = pgsql_dao.getAllColumnsByTb(master_info.master_host, master_info.master_port,
                                                master_info.master_user,
                                                prpCryptor.decrypt(master_info.master_password), db_name, tb_name)
            else:
                listCol = list()

            # 要把result转成JSON存进数据库里，方便SQL单子详细信息展示
            result['data'] = listCol
        except Exception as msg:
            result['status'] = 1
            result['msg'] = str(msg)
    else:
        try:
            slave_info = SlaveConfig.objects.get(cluster_name=clusterName)
        except Exception:
            result['status'] = 1
            result['msg'] = '找不到对应的从库配置信息，请配置'
            return HttpResponse(json.dumps(result), content_type='application/json')

        try:
            if slave_info.cluster_type == "mysql":
                # 取出该实例的连接方式，为了后面连进去获取表的所有字段
                listCol = dao.getAllColumnsByTb(slave_info.slave_host, slave_info.slave_port, slave_info.slave_user,
                                                prpCryptor.decrypt(slave_info.slave_password), db_name, tb_name)
            elif slave_info.cluster_type == "pgsql":
                # 取出该实例的连接方式，为了后面连进去获取表的所有字段
                listCol = pgsql_dao.getAllColumnsByTb(slave_info.slave_host, slave_info.slave_port, slave_info.slave_user,
                                                prpCryptor.decrypt(slave_info.slave_password), db_name, tb_name)
            else:
                listCol = list()

            # 要把result转成JSON存进数据库里，方便SQL单子详细信息展示
            result['data'] = listCol
        except Exception as msg:
            result['status'] = 1
            result['msg'] = str(msg)
    return HttpResponse(json.dumps(result), content_type='application/json')
