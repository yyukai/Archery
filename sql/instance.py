# -*- coding: UTF-8 -*-
import os
import subprocess
import time
import datetime

import simplejson as json
from django.conf import settings
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.decorators import permission_required

from django.db.models import Q
from django.http import HttpResponse, QueryDict

from common.config import SysConfig
from common.utils.aes_decryptor import Prpcrypt
from common.utils.extend_json_encoder import ExtendJSONEncoder
from sql.utils.permission import get_ding_user_id_by_permission
from sql.utils.ding_api import DingSender
from sql.utils.dao import Dao
from sql.utils.resource_group import user_instances
from sql.engines import get_engine

from .models import Instance, DataBase, ParamTemp, ParamHistory, Replication


# 获取实例列表
@permission_required('sql.menu_instance', raise_exception=True)
def lists(request):
    limit = int(request.POST.get('limit'))
    offset = int(request.POST.get('offset'))
    type = request.POST.get('type')
    db_type = request.POST.get('db_type')
    limit = offset + limit

    # 获取搜索参数
    search = request.POST.get('search', '')
    if type and db_type:
        instances = Instance.objects.filter(instance_name__contains=search, type=type, db_type=db_type)
    elif type:
        instances = Instance.objects.filter(instance_name__contains=search, type=type)
    elif db_type:
        instances = Instance.objects.filter(instance_name__contains=search, db_type=db_type)
    else:
        instances = Instance.objects.filter(instance_name__contains=search)

    count = instances.count()
    instances = instances[offset:limit].values("id", "instance_name", "db_type", "type", "host", "port", "user")

    # QuerySet 序列化
    rows = [row for row in instances]

    result = {"total": count, "rows": rows}
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


# 获取实例用户列表
@permission_required('sql.menu_instance', raise_exception=True)
def users(request):
    instance_id = request.POST.get('instance_id')
    instance_name = Instance.objects.get(id=instance_id).instance_name
    # sql_get_user = '''select concat("\'", user, "\'", '@', "\'", host,"\'") as query from mysql.user;'''
    search = request.POST.get('search', '')
    if search:
        sql_get_user = "SELECT user,host FROM mysql.user WHERE user like '%{0}%' or host like '%{0}%';".format(search)
    else:
        sql_get_user = "SELECT user,host FROM mysql.user;"
    dao = Dao(instance_name=instance_name, db_name='mysql', flag=True)
    db_users = dao.mysql_query('mysql', sql_get_user)['rows']
    # 获取用户权限信息
    data = []
    for db_user in db_users:
        user_info = {}
        user_priv = dao.mysql_query('mysql', "show grants for '{}'@'{}';".format(db_user[0], db_user[1]))['rows']
        user_info['user'] = db_user[0]
        user_info['host'] = db_user[1]
        user_info['privileges'] = user_priv
        data.append(user_info)
    # 关闭连接
    dao.close()
    result = {'status': 0, 'msg': 'ok', 'data': data}
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


# 对比实例schema信息
@permission_required('sql.menu_schemasync', raise_exception=True)
def schemasync(request):
    instance_name = request.POST.get('instance_name')
    db_name = request.POST.get('db_name')
    target_instance_name = request.POST.get('target_instance_name')
    target_db_name = request.POST.get('target_db_name')
    sync_auto_inc = '--sync-auto-inc' if request.POST.get('sync_auto_inc') == 'true' else ''
    sync_comments = '--sync-comments' if request.POST.get('sync_comments') == 'true' else ''
    result = {'status': 0, 'msg': 'ok', 'data': []}

    # diff 选项
    options = sync_auto_inc + ' ' + sync_comments

    # 循环对比全部数据库
    if db_name == 'all' or target_db_name == 'all':
        db_name = '*'
        target_db_name = '*'

    # 取出该实例的连接方式
    instance_info = Instance.objects.get(instance_name=instance_name)
    target_instance_info = Instance.objects.get(instance_name=target_instance_name)

    # 获取对比结果文件
    path = SysConfig().sys_config.get('schemasync', '')
    timestamp = int(time.time())
    output_directory = os.path.join(settings.BASE_DIR, 'downloads/schemasync/')

    command = path + ' %s --output-directory=%s --tag=%s \
            mysql://%s:%s@%s:%d/%s  mysql://%s:%s@%s:%d/%s' % (options,
                                                               output_directory,
                                                               timestamp,
                                                               instance_info.user,
                                                               Prpcrypt().decrypt(instance_info.password),
                                                               instance_info.host,
                                                               instance_info.port,
                                                               db_name,
                                                               target_instance_info.user,
                                                               Prpcrypt().decrypt(target_instance_info.password),
                                                               target_instance_info.host,
                                                               target_instance_info.port,
                                                               target_db_name)
    diff = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            shell=True, universal_newlines=True)
    diff_stdout, diff_stderr = diff.communicate()

    # 非全部数据库对比可以读取对比结果并在前端展示
    if db_name != '*':
        date = time.strftime("%Y%m%d", time.localtime())
        patch_sql_file = '%s%s_%s.%s.patch.sql' % (output_directory, target_db_name, timestamp, date)
        revert_sql_file = '%s%s_%s.%s.revert.sql' % (output_directory, target_db_name, timestamp, date)
        cat_patch_sql = subprocess.Popen(['cat', patch_sql_file], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                         stderr=subprocess.STDOUT, universal_newlines=True)
        cat_revert_sql = subprocess.Popen(['cat', revert_sql_file], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                          stderr=subprocess.STDOUT, universal_newlines=True)
        patch_stdout, patch_stderr = cat_patch_sql.communicate()
        revert_stdout, revert_stderr = cat_revert_sql.communicate()
        result['data'] = {'diff_stdout': diff_stdout, 'patch_stdout': patch_stdout, 'revert_stdout': revert_stdout}
    else:
        result['data'] = {'diff_stdout': diff_stdout, 'patch_stdout': '', 'revert_stdout': ''}

    # 删除对比文件
    # subprocess.call(['rm', '-rf', patch_sql_file, revert_sql_file, 'schemasync.log'])
    return HttpResponse(json.dumps(result), content_type='application/json')


# 获取实例里面的数据库集合
def get_db_name_list(request):
    instance_name = request.POST.get('instance_name')
    try:
        instance = Instance.objects.get(instance_name=instance_name)
    except Instance.DoesNotExist:
        result = {'status': 1, 'msg': '实例不存在', 'data': []}
        return HttpResponse(json.dumps(result), content_type='application/json')
    result = {'status': 0, 'msg': 'ok', 'data': []}

    try:
        # 取出该实例的连接方式，为了后面连进去获取所有databases
        query_engine = get_engine(instance=instance)
        db_list = query_engine.get_all_databases()
        # 要把result转成JSON存进数据库里，方便SQL单子详细信息展示
        result['data'] = db_list
        if not db_list:
            result['status'] = 1
            result['msg'] = '数据库列表为空, 可能是权限或配置有误'
    except Exception as msg:
        result['status'] = 1
        result['msg'] = str(msg)

    return HttpResponse(json.dumps(result), content_type='application/json')


# 获取数据库的表集合
def get_table_name_list(request):
    instance_name = request.POST.get('instance_name')
    try:
        instance = Instance.objects.get(instance_name=instance_name)
    except Instance.DoesNotExist:
        result = {'status': 1, 'msg': '实例不存在', 'data': []}
        return HttpResponse(json.dumps(result), content_type='application/json')
    db_name = request.POST.get('db_name')
    result = {'status': 0, 'msg': 'ok', 'data': []}

    try:
        # 取出该实例实例的连接方式，为了后面连进去获取所有的表
        query_engine = get_engine(instance=instance)
        table_list = query_engine.get_all_tables(db_name)
        # 要把result转成JSON存进数据库里，方便SQL单子详细信息展示
        result['data'] = table_list
        if not table_list:
            result['status'] = 1
            result['msg'] = '表列表为空, 可能是权限或配置有误, 请再次确认库名'
    except Exception as msg:
        result['status'] = 1
        result['msg'] = str(msg)

    return HttpResponse(json.dumps(result), content_type='application/json')


# 获取表里面的字段集合
def get_column_name_list(request):
    instance_name = request.POST.get('instance_name')
    try:
        instance = Instance.objects.get(instance_name=instance_name)
    except Instance.DoesNotExist:
        result = {'status': 1, 'msg': '实例不存在', 'data': []}
        return HttpResponse(json.dumps(result), content_type='application/json')
    db_name = request.POST.get('db_name')
    tb_name = request.POST.get('tb_name')
    result = {'status': 0, 'msg': 'ok', 'data': []}

    try:
        # 取出该实例的连接方式，为了后面连进去获取表的所有字段
        query_engine = get_engine(instance=instance)
        col_list = query_engine.get_all_columns_by_tb(db_name, tb_name)
        # 要把result转成JSON存进数据库里，方便SQL单子详细信息展示
        result['data'] = col_list
        if not col_list:
            result['status'] = 1
            result['msg'] = '字段列表为空, 可能是权限或配置有误'
    except Exception as msg:
        result['status'] = 1
        result['msg'] = str(msg)
    return HttpResponse(json.dumps(result), content_type='application/json')


def describe(request):
    instance_name = request.POST.get('instance_name')
    try:
        instance = Instance.objects.get(instance_name=instance_name)
    except Instance.DoesNotExist:
        result = {'status': 1, 'msg': '实例不存在', 'data': []}
        return HttpResponse(json.dumps(result), content_type='application/json')
    db_name = request.POST.get('db_name')
    tb_name = request.POST.get('tb_name')
    result = {'status': 0, 'msg': 'ok', 'data': []}

    try:
        # 取出该实例的连接方式，为了后面连进去获取表的所有字段
        query_engine = get_engine(instance=instance)
        query_result = query_engine.describe_table(db_name, tb_name)
        # 要把result转成JSON存进数据库里，方便SQL单子详细信息展示
        result['data'] = query_result.__dict__
    except Exception as msg:
        result['status'] = 1
        result['msg'] = str(msg)
    return HttpResponse(json.dumps(result), content_type='application/json')


@permission_required('sql.menu_database', raise_exception=True)
def db_list(request):
    res = {}
    if request.method == 'GET':
        instance_name = request.GET.get('instance_name', '')
        search = request.GET.get('search', '')
        try:
            if instance_name:
                obj_list = DataBase.objects.filter(instance_name=instance_name).filter(Q(db_name__contains=search) |
                                                                                       Q(db_application__contains=search) |
                                                                                       Q(db_person__contains=search))
            else:
                obj_list = DataBase.objects.filter(Q(db_name__contains=search) |
                                                   Q(db_application__contains=search) |
                                                   Q(db_person__contains=search))
            res = list()
            for obj in obj_list:
                res.append({
                    'id': obj.id,
                    'ip_port': '{}:{}'.format(obj.ip, obj.port),
                    'instance': obj.instance_name,
                    'db_name': obj.db_name,
                    'db_application': obj.db_application,
                    'db_person': obj.db_person,
                })
        except Instance.DoesNotExist:
            res = {'status': 1, 'msg': 'Instance.DoesNotExist'}
        except Exception as e:
            res = {'status': 1, 'msg': str(e)}
    if request.method == 'POST':
        db_name = request.POST.get('db_name', '')
        app_type = request.POST.get('app_type', '')
        db_application = request.POST.get('db_application', '')
        db_person = request.user.display
        try:
            db = DataBase.objects.create(db_name=db_name, app_type=app_type, db_application=db_application,
                                         db_person=db_person)
            res = {'status': 0, 'msg': 'ok'}
            msg = '申请新增数据库：\n数据库：{}\n业务：{}\n用途：{}\n申请人：{}\n地址：{}\n请您尽快补全或删除该数据库信息！'.format(db_name,
                    app_type, db_application, db_person, "http://dbms.weidai.com.cn/admin/sql/database/%s/change/" % db.id)
            ding_sender = DingSender()
            ding_user_ids = get_ding_user_id_by_permission('database_edit')
            for ding_id in ding_user_ids:
                ding_sender.send_msg(ding_id, msg)
        except Exception as e:
            res = {'status': 1, 'msg': str(e)}
    return HttpResponse(json.dumps(res, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


@permission_required('sql.menu_database', raise_exception=True)
def db_detail(request):
    instance_name = request.POST.get('instance_name', '')
    search = request.POST.get('search', '')
    try:
        if instance_name:
            obj_list = DataBase.objects.filter(instance_name=instance_name).filter(Q(db_name__contains=search) |
                                                                                   Q(db_application__contains=search) |
                                                                                   Q(db_person__contains=search))
        else:
            obj_list = DataBase.objects.filter(Q(db_name__contains=search) |
                                               Q(db_application__contains=search) |
                                               Q(db_person__contains=search))
        res = list()
        for obj in obj_list:
            res.append({
                'id': obj.id,
                'host': obj.host,
                'ip_port': '{}:{}'.format(obj.ip, obj.port),
                'instance': obj.instance_name,
                'db_name': obj.db_name,
                'db_application': obj.db_application,
                'db_person': obj.db_person,
            })
    except Instance.DoesNotExist:
        print('Instance.DoesNotExist')
        res = {'status': 1, 'msg': 'Instance.DoesNotExist'}
    except Exception as e:
        print(e)
        res = {'status': 1, 'msg': str(e)}
    return HttpResponse(json.dumps(res, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


@csrf_exempt
@permission_required('sql.menu_instance', raise_exception=True)
def replication_delay(request):
    """
        instance_info = [
            [ins1_id, ins1_ip, ins1],
            [ins2_id, ins2_ip, ins2],
            ......
            [ins6_id, ins6_ip, ins6]
        ]
        delay_info = {
            ins1_id: [[ins2, 0], [ins3, 0]],
            ins5_id: [[ins6, 0]]
        }
        :param request:
        :return:
        """
    masters = list()
    delay_info = {}
    all_instances = list()
    for ins in user_instances(request.user, type='all', db_type='mysql'):
        all_instances.append([str(ins.id), ins.host + ":" + str(ins.port), ins.instance_name])
    hour = datetime.datetime.now() - datetime.timedelta(hours=1)
    ins_name = request.GET.get('name', '')
    if ins_name:
        for ins in Instance.objects.filter(instance_name=ins_name):
            masters.append([str(ins.id), ins.host + ":" + str(ins.port), ins.instance_name])
    else:
        for ins in user_instances(request.user, type='master', db_type='mysql'):
            masters.append([str(ins.id), ins.host + ":" + str(ins.port), ins.instance_name])

    for ins in user_instances(request.user, type='all', db_type='mysql'):
        all_instances.append([str(ins.id), ins.host + ":" + str(ins.port), ins.instance_name])
        slave_ins_info = list()
        for slave in Instance.objects.filter(parent=ins, type='slave'):
            rep = Replication.objects.filter(master=ins.instance_name, slave=slave.instance_name, created__gte=hour)
            slave_ins_info.append([str(slave.id), rep[0].delay if rep else 'NaN'])
        delay_info[str(ins.id)] = slave_ins_info
    res = {'instance_info': all_instances, 'masters': masters, 'delay_info': delay_info}
    return HttpResponse(json.dumps(res, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


@permission_required('sql.param_view', raise_exception=True)
def param_list(request):
    instance_name = request.POST.get('instance_name')
    search = request.POST.get('search', '')
    try:
        ins = Instance.objects.get(instance_name=instance_name)
        params = dict()
        for p in ParamTemp.objects.filter(db_type=ins.db_type, param__contains=search):
            params[p.param] = [p.default_var, p.is_reboot, p.available_var, p.description]
        p_list = list(params.keys())
        sql = "SELECT * FROM `information_schema`.`GLOBAL_VARIABLES` WHERE VARIABLE_NAME in ('{}');".format("','".join(p_list))
        col_list = Dao(instance_name=instance_name).mysql_query('information_schema', sql)
        print(sql, col_list)
        res = list()
        for idx, val in col_list['rows']:
            res.append({
                'p': idx,
                'd_v': params[idx][0],
                'rb': '是' if params[idx][1] == 1 else '否',
                'a_v': params[idx][2],
                'desc': params[idx][3],
                'r_v': val
            })
    except Instance.DoesNotExist:
        print('Instance.DoesNotExist')
        res = {'status': 1, 'msg': 'Instance.DoesNotExist'}
    except Exception as e:
        print(e)
        res = {'status': 1, 'msg': str(e)}
    return HttpResponse(json.dumps(res, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


@permission_required('sql.param_view', raise_exception=True)
def param_history(request):
    limit = int(request.POST.get('limit'))
    offset = int(request.POST.get('offset'))
    limit = offset + limit
    instance_name = request.POST.get('instance_name')
    search = request.POST.get('search', '')
    if search:
        phs = ParamHistory.objects.filter(instance__instance_name=instance_name)[offset:limit]
    else:
        phs = ParamHistory.objects.filter(instance__instance_name=instance_name, param__contains=search)[offset:limit]
    res = list()
    for r in phs:
        is_active = '是' if r.is_active == 1 else '否'
        res.append({"id": r.id, "p": r.param, "old_v": r.old_var, "new_v": r.new_var, "act": is_active, "t": r.create_time})
    result = {'total': len(res), 'rows': res}
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


@csrf_exempt
@permission_required('sql.param_edit', raise_exception=True)
def param_save(request):
    post_data = QueryDict(request.body).dict()
    instance_name = post_data['instance_name']
    param = post_data['p']
    run_v = post_data['r_v']

    try:
        sql = "SELECT * FROM `information_schema`.`GLOBAL_VARIABLES` WHERE VARIABLE_NAME='{}';".format(param)
        col_list = Dao(instance_name=instance_name).mysql_query('information_schema', sql)
        p = col_list['rows'][0]
        if p[0] == run_v:
            return HttpResponse(json.dumps({'status': 1, 'msg': '参数与实际一致，未调整！'}), content_type='application/json')
        else:
            ph = ParamHistory.objects.create(instance=Instance.objects.get(instance_name=instance_name), param=param,
                                             old_var=p[1], new_var=run_v)
            sql = "UPDATE GLOBAL_VARIABLES SET VARIABLE_VALUE ='{}' WHERE VARIABLE_NAME='{}';".format(run_v, param)
            col_list = Dao(instance_name=instance_name).mysql_query('information_schema', sql)
            if 'Error' in col_list:
                ph.is_active = 1
                res = {'status': 1, 'msg': col_list['Error']}
            else:
                ph.is_active = 0
                res = {'status': 0, 'msg': '更新成功！'}
            ph.save()
    except Exception as e:
        res = {'status': 1, 'msg': str(e)}
    return HttpResponse(json.dumps(res), content_type='application/json')
