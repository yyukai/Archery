# -*- coding: UTF-8 -*-

import simplejson as json
from django.contrib.auth.decorators import permission_required
from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from sql.models import Instance
from sql.engines import get_engine
from common.utils.extend_json_encoder import ExtendJSONEncoder


@csrf_exempt
@permission_required('sql.instance_user', raise_exception=True)
def ip_white_list(request):
    instance_id = request.GET.get('instance_id')
    if not instance_id:
        return HttpResponse(json.dumps({'status': 0, 'msg': '', 'rows': []}), content_type='application/json')
    try:
        instance = Instance.objects.get(id=instance_id)
    except Instance.DoesNotExist:
        result = {'status': 1, 'msg': '实例不存在', 'data': []}
        return HttpResponse(json.dumps(result), content_type='application/json')

    sql_get_user = '''select user,host as query from mysql.user;'''
    query_engine = get_engine(instance=instance)
    query_result = query_engine.query('mysql', sql_get_user)
    if not query_result.error:
        db_users = query_result.rows
        # 获取用户权限信息
        data = []
        for db_user in db_users:
            user_priv = query_engine.query('mysql', 'show grants for \'{}\'@\'{}\';'.format(db_user[0], db_user[1]), close_conn=False).rows
            data.append({
                'user': db_user[0],
                'host': db_user[1],
                'user_host': '\'{}\'.\'{}\''.format(db_user[0], db_user[1]),
                'privileges': user_priv
            })
        result = {'status': 0, 'msg': 'ok', 'rows': data}
    else:
        result = {'status': 1, 'msg': query_result.error}
    # 关闭连接
    query_engine.close()
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


@csrf_exempt
@permission_required('sql.instance_user_edit', raise_exception=True)
def ip_white_add(request, instance_id):
    instance = Instance.objects.get(id=instance_id)
    user = request.POST.get('user', '')
    host = request.POST.get('host', '')
    password = request.POST.get('password', '')
    db_name = request.POST.get('db_name', '')
    db_name_list = request.POST.getlist('db_name_multiple', [])
    tb_name_list = request.POST.getlist('table_name', [])
    priv_list = request.POST.getlist('priv', [])
    if not priv_list:
        return JsonResponse({'code': 1, 'errmsg': '请选择权限！'})
    elif 'all' in priv_list:
        priv = 'ALL PRIVILEGES'
    else:
        priv = ','.join(priv_list)
    created, failed = [], []
    query_engine = get_engine(instance=instance)
    if db_name:
        for tb_name in tb_name_list:
            sql = """GRANT {} ON `{}`.`{}` TO '{}'@'{}' IDENTIFIED BY '{}';""".format(priv, db_name, tb_name, user, host, password)
            print(sql)
            res = query_engine.execute(sql=sql)
            if not res.error:
                created.append("{} {} {} {}".format(user, host, db_name, tb_name))
            else:
                failed.append("{} {} {}".format(db_name, tb_name, res.error))
    else:
        for db_name in db_name_list:
            sql = """GRANT {} ON `{}`.`*` TO '{}'@'{}' IDENTIFIED BY '{}';""".format(priv, db_name, user, host, password)
            print(sql)
            res = query_engine.execute(sql=sql)
            if not res.error:
                created.append("{} {} {}".format(user, host, db_name))
            else:
                failed.append("{} {} {} {}".format(user, host, db_name, res.error))
    # print(user, host, db_name, db_name_list, tb_name_list, priv_list)
    data = {
        'code': 0,
        'created': created,
        'created_info': 'Created {}'.format(len(created)),
        'failed': failed,
        'failed_info': 'Failed {}'.format(len(failed)),
        'msg': 'Created: {}., Error: {}'.format(len(created), len(failed))
    }
    return JsonResponse(data)


@csrf_exempt
@permission_required('sql.instance_user_edit', raise_exception=True)
def ip_white_edit(request, instance_id):
    instance = Instance.objects.get(id=instance_id)
    user = request.POST.get('user', '')
    host = request.POST.get('host', '')
    password = request.POST.get('password', '')
    db_name = request.POST.get('db_name', '')
    db_name_list = request.POST.getlist('db_name_multiple', [])
    tb_name_list = request.POST.getlist('table_name', [])
    priv_list = request.POST.getlist('priv', [])
    if 'all' in priv_list:
        priv = 'ALL PRIVILEGES'
    else:
        priv = ','.join(priv_list)
    created, failed = [], []
    query_engine = get_engine(instance=instance)
    if db_name:
        for tb_name in tb_name_list:
            sql = """REVOKE ALL PRIVILEGES ON `{}`.`{}` from '{}'@'{}';""".format(
                db_name, tb_name, user, host
            )
            print(sql)
            res = query_engine.execute(sql=sql)
            if not res.error or '1147' in res.error:
                if priv:
                    if password:
                        sql = """GRANT {} ON `{}`.`{}` TO '{}'@'{}' IDENTIFIED BY '{}';""".format(
                            priv, db_name, tb_name, user, host, password
                        )
                    else:
                        sql = """GRANT {} ON `{}`.`{}` TO '{}'@'{}';""".format(priv, db_name, tb_name, user, host)
                    print(sql)
                    res = query_engine.execute(sql=sql)
                    if not res.error:
                        created.append("{} {} {} {} 授权成功！".format(user, host, db_name, tb_name))
                    else:
                        failed.append("{} {} {}".format(db_name, tb_name, res.error))
                else:
                    # 未选择任何权限
                    created.append("{}@{} {}.{} 已去除所有权限！".format(user, host, db_name, tb_name))
            else:
                failed.append("{}@{} {}.* {}".format(user, host, db_name, res.error))
    else:
        for db_name in db_name_list:
            sql = """REVOKE ALL PRIVILEGES ON `{}`.`*` from '{}'@'{}';""".format(db_name, user, host)
            print(sql)
            res = query_engine.execute(sql=sql)
            if not res.error or '1147' in res.error:
                if priv:
                    if password:
                        sql = """GRANT {} ON `{}`.`*` TO '{}'@'{}' IDENTIFIED BY '{}';""".format(
                            priv, db_name, user, host, password
                        )
                    else:
                        sql = """GRANT {} ON `{}`.`*` TO '{}'@'{}';""".format(priv, db_name, user, host)
                    print(sql)
                    res = query_engine.execute(sql=sql)
                    if not res.error:
                        created.append("{} {} {} 授权成功！".format(user, host, db_name))
                    else:
                        failed.append("{} {} {} {}".format(user, host, db_name, res.error))
                else:
                    # 未选择任何权限
                    created.append("{}@{} {}.* 已去除所有权限！".format(user, host, db_name))
            else:
                failed.append("{}@{} {}.* {}".format(user, host, db_name, res.error))

    # print(user, host, db_name, db_name_list, tb_name_list, priv_list)
    data = {
        'code': 0,
        'created': created,
        'created_info': 'Created {}'.format(len(created)),
        'failed': failed,
        'failed_info': 'Failed {}'.format(len(failed)),
        'msg': 'Created: {}., Error: {}'.format(len(created), len(failed))
    }
    return JsonResponse(data)


@csrf_exempt
@permission_required('sql.instance_user_edit', raise_exception=True)
def ip_white_del(request, instance_id):
    instance = Instance.objects.get(id=instance_id)
    user_host = request.POST.get('user_host', '')
    if not user_host:
        return JsonResponse({'code': 1, 'errmsg': '非法调用！'})
    query_engine = get_engine(instance=instance)
    sql = "DROP USER {};".format(user_host)
    res = query_engine.execute(sql=sql)
    if res.error:
        result = {'code': 1, 'errmsg': res.error}
    else:
        result = {'code': 0, 'msg': '删除成功！'}
    return JsonResponse(result)
