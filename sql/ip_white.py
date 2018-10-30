# -*- coding: UTF-8 -*-

import simplejson as json
from django.contrib.auth.decorators import permission_required
from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from sql.models import Instance
from sql.utils.dao import Dao
from common.utils.extend_json_encoder import ExtendJSONEncoder


@csrf_exempt
@permission_required('sql.instance_user', raise_exception=True)
def ip_white_list(request, instance_id):
    instance_name = Instance.objects.get(id=int(instance_id)).instance_name
    search = request.POST.get('search', '')
    if search:
        sql = "SELECT user,host FROM mysql.user WHERE user like '%{0}%' or host like '%{0}%';".format(search)
    else:
        sql = "SELECT user,host FROM mysql.user;"

    col_list = Dao(instance_name=instance_name).mysql_query('mysql', sql)
    result = list()
    for idx, val in col_list['rows']:
        result.append({
            'user': idx,
            'host': val
        })
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


@csrf_exempt
@permission_required('sql.instance_user_edit', raise_exception=True)
def ip_white_add(request, instance_id):
    instance_name = Instance.objects.get(id=int(instance_id)).instance_name
    db = request.POST.get('db', '')
    user = request.POST.get('user', '')
    host = request.POST.get('host', '')
    pwd = request.POST.get('password', '')
    privileges = request.POST.getlist('privileges', [])
    if not db or not user or not host or not privileges:
        result = {'status': 1, 'errmsg': '不能未空'}
        return JsonResponse(result)

    sql = "GRANT {0} ON `{1}`.* TO '{2}'@'{3}' IDENTIFIED by '{4}';".format(','.join(privileges), db, user, host, pwd)
    col_list = Dao(instance_name=instance_name).mysql_query('mysql', sql)
    result = {}
    return JsonResponse(result)


@csrf_exempt
@permission_required('sql.instance_user_edit', raise_exception=True)
def ip_white_edit(request, instance_id):
    instance_name = Instance.objects.get(id=int(instance_id)).instance_name
    user = request.POST.get('user', '')
    host = request.POST.get('host', '')
    old_user = request.POST.get('olduser', '')
    old_host = request.POST.get('oldhost', '')
    sql = "UPDATE mysql.user SET user={0},host={1} WHERE user='{2}' and host='{3}';".format(user, host, old_user, old_host)
    col_list = Dao(instance_name=instance_name).mysql_query('mysql', sql)
    result = {}
    return JsonResponse(result)


@csrf_exempt
@permission_required('sql.instance_user_edit', raise_exception=True)
def ip_white_del(request, instance_id):
    instance_name = Instance.objects.get(id=int(instance_id)).instance_name
    user = request.POST.get('user', '')
    host = request.POST.get('host', '')

    sql = "DELETE FROM mysql.user WHERE user='{}' and host='{}';".format(user, host)
    col_list = Dao(instance_name=instance_name).mysql_query('mysql', sql)
    result = {}
    return JsonResponse(result)
