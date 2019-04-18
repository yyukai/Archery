# -*- coding: UTF-8 -*-

import simplejson as json
import logging
from django.contrib.auth.decorators import permission_required
from django.http import HttpResponse
from django.db.models import Q
from sql.utils.dao import Dao
from sql.utils.resource_group import user_instances
from common.utils.extend_json_encoder import ExtendJSONEncoder

logger = logging.getLogger('default')


# 获取binlog列表
@permission_required('sql.menu_binlog', raise_exception=True)
def binlog_list(request):
    limit = int(request.POST.get('limit'))
    offset = int(request.POST.get('offset'))
    limit = offset + limit

    result = []
    obj_list = user_instances(request.user, 'all')

    instance_name = request.POST.get('instance_name', '')
    if instance_name:
        obj_list = obj_list.filter(instance_name=instance_name)

    search = request.POST.get('search', '')
    if search:
        obj_list = obj_list.filter(Q(instance_name__contains=search) | Q(host__contains=search))

    for ins in obj_list[offset:limit]:
        binlog = Dao(instance_name=ins.instance_name).mysql_query('information_schema', 'show binary logs;')
        column_list = binlog['column_list']
        ins_info = []
        for row in binlog['rows']:
            row_info = {}
            for row_index, row_item in enumerate(row):
                row_info[column_list[row_index]] = row_item
            ins_info.append(row_info)
        result.append({'instance_name': ins.instance_name, 'count': len(ins_info), 'data': ins_info})
    result = {'total': obj_list.count(), 'rows': result}
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


@permission_required('sql.menu_binlog', raise_exception=True)
def delete_log(request):
    instance_name = request.POST.get('instance_name', '')
    binlog = request.POST.get('binlog', '')
    if instance_name and binlog:
        sql = "purge master logs to '{}';".format(binlog)
        result = Dao(instance_name=instance_name).mysql_query('mysql', sql)
        result = {'status': 0, 'msg': 'success', 'data': result}
    else:
        result = {'status': 1, 'msg': 'Error:未选择实例或binlog！', 'data': ''}
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')
