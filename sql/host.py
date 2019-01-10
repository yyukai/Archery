# -*- coding: UTF-8 -*-

import simplejson as json
from django.contrib.auth.decorators import permission_required
from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.db.models import Q
from sql.models import Host, Instance
from sql.utils.dao import Dao
from common.utils.extend_json_encoder import ExtendJSONEncoder


@csrf_exempt
@permission_required('sql.menu_host', raise_exception=True)
def host_list(request):
    host_type = request.POST.get('type', '')
    search = request.POST.get('search', '')
    if host_type:
        obj_list = Host.objects.filter(type=host_type)
    else:
        obj_list = Host.objects.get_queryset()
    if search:
        obj_list = obj_list.filter(ip__contains=search)

    result = list()
    for obj in obj_list:
        instance_names = [i["instance_name"] for i in Instance.objects.filter(host=obj.ip).values('instance_name')]
        for i in range(0, 6 - len(instance_names)):
            instance_names.append('')
        (ins1, ins2, ins3, ins4, ins5, ins6) = instance_names
        result.append({
            'id': obj.id,
            'ip': obj.ip,
            'mem': obj.memory,
            'cpu': obj.cpu,
            'type': obj.type,
            'inited': obj.inited,
            'ins1': ins1,
            'ins2': ins2,
            'ins3': ins3,
            'ins4': ins4,
            'ins5': ins5,
            'ins6': ins6,
            'time': obj.update_time
        })
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


@csrf_exempt
@permission_required('sql.ip_white_edit', raise_exception=True)
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
