# -*- coding: UTF-8 -*-

import simplejson as json
from django.contrib.auth.decorators import permission_required
from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from sql.models import Host, Instance
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
        instance_names = [{"id": i["id"], "instance_name": i["instance_name"]} for i in Instance.objects.filter(host=obj.ip).values('id', 'instance_name')]
        for i in range(0, 6 - len(instance_names)):
            instance_names.append({"id": -1, "instance_name": ""})
        (ins1, ins2, ins3, ins4, ins5, ins6) = instance_names
        result.append({
            'id': obj.id,
            'os': obj.os,
            'ip': obj.ip,
            'mem': obj.memory,
            'mem_used': obj.memory_used,
            'cpu': obj.cpu,
            'cpu_used': obj.cpu_used,
            'net_io': obj.net_io,
            'type': obj.type,
            'inited': obj.inited,
            'ins1': ins1,
            'ins2': ins2,
            'ins3': ins3,
            'ins4': ins4,
            'ins5': ins5,
            'ins6': ins6,
            'update_time': obj.update_time
        })
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')
