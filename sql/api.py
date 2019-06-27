# -*- coding: UTF-8 -*-

import simplejson as json
from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from sql.models import Host, Instance, InstancePerf
from common.utils.extend_json_encoder import ExtendJSONEncoder


@csrf_exempt
def api_host_edit(request):
    host = request.POST.get('host', '')
    release = request.POST.get('release', '')
    memory = request.POST.get('memory', '')
    memory_used = request.POST.get('memory_used', '')
    cpu = request.POST.get('cpu', '')
    cpu_used = request.POST.get('cpu_used', '')
    net_io = request.POST.get('net_io', '')

    Host.objects.filter(ip=host).update(**{
        'release': release,
        'memory': memory,
        'memory_used': memory_used,
        'cpu': cpu,
        'cpu_used': cpu_used,
        'net_io': net_io,
    })
    result = {"code": 0, "result": "更新成功！"}
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


@csrf_exempt
def api_instance_edit(request):
    host = request.POST.get('host', '')
    port = request.POST.get('port', '')
    base_path = request.POST.get('base_path', '')
    conf_path = request.POST.get('conf_path', '')
    data_path = request.POST.get('data_path', '')
    err_log_path = request.POST.get('err_log_path', '')
    slow_log_path = request.POST.get('slow_log_path', '')
    disk = request.POST.get('disk', '')
    disk_used = request.POST.get('disk_used', '')
    disk_io = request.POST.get('disk_io', '')

    Instance.objects.filter(host=host, port=port).update(**{
        'base_path': base_path,
        'conf_path': conf_path,
        'data_path': data_path,
        'err_log_path': err_log_path,
        'slow_log_path': slow_log_path,
        'disk': disk,
        'disk_used': disk_used,
        'disk_io': disk_io
    })
    result = {"code": 0, "result": "更新成功！"}
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


@csrf_exempt
def db_agent(request):
    data = json.loads(request.body.decode('utf-8'))
    ip = data.get("ip")
    if "disk_used" in data:
        data['disk'] = '\n'.join(data.pop("disk_used"))
    if "memory" in data:
        t = data.pop("memory")
        data['memory'] = t["mem"]
        data['memory_used'] = t["mem_used"]
    if "mysql" in data:
        t = data.pop("mysql")
        for port, m_perf in t.items():
            try:
                ins = Instance.objects.get(host=ip, port=port)
            except Instance.DoesNotExist:
                continue
            m_perf['instance'] = ins
            InstancePerf.objects.update_or_create(**m_perf)

    if Host.objects.filter(ip=ip).exists():
        Host.objects.filter(ip=ip).update(**data)
    else:
        Host.objects.create(**data)
    result = {"code": 0, "result": "update success."}
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')
