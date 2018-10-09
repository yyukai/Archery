# -*- coding: UTF-8 -*-
import datetime
import logging
import simplejson as json
from django.db.models import Q
from django.http import HttpResponse
from sql.models import Backup, Instance
from common.utils.permission import superuser_required
from common.utils.extend_json_encoder import ExtendJSONEncoder

logger = logging.getLogger('default')


@superuser_required
def backup_list(request):
    search = request.POST.get('search')
    if search is None:
        clusters = Instance.objects.get_queryset()
    else:
        clusters = Instance.objects.filter(Q(instance_name__contains=search) | Q(host__contains=search))
    result = list()
    for c in clusters:
        data_bk = Backup.objects.filter(data_type='data', db_cluster=c.instance_name).order_by('-create_time')
        binlog_bk = Backup.objects.filter(data_type='binlog', db_cluster=c.instance_name).order_by('-create_time')
        result.append(
            {
                'id': c.id,
                'db_cluster': c.instance_name,
                'data': data_bk[0].create_time if data_bk else '',
                'data_size': data_bk[0].bk_size if data_bk else '',
                'data_state': data_bk[0].bk_state if data_bk else '',
                'binlog': binlog_bk[0].create_time if binlog_bk else '',
                'binlog_size': binlog_bk[0].bk_size if binlog_bk else '',
                'binlog_state': binlog_bk[0].bk_state if binlog_bk else ''
            }
        )

    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


@superuser_required
def backup_detail_list(request, db_cluster):
    limit = int(request.POST.get('limit'))
    offset = int(request.POST.get('offset'))
    limit = offset + limit

    s_time = request.POST.get('StartTime')
    e_time = request.POST.get('EndTime')
    e_time = datetime.datetime.strptime(e_time, '%Y-%m-%d') + datetime.timedelta(days=1)
    obj_list = Backup.objects.filter(db_cluster=db_cluster, create_time__range=(s_time, e_time))

    # 下拉列表
    data_type = request.POST.get('data_type')
    if data_type:
        obj_list = obj_list.filter(data_type=data_type)

    # 获取搜索参数
    search = request.POST.get('search')
    if search is None:
        obj_list = obj_list[offset:limit].values("bk_ip", "db_cluster", "db_type", "bk_path",
                                                 "bk_size", "bk_state", "data_type", "check_man", "bk_start_time",
                                                 "bk_end_time", "create_time")
    else:
        obj_list = obj_list.filter(Q(bk_ip__contains=search) | Q(db_cluster__contains=search) |
                                   Q(db_type__contains=search) | Q(check_man__contains=search)
                                   )[offset:limit].values("bk_ip", "db_cluster", "db_type", "bk_path",
                                                          "bk_size", "bk_state", "data_type", "check_man",
                                                          "bk_start_time", "bk_end_time", "create_time")
    obj_count = obj_list.count()

    # QuerySet 序列化
    rows = [row for row in obj_list]

    result = {"total": obj_count, "rows": rows}
    # 返回查询结果
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')
