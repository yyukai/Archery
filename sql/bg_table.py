# -*- coding: UTF-8 -*-
import simplejson as json
from django.db.models import Q
from django.http import HttpResponse
from django.contrib.auth.decorators import permission_required
from sql.models import BGTable
from common.utils.extend_json_encoder import ExtendJSONEncoder


@permission_required('sql.menu_database', raise_exception=True)
def bg_table_list(request):
    limit = int(request.GET.get('limit'))
    offset = int(request.GET.get('offset'))
    limit = offset + limit

    # 下拉列表
    db_name = request.GET.get('db_name', '')
    search = request.GET.get('search', '')
    if db_name and search:
        obj_list = BGTable.objects.filter(db_name=db_name).filter(table_name__contains=search).distinct()
    elif db_name:
        obj_list = BGTable.objects.filter(db_name=db_name)
    elif search:
        obj_list = BGTable.objects.filter(
            Q(db_name__contains=search) | Q(table_name__contains=search)).distinct()
    else:
        obj_list = BGTable.objects.get_queryset()
    obj_count = obj_list.count()

    # QuerySet 序列化
    rows = [row for row in obj_list[offset:limit].values('id', 'db_name', 'table_name', 'create_time')]

    result = {"total": obj_count, "rows": rows}
    # 返回查询结果
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')
