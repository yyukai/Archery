# -*- coding: UTF-8 -*-

import simplejson as json
from django.contrib.auth.decorators import permission_required
from django.http import HttpResponse
from django.db.models import Q
from sql.models import QueryAudit
from common.utils.extend_json_encoder import ExtendJSONEncoder


@permission_required('sql.query_audit', raise_exception=True)
def query_audit(request):
    instance_name = request.POST.get('instance_name', '')
    db_name = request.POST.get('db_name', '')
    db_user = request.POST.get('db_user', '')
    limit = int(request.POST.get('limit'))
    offset = int(request.POST.get('offset'))
    limit = offset + limit

    if instance_name == '' and db_name != '':
        obj_list = QueryAudit.objects.filter(db_name=db_name).order_by('-update_time')
    elif instance_name != '' and db_name != '':
        obj_list = QueryAudit.objects.filter(instance_name=instance_name, db_name=db_name).order_by('-update_time')
    else:
        obj_list = QueryAudit.objects.all().order_by('-update_time')
    if db_user:
        obj_list = obj_list.filter(db_user=db_user)

    search = request.POST.get('search', '')
    if search:
        obj_list = obj_list.filter(Q(db_user__contains=search) | Q(query_cost__contains=search)).distinct()

    rows = [row for row in obj_list[offset:limit].values('id', 'query_time', 'instance_name', 'db_name', 'db_user', 'query_cost', 'query_sql', 'create_time')]
    result = {"total": obj_list.count(), "rows": rows}
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')
